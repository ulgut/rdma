#include "rdma/pipelines/mpmc_synra_pipeline.h"

// Replicated Rigtorp MPMC bounded queue over RDMA (N server replicas).
//
// Producer push:
//   1. FAA(tail) on node 0                          — claim position P
//   2. CAS(EMPTY, P) on flat push_log[P] on all N   — replicate claim (synra_faa pattern)
//   3. CAS-read on all N replicas                   — spin on slot turn, advance if any match
//   4. Write data + CAS turn to all N               — publish slot, wait for all
//
// Consumer pop:
//   1. FAA(head) on node 0                          — claim position P
//   2. CAS(EMPTY, P) on flat pop_log[P] on all N    — replicate claim (synra_faa pattern)
//   3. CAS-read on all N replicas                   — spin on slot turn, advance if any match
//   4. Read data from node 0                        — single replica read
//   5. CAS turn advance on all N                    — recycle slot, wait for all
//
// Wait_turn broadcasts to all replicas and collects all completions before
// checking results. This avoids orphaned CQEs from a first-wins approach.

#include "rdma/client.h"
#include "rdma/common.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

enum class MpmcPhase : uint8_t {
    idle            = 0,
    claim_position  = 1,
    replicate_claim = 2,
    wait_turn       = 3,
    write_advance   = 4,
    read_data       = 5,
    advance_turn    = 6,
};

// WR id layout: [generation:32][slot:16][phase:8][conn:8]
constexpr size_t kConnBits = 8;

uint64_t encode_wr_id(uint32_t generation, uint32_t slot, MpmcPhase phase, uint8_t conn) {
    return (static_cast<uint64_t>(generation) << 32)
         | (static_cast<uint64_t>(slot) << 16)
         | (static_cast<uint64_t>(phase) << kConnBits)
         | static_cast<uint64_t>(conn);
}

uint32_t wr_generation(uint64_t wr_id) { return static_cast<uint32_t>(wr_id >> 32); }
uint32_t wr_slot(uint64_t wr_id) { return static_cast<uint32_t>((wr_id >> 16) & 0xFFFFu); }
MpmcPhase wr_phase(uint64_t wr_id) { return static_cast<MpmcPhase>((wr_id >> kConnBits) & 0xFFu); }
uint8_t wr_conn(uint64_t wr_id) { return static_cast<uint8_t>(wr_id & 0xFFu); }

struct MpmcOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    MpmcPhase phase = MpmcPhase::idle;
    uint64_t position = 0;
    uint32_t responses = 0;
    size_t latency_index = 0;
    std::chrono::steady_clock::time_point started_at{};
};

// Local buffer layout per active-window slot:
//   [N x 8B atomic results, 64B-aligned] [64B data staging]
struct MpmcSynraBuffers {
    size_t slot_stride = 0;
    size_t num_replicas = 0;

    uint64_t* atomic_results(uint32_t slot) {
        return reinterpret_cast<uint64_t*>(base + slot * slot_stride);
    }
    uint8_t* data_buf(uint32_t slot) {
        return base + slot * slot_stride + align_up(num_replicas * sizeof(uint64_t), 64);
    }

    uint8_t* base = nullptr;
};

void post_claim_position(
    const Client& client, MpmcOpCtx& op, MpmcSynraBuffers& bufs,
    bool is_producer
) {
    const auto& node0 = client.connections().front();
    auto* result = &bufs.atomic_results(op.slot)[0];
    const size_t counter_offset = is_producer ? mpmc_tail_offset() : mpmc_head_offset();

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = client.mr()->lkey;

    ibv_send_wr wr{}, *bad = nullptr;
    wr.wr_id = encode_wr_id(op.generation, op.slot, MpmcPhase::claim_position, 0);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = node0.addr + counter_offset;
    wr.wr.atomic.rkey = node0.rkey;
    wr.wr.atomic.compare_add = 1;

    if (ibv_post_send(node0.id->qp, &wr, &bad))
        throw std::runtime_error("mpmc_synra: claim FAA post failed");

    op.phase = MpmcPhase::claim_position;
}

// Replicate a single position claim to one replica via CAS(EMPTY_SLOT, position)
// on the flat replication log. Each position maps to a unique log entry.
void post_replicate_claim_single(
    const Client& client, MpmcOpCtx& op, MpmcSynraBuffers& bufs,
    uint8_t conn_idx, bool is_producer
) {
    const auto& conns = client.connections();
    const size_t log_offset = is_producer
        ? mpmc_synra_push_log_offset(op.position)
        : mpmc_synra_pop_log_offset(op.position);
    auto* result = &bufs.atomic_results(op.slot)[conn_idx];

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = client.mr()->lkey;

    ibv_send_wr wr{}, *bad = nullptr;
    wr.wr_id = encode_wr_id(op.generation, op.slot, MpmcPhase::replicate_claim, conn_idx);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = conns[conn_idx].addr + log_offset;
    wr.wr.atomic.rkey = conns[conn_idx].rkey;
    wr.wr.atomic.compare_add = EMPTY_SLOT;
    wr.wr.atomic.swap = op.position;

    if (ibv_post_send(conns[conn_idx].id->qp, &wr, &bad))
        throw std::runtime_error("mpmc_synra: replicate CAS post failed");
}

// Post CAS(EMPTY_SLOT, position) to ALL replicas on the flat replication log.
// Like synra_faa: FAA on node 0 gives a unique position, then CAS into the
// log slot on every replica to record the claim.
void post_replicate_claim(
    const Client& client, MpmcOpCtx& op, MpmcSynraBuffers& bufs,
    bool is_producer
) {
    const auto& conns = client.connections();
    for (size_t i = 0; i < conns.size(); ++i) {
        post_replicate_claim_single(client, op, bufs, static_cast<uint8_t>(i), is_producer);
    }
    op.phase = MpmcPhase::replicate_claim;
    op.responses = 0;
}

// Broadcast CAS(expected, expected) to all replicas to check the slot turn.
// All N completions are collected before checking if any replica returned
// the expected turn value.
void post_wait_turn(
    const Client& client, MpmcOpCtx& op, MpmcSynraBuffers& bufs,
    bool is_producer
) {
    const auto& conns = client.connections();
    const uint32_t idx = static_cast<uint32_t>(op.position & MPMC_QUEUE_MASK);
    const uint64_t expected = is_producer ? op.position : op.position + 1;
    auto* results = bufs.atomic_results(op.slot);

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&results[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = client.mr()->lkey;

        ibv_send_wr wr{}, *bad = nullptr;
        wr.wr_id = encode_wr_id(op.generation, op.slot, MpmcPhase::wait_turn,
                                static_cast<uint8_t>(i));
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.atomic.remote_addr = conns[i].addr + mpmc_slot_turn_offset(idx);
        wr.wr.atomic.rkey = conns[i].rkey;
        wr.wr.atomic.compare_add = expected;
        wr.wr.atomic.swap = expected;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad))
            throw std::runtime_error("mpmc_synra: wait_turn CAS post failed");
    }

    op.phase = MpmcPhase::wait_turn;
    op.responses = 0;
}

// Producer: write payload + CAS turn advance to all replicas.
// Per replica, the unsignaled write is ordered before the signaled CAS
// on the same QP.
void post_write_advance(
    const Client& client, MpmcOpCtx& op, MpmcSynraBuffers& bufs
) {
    const auto& conns = client.connections();
    const uint32_t idx = static_cast<uint32_t>(op.position & MPMC_QUEUE_MASK);
    auto* data = bufs.data_buf(op.slot);
    auto* results = bufs.atomic_results(op.slot);

    uint64_t val = (static_cast<uint64_t>(client.id()) << 32)
                 | static_cast<uint32_t>(op.latency_index);
    std::memcpy(data, &val, sizeof(val));
    std::memset(data + sizeof(val), 0, MPMC_SLOT_DATA_SIZE - sizeof(val));

    for (size_t i = 0; i < conns.size(); ++i) {
        {
            ibv_sge sge{};
            sge.addr = reinterpret_cast<uintptr_t>(data);
            sge.length = MPMC_SLOT_DATA_SIZE;
            sge.lkey = client.mr()->lkey;

            ibv_send_wr wr{}, *bad = nullptr;
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags = IBV_SEND_INLINE;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.wr.rdma.remote_addr = conns[i].addr + mpmc_slot_data_offset(idx);
            wr.wr.rdma.rkey = conns[i].rkey;

            if (ibv_post_send(conns[i].id->qp, &wr, &bad))
                throw std::runtime_error("mpmc_synra: write post failed");
        }

        {
            ibv_sge sge{};
            sge.addr = reinterpret_cast<uintptr_t>(&results[i]);
            sge.length = sizeof(uint64_t);
            sge.lkey = client.mr()->lkey;

            ibv_send_wr wr{}, *bad = nullptr;
            wr.wr_id = encode_wr_id(op.generation, op.slot, MpmcPhase::write_advance,
                                    static_cast<uint8_t>(i));
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.wr.atomic.remote_addr = conns[i].addr + mpmc_slot_turn_offset(idx);
            wr.wr.atomic.rkey = conns[i].rkey;
            wr.wr.atomic.compare_add = op.position;
            wr.wr.atomic.swap = op.position + 1;

            if (ibv_post_send(conns[i].id->qp, &wr, &bad))
                throw std::runtime_error("mpmc_synra: turn advance CAS post failed");
        }
    }

    op.phase = MpmcPhase::write_advance;
    op.responses = 0;
}

// Consumer: read data from node 0 only (all replicas have identical data).
void post_read_data(
    const Client& client, MpmcOpCtx& op, MpmcSynraBuffers& bufs
) {
    const auto& server = client.connections().front();
    const uint32_t idx = static_cast<uint32_t>(op.position & MPMC_QUEUE_MASK);
    auto* data = bufs.data_buf(op.slot);

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(data);
    sge.length = MPMC_SLOT_DATA_SIZE;
    sge.lkey = client.mr()->lkey;

    ibv_send_wr wr{}, *bad = nullptr;
    wr.wr_id = encode_wr_id(op.generation, op.slot, MpmcPhase::read_data, 0);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = server.addr + mpmc_slot_data_offset(idx);
    wr.wr.rdma.rkey = server.rkey;

    if (ibv_post_send(server.id->qp, &wr, &bad))
        throw std::runtime_error("mpmc_synra: read post failed");

    op.phase = MpmcPhase::read_data;
    op.responses = 0;
}

// Consumer: CAS turn advance on all replicas to recycle the slot.
// Turn goes from pos + 1 -> pos + capacity.
void post_advance_turn(
    const Client& client, MpmcOpCtx& op, MpmcSynraBuffers& bufs
) {
    const auto& conns = client.connections();
    const uint32_t idx = static_cast<uint32_t>(op.position & MPMC_QUEUE_MASK);
    auto* results = bufs.atomic_results(op.slot);

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&results[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = client.mr()->lkey;

        ibv_send_wr wr{}, *bad = nullptr;
        wr.wr_id = encode_wr_id(op.generation, op.slot, MpmcPhase::advance_turn,
                                static_cast<uint8_t>(i));
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.atomic.remote_addr = conns[i].addr + mpmc_slot_turn_offset(idx);
        wr.wr.atomic.rkey = conns[i].rkey;
        wr.wr.atomic.compare_add = op.position + 1;
        wr.wr.atomic.swap = op.position + MPMC_QUEUE_CAPACITY;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad))
            throw std::runtime_error("mpmc_synra: advance_turn CAS post failed");
    }

    op.phase = MpmcPhase::advance_turn;
    op.responses = 0;
}

} // namespace

MpmcSynraPipelineConfig load_mpmc_synra_pipeline_config() {
    const size_t clients_per_machine = get_uint_env_or("CLIENTS_PER_MACHINE", NUM_CLIENTS_PER_MACHINE);
    const size_t total_clients = clients_per_machine * get_uint_env_or("TOTAL_CLIENT_MACHINES", static_cast<unsigned int>(TOTAL_CLIENT_MACHINES));
    const size_t num_ops = get_uint_env_or("NUM_OPS", NUM_OPS);
    return {
        .is_producer = get_uint_env_or("MPMC_IS_PRODUCER", 1) != 0,
        .queue_capacity = MPMC_QUEUE_CAPACITY,
        .active_window = std::max<size_t>(1, get_uint_env_or("MPMC_ACTIVE_WINDOW", MPMC_ACTIVE_WINDOW)),
        .cq_batch = std::max<size_t>(1, MPMC_CQ_BATCH),
        .num_ops = num_ops / total_clients,
    };
}

size_t mpmc_synra_pipeline_client_buffer_size(const MpmcSynraPipelineConfig& config) {
    const size_t N = CLUSTER_NODES.size();
    const size_t atomic_bytes = align_up(N * sizeof(uint64_t), 64);
    const size_t slot_stride = atomic_bytes + 64;
    return align_up(config.active_window * slot_stride + PAGE_SIZE, PAGE_SIZE);
}

void run_mpmc_synra_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* /*lock_counts*/,
    const MpmcSynraPipelineConfig& config
) {
    const auto& conns = client.connections();
    if (conns.empty())
        throw std::runtime_error("mpmc_synra pipeline: no server connections");

    const size_t N = conns.size();
    const size_t atomic_bytes = align_up(N * sizeof(uint64_t), 64);

    MpmcSynraBuffers bufs{};
    bufs.base = static_cast<uint8_t*>(client.buffer());
    bufs.slot_stride = atomic_bytes + 64;
    bufs.num_replicas = N;

    std::vector<MpmcOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);

    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;

    auto submit_op = [&](size_t slot) {
        auto& op = ops[slot];
        op.active = true;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.phase = MpmcPhase::idle;
        op.position = 0;
        op.responses = 0;
        op.latency_index = submitted;
        op.started_at = std::chrono::steady_clock::now();
        post_claim_position(client, op, bufs, config.is_producer);
        submitted++;
        active++;
    };

    while (active < config.active_window && submitted < config.num_ops) {
        submit_op(active);
    }

    while (completed < config.num_ops) {
        const int polled = ibv_poll_cq(client.cq(),
            static_cast<int>(completions.size()), completions.data());
        if (polled < 0)
            throw std::runtime_error("mpmc_synra pipeline: CQ poll failed");
        if (polled == 0) continue;

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[i];
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "mpmc_synra pipeline: WC error status="
                    + std::to_string(wc.status)
                    + " opcode=" + std::to_string(wc.opcode));
            }

            const uint32_t slot = wr_slot(wc.wr_id);
            if (slot >= ops.size()) continue;
            auto& op = ops[slot];
            if (!op.active || op.generation != wr_generation(wc.wr_id)) continue;

            const MpmcPhase phase = wr_phase(wc.wr_id);
            if (phase != op.phase) continue;

            auto finish_op = [&]() {
                latencies[op.latency_index] = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count());
                op.active = false;
                op.phase = MpmcPhase::idle;
                completed++;
                active--;
                if (submitted < config.num_ops) submit_op(slot);
            };

            switch (phase) {
            case MpmcPhase::claim_position:
                op.position = bufs.atomic_results(op.slot)[0];
                if (N > 1) {
                    post_replicate_claim(client, op, bufs, config.is_producer);
                } else {
                    post_wait_turn(client, op, bufs, config.is_producer);
                }
                break;

            case MpmcPhase::replicate_claim: {
                const uint8_t conn = wr_conn(wc.wr_id);
                const uint64_t old_val = bufs.atomic_results(op.slot)[conn];
                if (old_val == EMPTY_SLOT) {
                    // CAS succeeded — flat log slot was empty, now claimed.
                    op.responses++;
                    if (op.responses >= N) {
                        post_wait_turn(client, op, bufs, config.is_producer);
                    }
                } else {
                    throw std::runtime_error(
                        "mpmc_synra: flat log CAS failed at position "
                        + std::to_string(op.position) + " replica " + std::to_string(conn)
                        + " (got " + std::to_string(old_val) + ", expected EMPTY_SLOT)");
                }
                break;
            }

            case MpmcPhase::wait_turn: {
                op.responses++;
                if (op.responses < N) break;
                // All completions collected; check if any replica has the expected turn.
                const uint64_t expected = config.is_producer
                    ? op.position : op.position + 1;
                auto* results = bufs.atomic_results(op.slot);
                bool ready = false;
                for (size_t r = 0; r < N; ++r) {
                    if (results[r] == expected) { ready = true; break; }
                }
                if (ready) {
                    if (config.is_producer)
                        post_write_advance(client, op, bufs);
                    else
                        post_read_data(client, op, bufs);
                } else {
                    post_wait_turn(client, op, bufs, config.is_producer);
                }
                break;
            }

            case MpmcPhase::write_advance:
                op.responses++;
                if (op.responses < N) break;
                finish_op();
                break;

            case MpmcPhase::read_data:
                post_advance_turn(client, op, bufs);
                break;

            case MpmcPhase::advance_turn:
                op.responses++;
                if (op.responses < N) break;
                finish_op();
                break;

            case MpmcPhase::idle:
                break;
            }
        }
    }
}
