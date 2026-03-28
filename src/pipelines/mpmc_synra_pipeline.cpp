#include "rdma/pipelines/mpmc_synra_pipeline.h"

// Phase-based replicated MPMC queue with configurable active window.
// All RDMA ops broadcast to all replicas. Completion-driven state machine.

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

// ─── Phase enum ───

enum class MpmcPhase : uint8_t {
    idle           = 0,
    claim_position = 1,
    wait_turn      = 2,
    write_advance  = 3,
    read_data      = 4,
    advance_turn   = 5,
};

// ─── WR ID encoding ───

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

// ─── Op context ───

struct MpmcOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    MpmcPhase phase = MpmcPhase::idle;
    uint64_t position = 0;
    uint32_t responses = 0;  // completions received for current broadcast phase
    size_t latency_index = 0;
    std::chrono::steady_clock::time_point started_at{};
};

// ─── Buffer layout ───
// Per active-window slot: [N × 8B atomic results, 64B-aligned][64B data staging]

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

// ─── Phase posting helpers ───

void post_claim_position(
    const Client& client, MpmcOpCtx& op, MpmcSynraBuffers& bufs,
    bool is_producer
) {
    const auto& conns = client.connections();
    auto* results = bufs.atomic_results(op.slot);
    const size_t counter_offset = is_producer ? mpmc_tail_offset() : mpmc_head_offset();

    for (size_t i = 0; i < conns.size(); ++i) {
        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(&results[i]);
        sge.length = sizeof(uint64_t);
        sge.lkey = client.mr()->lkey;

        ibv_send_wr wr{}, *bad = nullptr;
        wr.wr_id = encode_wr_id(op.generation, op.slot, MpmcPhase::claim_position,
                                static_cast<uint8_t>(i));
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.atomic.remote_addr = conns[i].addr + counter_offset;
        wr.wr.atomic.rkey = conns[i].rkey;
        wr.wr.atomic.compare_add = 1;

        if (ibv_post_send(conns[i].id->qp, &wr, &bad))
            throw std::runtime_error("mpmc_synra: claim FAA post failed");
    }

    op.phase = MpmcPhase::claim_position;
    op.responses = 0;
}

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

void post_write_advance(
    const Client& client, MpmcOpCtx& op, MpmcSynraBuffers& bufs
) {
    const auto& conns = client.connections();
    const uint32_t idx = static_cast<uint32_t>(op.position & MPMC_QUEUE_MASK);
    auto* data = bufs.data_buf(op.slot);
    auto* results = bufs.atomic_results(op.slot);

    // Prepare data
    uint64_t val = (static_cast<uint64_t>(client.id()) << 32)
                 | static_cast<uint32_t>(op.latency_index);
    std::memcpy(data, &val, sizeof(val));
    std::memset(data + sizeof(val), 0, MPMC_SLOT_DATA_SIZE - sizeof(val));

    for (size_t i = 0; i < conns.size(); ++i) {
        // Inline WRITE data (unsignaled — ordered before the next signaled CAS on this QP)
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
                throw std::runtime_error("mpmc_synra: WRITE post failed");
        }

        // CAS turn advance: pos → pos+1 (signaled)
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

void post_read_data(
    const Client& client, MpmcOpCtx& op, MpmcSynraBuffers& bufs
) {
    // READ from node 0 only — all replicas have the same data
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
        throw std::runtime_error("mpmc_synra: READ post failed");

    op.phase = MpmcPhase::read_data;
    op.responses = 0;
}

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
    return {
        .is_producer = get_uint_env_or("MPMC_IS_PRODUCER", 1) != 0,
        .queue_capacity = MPMC_QUEUE_CAPACITY,
        .active_window = std::max<size_t>(1, MPMC_ACTIVE_WINDOW),
        .cq_batch = std::max<size_t>(1, MPMC_CQ_BATCH),
    };
}

size_t mpmc_synra_pipeline_client_buffer_size(const MpmcSynraPipelineConfig& config) {
    const size_t N = CLUSTER_NODES.size();
    const size_t atomic_bytes = align_up(N * sizeof(uint64_t), 64);
    const size_t data_bytes = 64;
    const size_t slot_stride = atomic_bytes + data_bytes;
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

    // Fill the active window
    while (active < config.active_window && submitted < NUM_OPS_PER_CLIENT) {
        submit_op(active);
    }

    // Completion-driven state machine
    while (completed < NUM_OPS_PER_CLIENT) {
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

            op.responses++;

            auto finish_op = [&]() {
                latencies[op.latency_index] = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - op.started_at).count());
                op.active = false;
                op.phase = MpmcPhase::idle;
                completed++;
                active--;
                if (submitted < NUM_OPS_PER_CLIENT) submit_op(slot);
            };

            switch (phase) {
            case MpmcPhase::claim_position:
                if (op.responses < N) break;
                op.position = bufs.atomic_results(op.slot)[0];
                post_wait_turn(client, op, bufs, config.is_producer);
                break;

            case MpmcPhase::wait_turn: {
                if (op.responses < N) break;
                const uint64_t expected = config.is_producer
                    ? op.position : op.position + 1;
                if (bufs.atomic_results(op.slot)[0] == expected) {
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
                if (op.responses < N) break;
                finish_op();
                break;

            case MpmcPhase::read_data:
                // Single completion (node 0 only)
                post_advance_turn(client, op, bufs);
                break;

            case MpmcPhase::advance_turn:
                if (op.responses < N) break;
                finish_op();
                break;

            case MpmcPhase::idle:
                break;
            }
        }
    }
}
