#include "rdma/pipelines/mpmc_simple_pipeline.h"

// Non-replicated Rigtorp MPMC bounded queue over RDMA (single server).
//
// Producer push: FAA(tail) -> spin on slot turn -> write data + CAS turn advance
// Consumer pop:  FAA(head) -> spin on slot turn -> read data -> CAS turn advance
//
// The spin uses CAS(expected, expected) as a non-destructive remote read.
// The write is unsignaled inline, ordered before the signaled CAS on the same QP.

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
    idle           = 0,
    claim_position = 1,
    wait_turn      = 2,
    write_advance  = 3,
    read_data      = 4,
    advance_turn   = 5,
};

// WR id layout: [generation:32][slot:16][phase:8][unused:8]
constexpr size_t kConnBits = 8;

uint64_t encode_wr_id(uint32_t generation, uint32_t slot, MpmcPhase phase) {
    return (static_cast<uint64_t>(generation) << 32)
         | (static_cast<uint64_t>(slot) << 16)
         | (static_cast<uint64_t>(phase) << kConnBits);
}

uint32_t wr_generation(uint64_t wr_id) { return static_cast<uint32_t>(wr_id >> 32); }
uint32_t wr_slot(uint64_t wr_id) { return static_cast<uint32_t>((wr_id >> 16) & 0xFFFFu); }
MpmcPhase wr_phase(uint64_t wr_id) { return static_cast<MpmcPhase>((wr_id >> kConnBits) & 0xFFu); }

struct MpmcOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    MpmcPhase phase = MpmcPhase::idle;
    uint64_t position = 0;
    size_t latency_index = 0;
    std::chrono::steady_clock::time_point started_at{};
};

// Local buffer layout per active-window slot: [8B atomic result | 56B data staging]
struct MpmcBuffers {
    uint64_t* atomic_result(uint32_t slot) {
        return reinterpret_cast<uint64_t*>(base + slot * 64);
    }
    uint8_t* data_buf(uint32_t slot) {
        return base + slot * 64 + 8;
    }
    uint8_t* base = nullptr;
};

void post_claim_position(
    const Client& client, MpmcOpCtx& op, MpmcBuffers& bufs,
    bool is_producer
) {
    const auto& server = client.connections().front();
    auto* result = bufs.atomic_result(op.slot);

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = client.mr()->lkey;

    ibv_send_wr wr{}, *bad = nullptr;
    wr.wr_id = encode_wr_id(op.generation, op.slot, MpmcPhase::claim_position);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = server.addr
        + (is_producer ? mpmc_tail_offset() : mpmc_head_offset());
    wr.wr.atomic.rkey = server.rkey;
    wr.wr.atomic.compare_add = 1;

    if (ibv_post_send(server.id->qp, &wr, &bad))
        throw std::runtime_error("mpmc_simple: claim FAA post failed");

    op.phase = MpmcPhase::claim_position;
}

// CAS(expected, expected) — non-destructive read of the slot turn value.
// Returns the current turn; caller checks if it matches the expected value.
void post_wait_turn(
    const Client& client, MpmcOpCtx& op, MpmcBuffers& bufs,
    bool is_producer
) {
    const auto& server = client.connections().front();
    const uint32_t idx = static_cast<uint32_t>(op.position & MPMC_QUEUE_MASK);
    const uint64_t expected = is_producer ? op.position : op.position + 1;
    auto* result = bufs.atomic_result(op.slot);

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = client.mr()->lkey;

    ibv_send_wr wr{}, *bad = nullptr;
    wr.wr_id = encode_wr_id(op.generation, op.slot, MpmcPhase::wait_turn);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = server.addr + mpmc_slot_turn_offset(idx);
    wr.wr.atomic.rkey = server.rkey;
    wr.wr.atomic.compare_add = expected;
    wr.wr.atomic.swap = expected;

    if (ibv_post_send(server.id->qp, &wr, &bad))
        throw std::runtime_error("mpmc_simple: wait_turn CAS post failed");

    op.phase = MpmcPhase::wait_turn;
}

// Producer: write payload then advance turn in a single post pair.
// The write is unsignaled inline; the CAS is signaled. RDMA ordering on
// the same QP guarantees the write completes before the CAS.
void post_write_advance(
    const Client& client, MpmcOpCtx& op, MpmcBuffers& bufs
) {
    const auto& server = client.connections().front();
    const uint32_t idx = static_cast<uint32_t>(op.position & MPMC_QUEUE_MASK);
    auto* data = bufs.data_buf(op.slot);
    auto* result = bufs.atomic_result(op.slot);

    uint64_t val = (static_cast<uint64_t>(client.id()) << 32)
                 | static_cast<uint32_t>(op.latency_index);
    std::memcpy(data, &val, sizeof(val));
    std::memset(data + sizeof(val), 0, MPMC_SLOT_DATA_SIZE - sizeof(val));

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
        wr.wr.rdma.remote_addr = server.addr + mpmc_slot_data_offset(idx);
        wr.wr.rdma.rkey = server.rkey;

        if (ibv_post_send(server.id->qp, &wr, &bad))
            throw std::runtime_error("mpmc_simple: write post failed");
    }

    // Turn advance: pos -> pos + 1 makes the slot visible to consumers
    {
        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(result);
        sge.length = sizeof(uint64_t);
        sge.lkey = client.mr()->lkey;

        ibv_send_wr wr{}, *bad = nullptr;
        wr.wr_id = encode_wr_id(op.generation, op.slot, MpmcPhase::write_advance);
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.atomic.remote_addr = server.addr + mpmc_slot_turn_offset(idx);
        wr.wr.atomic.rkey = server.rkey;
        wr.wr.atomic.compare_add = op.position;
        wr.wr.atomic.swap = op.position + 1;

        if (ibv_post_send(server.id->qp, &wr, &bad))
            throw std::runtime_error("mpmc_simple: turn advance CAS post failed");
    }

    op.phase = MpmcPhase::write_advance;
}

void post_read_data(
    const Client& client, MpmcOpCtx& op, MpmcBuffers& bufs
) {
    const auto& server = client.connections().front();
    const uint32_t idx = static_cast<uint32_t>(op.position & MPMC_QUEUE_MASK);
    auto* data = bufs.data_buf(op.slot);

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(data);
    sge.length = MPMC_SLOT_DATA_SIZE;
    sge.lkey = client.mr()->lkey;

    ibv_send_wr wr{}, *bad = nullptr;
    wr.wr_id = encode_wr_id(op.generation, op.slot, MpmcPhase::read_data);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = server.addr + mpmc_slot_data_offset(idx);
    wr.wr.rdma.rkey = server.rkey;

    if (ibv_post_send(server.id->qp, &wr, &bad))
        throw std::runtime_error("mpmc_simple: read post failed");

    op.phase = MpmcPhase::read_data;
}

// Consumer turn advance: pos + 1 -> pos + capacity, recycling the slot
// for the next producer round.
void post_advance_turn(
    const Client& client, MpmcOpCtx& op, MpmcBuffers& bufs
) {
    const auto& server = client.connections().front();
    const uint32_t idx = static_cast<uint32_t>(op.position & MPMC_QUEUE_MASK);
    auto* result = bufs.atomic_result(op.slot);

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result);
    sge.length = sizeof(uint64_t);
    sge.lkey = client.mr()->lkey;

    ibv_send_wr wr{}, *bad = nullptr;
    wr.wr_id = encode_wr_id(op.generation, op.slot, MpmcPhase::advance_turn);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = server.addr + mpmc_slot_turn_offset(idx);
    wr.wr.atomic.rkey = server.rkey;
    wr.wr.atomic.compare_add = op.position + 1;
    wr.wr.atomic.swap = op.position + MPMC_QUEUE_CAPACITY;

    if (ibv_post_send(server.id->qp, &wr, &bad))
        throw std::runtime_error("mpmc_simple: advance_turn CAS post failed");

    op.phase = MpmcPhase::advance_turn;
}

} // namespace

MpmcSimplePipelineConfig load_mpmc_simple_pipeline_config() {
    const size_t clients_per_machine = get_uint_env_or("CLIENTS_PER_MACHINE", NUM_CLIENTS_PER_MACHINE);
    const size_t total_clients = clients_per_machine * TOTAL_CLIENT_MACHINES;
    const size_t num_ops = get_uint_env_or("NUM_OPS", NUM_OPS);
    return {
        .is_producer = get_uint_env_or("MPMC_IS_PRODUCER", 1) != 0,
        .queue_capacity = MPMC_QUEUE_CAPACITY,
        .active_window = std::max<size_t>(1, get_uint_env_or("MPMC_ACTIVE_WINDOW", MPMC_ACTIVE_WINDOW)),
        .cq_batch = std::max<size_t>(1, MPMC_CQ_BATCH),
        .num_ops = num_ops / total_clients,
    };
}

size_t mpmc_simple_pipeline_client_buffer_size(const MpmcSimplePipelineConfig& config) {
    return align_up(config.active_window * 64 + PAGE_SIZE, PAGE_SIZE);
}

void run_mpmc_simple_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* /*lock_counts*/,
    const MpmcSimplePipelineConfig& config
) {
    const auto& conns = client.connections();
    if (conns.empty())
        throw std::runtime_error("mpmc_simple pipeline: no server connections");

    MpmcBuffers bufs{};
    bufs.base = static_cast<uint8_t*>(client.buffer());

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
            throw std::runtime_error("mpmc_simple pipeline: CQ poll failed");
        if (polled == 0) continue;

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[i];
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "mpmc_simple pipeline: WC error status="
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
                op.position = *bufs.atomic_result(op.slot);
                post_wait_turn(client, op, bufs, config.is_producer);
                break;

            case MpmcPhase::wait_turn: {
                const uint64_t expected = config.is_producer
                    ? op.position : op.position + 1;
                if (*bufs.atomic_result(op.slot) == expected) {
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
                finish_op();
                break;

            case MpmcPhase::read_data:
                post_advance_turn(client, op, bufs);
                break;

            case MpmcPhase::advance_turn:
                finish_op();
                break;

            case MpmcPhase::idle:
                break;
            }
        }
    }
}
