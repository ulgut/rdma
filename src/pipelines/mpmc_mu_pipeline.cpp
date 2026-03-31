#include "rdma/pipelines/mpmc_mu_pipeline.h"

// Client-side MPMC MU pipeline: SEND/RECV RPCs to the leader.
// Each op is a single request-response: push sends data, pop receives data.

#include "rdma/client.h"
#include "rdma/common.h"
#include "rdma/mpmc_mu_encoding.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

constexpr uint64_t CLIENT_RECV_TAG = 0xE1ULL;
constexpr uint64_t CLIENT_SEND_TAG = 0xE2ULL;
constexpr uint64_t CLIENT_TAG_SHIFT = 56;
constexpr size_t CLIENT_RECV_RING_MIN = 32;

uint64_t make_recv_wr_id(uint32_t recv_slot) {
    return (CLIENT_RECV_TAG << CLIENT_TAG_SHIFT) | recv_slot;
}

bool is_recv(uint64_t wr_id) {
    return (wr_id >> CLIENT_TAG_SHIFT) == CLIENT_RECV_TAG;
}

uint32_t recv_slot(uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id & 0xFFFFFFFFu);
}

struct MpmcMuOpCtx {
    bool active = false;
    uint32_t generation = 0;
    uint32_t slot = 0;
    uint32_t req_id = 0;
    size_t latency_index = 0;
    std::chrono::steady_clock::time_point started_at{};
};

void post_recv(Client& client, MpmcMuResponse* response, uint32_t slot) {
    auto& leader = client.connections().front();

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(response);
    sge.length = sizeof(MpmcMuResponse);
    sge.lkey = client.mr()->lkey;

    ibv_recv_wr wr{}, *bad = nullptr;
    wr.wr_id = make_recv_wr_id(slot);
    wr.sg_list = &sge;
    wr.num_sge = 1;

    if (ibv_post_recv(leader.id->qp, &wr, &bad))
        throw std::runtime_error("mpmc_mu pipeline: post_recv failed");
}

void post_request(
    Client& client,
    const MpmcMuRequest& request,
    uint32_t& signal_count,
    uint32_t signal_every
) {
    auto& leader = client.connections().front();

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&request);
    sge.length = sizeof(MpmcMuRequest);
    sge.lkey = 0;

    ibv_send_wr wr{}, *bad = nullptr;
    wr.wr_id = (CLIENT_SEND_TAG << CLIENT_TAG_SHIFT) | signal_count;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_INLINE;
    if (++signal_count % std::max(signal_every, 1u) == 0)
        wr.send_flags |= IBV_SEND_SIGNALED;

    if (ibv_post_send(leader.id->qp, &wr, &bad))
        throw std::runtime_error("mpmc_mu pipeline: post_request failed");
}

} // namespace

MpmcMuPipelineConfig load_mpmc_mu_pipeline_config() {
    const size_t clients_per_machine = get_uint_env_or("CLIENTS_PER_MACHINE", NUM_CLIENTS_PER_MACHINE);
    const size_t total_clients = clients_per_machine * get_uint_env_or("TOTAL_CLIENT_MACHINES", static_cast<unsigned int>(TOTAL_CLIENT_MACHINES));
    const size_t num_ops = get_uint_env_or("NUM_OPS", NUM_OPS);
    return {
        .is_producer = get_uint_env_or("MPMC_IS_PRODUCER", 1) != 0,
        .queue_capacity = MPMC_QUEUE_CAPACITY,
        .active_window = std::max<size_t>(1, get_uint_env_or("MPMC_ACTIVE_WINDOW", MPMC_MU_ACTIVE_WINDOW)),
        .cq_batch = std::max<size_t>(1, MPMC_MU_CQ_BATCH),
        .num_ops = num_ops / total_clients,
        .client_send_signal_every = MPMC_MU_CLIENT_SEND_SIGNAL_EVERY,
    };
}

size_t mpmc_mu_pipeline_client_buffer_size(const MpmcMuPipelineConfig& config) {
    const size_t recv_ring = std::max(config.active_window * 2, CLIENT_RECV_RING_MIN);
    const size_t response_bytes = align_up(recv_ring * sizeof(MpmcMuResponse), 64);
    return align_up(response_bytes + PAGE_SIZE, PAGE_SIZE);
}

void run_mpmc_mu_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* /*lock_counts*/,
    const MpmcMuPipelineConfig& config
) {
    if (client.connections().empty())
        throw std::runtime_error("mpmc_mu pipeline: no leader connection");

    const size_t recv_ring = std::max(config.active_window * 2, CLIENT_RECV_RING_MIN);
    auto* recv_base = static_cast<MpmcMuResponse*>(client.buffer());

    std::vector<MpmcMuOpCtx> ops(config.active_window);
    std::vector<ibv_wc> completions(config.cq_batch);
    std::unordered_map<uint32_t, uint32_t> req_to_slot;
    req_to_slot.reserve(config.active_window * 2);

    uint32_t signal_count = 0;
    size_t submitted = 0;
    size_t completed = 0;
    size_t active = 0;
    uint32_t next_req_id = 1;

    // Pre-post receive ring
    for (uint32_t r = 0; r < recv_ring; ++r)
        post_recv(client, &recv_base[r], r);

    auto submit_op = [&](size_t slot) {
        auto& op = ops[slot];
        op.active = true;
        op.generation++;
        op.slot = static_cast<uint32_t>(slot);
        op.req_id = next_req_id++;
        op.latency_index = submitted;
        op.started_at = std::chrono::steady_clock::now();
        req_to_slot[op.req_id] = op.slot;

        MpmcMuRequest req{};
        req.op = static_cast<uint8_t>(
            config.is_producer ? MpmcMuOp::Push : MpmcMuOp::Pop);
        req.client_id = static_cast<uint16_t>(client.id());
        req.req_id = op.req_id;

        if (config.is_producer) {
            // Fill payload with identifiable data
            uint64_t val = (static_cast<uint64_t>(client.id()) << 32)
                         | static_cast<uint32_t>(op.latency_index);
            std::memcpy(req.payload, &val, sizeof(val));
            std::memset(req.payload + sizeof(val), 0,
                        MPMC_SLOT_DATA_SIZE - sizeof(val));
        }

        post_request(client, req, signal_count, config.client_send_signal_every);
        submitted++;
        active++;
    };

    // Fill active window
    while (active < config.active_window && submitted < config.num_ops)
        submit_op(active);

    while (completed < config.num_ops) {
        const int polled = ibv_poll_cq(client.cq(),
            static_cast<int>(completions.size()), completions.data());
        if (polled < 0)
            throw std::runtime_error("mpmc_mu pipeline: CQ poll failed");
        if (polled == 0) continue;

        for (int i = 0; i < polled; ++i) {
            const ibv_wc& wc = completions[i];
            if (wc.status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    "mpmc_mu pipeline: WC error status="
                    + std::to_string(wc.status)
                    + " opcode=" + std::to_string(wc.opcode));
            }

            // Only process recv completions; send completions are ignored.
            if (!(wc.opcode & IBV_WC_RECV)) continue;
            if (!is_recv(wc.wr_id)) continue;

            const uint32_t rslot = recv_slot(wc.wr_id);
            const MpmcMuResponse& resp = recv_base[rslot];

            // Re-post the recv buffer immediately
            post_recv(client, &recv_base[rslot], rslot);

            // Match response to op by req_id
            auto it = req_to_slot.find(resp.req_id);
            if (it == req_to_slot.end()) continue;

            const uint32_t op_slot = it->second;
            req_to_slot.erase(it);

            auto& op = ops[op_slot];
            if (!op.active) continue;

            latencies[op.latency_index] = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - op.started_at).count());

            op.active = false;
            completed++;
            active--;

            if (submitted < config.num_ops)
                submit_op(op_slot);
        }
    }
}
