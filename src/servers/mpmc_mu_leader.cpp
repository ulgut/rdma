#include "rdma/servers/mpmc_mu_leader.h"

// MPMC MU leader: serializes all push/pop operations, replicates state changes
// to followers via RDMA WRITE, and responds to clients after all replicas ack.
//
// Push: write data + turn to local buffer, RDMA WRITE slot + tail to followers
// Pop:  read data, update turn + head locally, RDMA WRITE turn + head to followers
//
// All replication writes use IBV_SEND_INLINE to capture data at post time,
// which is required for correctness when pipelining multiple operations.

#include "rdma/common.h"
#include "rdma/mpmc_mu_encoding.h"

#include <algorithm>
#include <cstring>
#include <deque>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

struct PendingOp {
    uint16_t client_id;
    uint32_t req_id;
    uint8_t payload[56];
};

struct MutCtx {
    bool in_use = false;
    uint32_t generation = 0;
    MpmcMuOp op = MpmcMuOp::Push;
    uint16_t client_id = 0;
    uint32_t req_id = 0;
    uint32_t slot_idx = 0;
    uint8_t pop_data[56] = {};
    uint32_t pending_followers = 0;
};

struct Runtime {
    uint32_t node_id;
    uint8_t* buf;
    ibv_pd* pd;
    ibv_cq* cq;
    ibv_mr* mr;
    std::vector<RemoteConnection>& clients;
    std::vector<RemoteConnection>& peers;
    uint32_t num_clients;

    uint64_t head = 0;
    uint64_t tail = 0;

    std::vector<MpmcMuRequest> recv_buffers;
    ibv_mr* recv_mr = nullptr;

    std::vector<MutCtx> mutations;
    std::deque<uint32_t> free_mutations;

    std::deque<PendingOp> pending_pushes;
    std::deque<PendingOp> pending_pops;

    std::vector<size_t> follower_indices;
    std::vector<uint32_t> client_send_signal_counts;
};

// WR id tags — distinct from MU leader tags to avoid collisions in shared headers.
constexpr uint64_t RECV_TAG = 0xD1ULL;
constexpr uint64_t REPL_TAG = 0xD2ULL;
constexpr uint64_t RESP_TAG = 0xD3ULL;
constexpr uint64_t TAG_SHIFT = 56;
constexpr uint64_t REPL_GEN_SHIFT = 24;
constexpr uint64_t REPL_GEN_MASK = 0xFFFFFFFFULL;
constexpr uint64_t REPL_ID_MASK = 0xFFFFFFULL;

uint64_t make_recv_wr_id(uint16_t client_id, uint16_t recv_slot) {
    return (RECV_TAG << TAG_SHIFT)
         | (static_cast<uint64_t>(client_id) << 16)
         | static_cast<uint64_t>(recv_slot);
}

uint16_t recv_client_id(uint64_t wr_id) {
    return static_cast<uint16_t>((wr_id >> 16) & 0xFFFFu);
}

uint16_t recv_slot_index(uint64_t wr_id) {
    return static_cast<uint16_t>(wr_id & 0xFFFFu);
}

uint64_t make_repl_wr_id(uint32_t mut_id, uint32_t gen) {
    return (REPL_TAG << TAG_SHIFT)
         | ((static_cast<uint64_t>(gen) & REPL_GEN_MASK) << REPL_GEN_SHIFT)
         | (static_cast<uint64_t>(mut_id) & REPL_ID_MASK);
}

uint32_t repl_gen(uint64_t wr_id) {
    return static_cast<uint32_t>((wr_id >> REPL_GEN_SHIFT) & REPL_GEN_MASK);
}

uint32_t repl_mut_id(uint64_t wr_id) {
    return static_cast<uint32_t>(wr_id & REPL_ID_MASK);
}

bool is_recv(uint64_t wr_id) { return (wr_id >> TAG_SHIFT) == RECV_TAG; }
bool is_repl(uint64_t wr_id) { return (wr_id >> TAG_SHIFT) == REPL_TAG; }
bool is_resp(uint64_t wr_id) { return (wr_id >> TAG_SHIFT) == RESP_TAG; }

void post_recv(Runtime& rt, uint16_t client_id, uint16_t recv_slot) {
    auto& buffer = rt.recv_buffers[
        static_cast<size_t>(client_id) * MPMC_MU_SERVER_RECV_RING + recv_slot];

    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&buffer);
    sge.length = sizeof(MpmcMuRequest);
    sge.lkey = rt.recv_mr->lkey;

    ibv_recv_wr wr{}, *bad = nullptr;
    wr.wr_id = make_recv_wr_id(client_id, recv_slot);
    wr.sg_list = &sge;
    wr.num_sge = 1;

    if (ibv_post_recv(rt.clients[client_id].cm_id->qp, &wr, &bad))
        throw std::runtime_error("MpmcMuLeader: post_recv failed");
}

void send_response(Runtime& rt, const MpmcMuResponse& resp) {
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&resp);
    sge.length = sizeof(MpmcMuResponse);
    sge.lkey = 0;

    ibv_send_wr wr{}, *bad = nullptr;
    wr.wr_id = (RESP_TAG << TAG_SHIFT) | resp.client_id;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_INLINE;
    if (++rt.client_send_signal_counts[resp.client_id]
            % MPMC_MU_SERVER_SEND_SIGNAL_EVERY == 0)
        wr.send_flags |= IBV_SEND_SIGNALED;

    if (ibv_post_send(rt.clients[resp.client_id].cm_id->qp, &wr, &bad))
        throw std::runtime_error("MpmcMuLeader: send_response failed");
}

std::optional<uint32_t> alloc_mutation(Runtime& rt) {
    if (rt.free_mutations.empty()) return std::nullopt;
    uint32_t id = rt.free_mutations.front();
    rt.free_mutations.pop_front();
    auto& ctx = rt.mutations[id];
    ctx.in_use = true;
    ctx.generation++;
    ctx.pending_followers = 0;
    return id;
}

void release_mutation(Runtime& rt, uint32_t id) {
    rt.mutations[id].in_use = false;
    rt.free_mutations.push_back(id);
}

// Forward declaration — complete_mutation and try_service_pending are mutually recursive.
void try_service_pending(Runtime& rt);

void complete_mutation(Runtime& rt, uint32_t mut_id) {
    auto& ctx = rt.mutations[mut_id];

    MpmcMuResponse resp{};
    resp.op = static_cast<uint8_t>(ctx.op);
    resp.status = static_cast<uint8_t>(MpmcMuStatus::Ok);
    resp.client_id = ctx.client_id;
    resp.req_id = ctx.req_id;
    if (ctx.op == MpmcMuOp::Pop)
        std::memcpy(resp.payload, ctx.pop_data, MPMC_SLOT_DATA_SIZE);

    send_response(rt, resp);
    release_mutation(rt, mut_id);
    try_service_pending(rt);
}

// Replicate a push: slot data+turn (64B inline) then tail counter (8B inline signaled)
// per follower. Inline captures data at post time for pipeline safety.
void replicate_push(Runtime& rt, uint32_t mut_id, uint32_t idx) {
    auto& ctx = rt.mutations[mut_id];

    for (size_t fi = 0; fi < rt.follower_indices.size(); ++fi) {
        const size_t follower_idx = rt.follower_indices[fi];
        auto& follower = rt.peers[follower_idx];

        // Slot (turn + data) — 64B unsignaled inline
        {
            ibv_sge sge{};
            sge.addr = reinterpret_cast<uintptr_t>(
                rt.buf + mpmc_slot_turn_offset(idx));
            sge.length = MPMC_SLOT_SIZE;
            sge.lkey = 0;

            ibv_send_wr wr{}, *bad = nullptr;
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags = IBV_SEND_INLINE;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.wr.rdma.remote_addr = follower.remote_addr
                + mpmc_slot_turn_offset(idx);
            wr.wr.rdma.rkey = follower.rkey;

            if (ibv_post_send(follower.cm_id->qp, &wr, &bad))
                throw std::runtime_error("MpmcMuLeader: slot WRITE failed");
        }

        // Tail counter — 8B signaled inline
        {
            ibv_sge sge{};
            sge.addr = reinterpret_cast<uintptr_t>(
                rt.buf + mpmc_tail_offset());
            sge.length = sizeof(uint64_t);
            sge.lkey = 0;

            ibv_send_wr wr{}, *bad = nullptr;
            wr.wr_id = make_repl_wr_id(mut_id, ctx.generation);
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.wr.rdma.remote_addr = follower.remote_addr + mpmc_tail_offset();
            wr.wr.rdma.rkey = follower.rkey;

            if (ibv_post_send(follower.cm_id->qp, &wr, &bad))
                throw std::runtime_error("MpmcMuLeader: tail WRITE failed");
        }

        ctx.pending_followers++;
    }

    if (ctx.pending_followers == 0)
        complete_mutation(rt, mut_id);
}

// Replicate a pop: slot turn (8B inline) then head counter (8B inline signaled)
void replicate_pop(Runtime& rt, uint32_t mut_id, uint32_t idx) {
    auto& ctx = rt.mutations[mut_id];

    for (size_t fi = 0; fi < rt.follower_indices.size(); ++fi) {
        const size_t follower_idx = rt.follower_indices[fi];
        auto& follower = rt.peers[follower_idx];

        // Slot turn — 8B unsignaled inline
        {
            ibv_sge sge{};
            sge.addr = reinterpret_cast<uintptr_t>(
                rt.buf + mpmc_slot_turn_offset(idx));
            sge.length = sizeof(uint64_t);
            sge.lkey = 0;

            ibv_send_wr wr{}, *bad = nullptr;
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags = IBV_SEND_INLINE;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.wr.rdma.remote_addr = follower.remote_addr
                + mpmc_slot_turn_offset(idx);
            wr.wr.rdma.rkey = follower.rkey;

            if (ibv_post_send(follower.cm_id->qp, &wr, &bad))
                throw std::runtime_error("MpmcMuLeader: turn WRITE failed");
        }

        // Head counter — 8B signaled inline
        {
            ibv_sge sge{};
            sge.addr = reinterpret_cast<uintptr_t>(
                rt.buf + mpmc_head_offset());
            sge.length = sizeof(uint64_t);
            sge.lkey = 0;

            ibv_send_wr wr{}, *bad = nullptr;
            wr.wr_id = make_repl_wr_id(mut_id, ctx.generation);
            wr.opcode = IBV_WR_RDMA_WRITE;
            wr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
            wr.sg_list = &sge;
            wr.num_sge = 1;
            wr.wr.rdma.remote_addr = follower.remote_addr + mpmc_head_offset();
            wr.wr.rdma.rkey = follower.rkey;

            if (ibv_post_send(follower.cm_id->qp, &wr, &bad))
                throw std::runtime_error("MpmcMuLeader: head WRITE failed");
        }

        ctx.pending_followers++;
    }

    if (ctx.pending_followers == 0)
        complete_mutation(rt, mut_id);
}

void execute_push(Runtime& rt, uint16_t client_id, uint32_t req_id,
                  const uint8_t* payload) {
    if (rt.tail - rt.head >= MPMC_QUEUE_CAPACITY) {
        PendingOp op{};
        op.client_id = client_id;
        op.req_id = req_id;
        std::memcpy(op.payload, payload, MPMC_SLOT_DATA_SIZE);
        rt.pending_pushes.push_back(op);
        return;
    }

    auto mut_opt = alloc_mutation(rt);
    if (!mut_opt) {
        PendingOp op{};
        op.client_id = client_id;
        op.req_id = req_id;
        std::memcpy(op.payload, payload, MPMC_SLOT_DATA_SIZE);
        rt.pending_pushes.push_back(op);
        return;
    }

    const uint64_t pos = rt.tail++;
    const uint32_t idx = static_cast<uint32_t>(pos & MPMC_QUEUE_MASK);

    // Update local buffer
    std::memcpy(rt.buf + mpmc_slot_data_offset(idx), payload, MPMC_SLOT_DATA_SIZE);
    *reinterpret_cast<uint64_t*>(rt.buf + mpmc_slot_turn_offset(idx)) = pos + 1;
    *reinterpret_cast<uint64_t*>(rt.buf + mpmc_tail_offset()) = rt.tail;

    const uint32_t mut_id = *mut_opt;
    auto& ctx = rt.mutations[mut_id];
    ctx.op = MpmcMuOp::Push;
    ctx.client_id = client_id;
    ctx.req_id = req_id;
    ctx.slot_idx = idx;

    replicate_push(rt, mut_id, idx);
}

void execute_pop(Runtime& rt, uint16_t client_id, uint32_t req_id) {
    if (rt.head >= rt.tail) {
        PendingOp op{};
        op.client_id = client_id;
        op.req_id = req_id;
        rt.pending_pops.push_back(op);
        return;
    }

    auto mut_opt = alloc_mutation(rt);
    if (!mut_opt) {
        PendingOp op{};
        op.client_id = client_id;
        op.req_id = req_id;
        rt.pending_pops.push_back(op);
        return;
    }

    const uint64_t pos = rt.head++;
    const uint32_t idx = static_cast<uint32_t>(pos & MPMC_QUEUE_MASK);

    const uint32_t mut_id = *mut_opt;
    auto& ctx = rt.mutations[mut_id];

    // Read data before updating turn
    std::memcpy(ctx.pop_data, rt.buf + mpmc_slot_data_offset(idx),
                MPMC_SLOT_DATA_SIZE);

    // Update local buffer — recycle slot for next producer round
    *reinterpret_cast<uint64_t*>(rt.buf + mpmc_slot_turn_offset(idx))
        = pos + MPMC_QUEUE_CAPACITY;
    *reinterpret_cast<uint64_t*>(rt.buf + mpmc_head_offset()) = rt.head;

    ctx.op = MpmcMuOp::Pop;
    ctx.client_id = client_id;
    ctx.req_id = req_id;
    ctx.slot_idx = idx;

    replicate_pop(rt, mut_id, idx);
}

// After each completed mutation, try to drain pending queues.
// A completed push may unblock a pending pop (data available),
// and a completed pop may unblock a pending push (space available).
void try_service_pending(Runtime& rt) {
    while (!rt.pending_pops.empty() && rt.head < rt.tail) {
        auto mut_opt = alloc_mutation(rt);
        if (!mut_opt) break;

        auto pop = rt.pending_pops.front();
        rt.pending_pops.pop_front();

        const uint64_t pos = rt.head++;
        const uint32_t idx = static_cast<uint32_t>(pos & MPMC_QUEUE_MASK);

        const uint32_t mut_id = *mut_opt;
        auto& ctx = rt.mutations[mut_id];
        std::memcpy(ctx.pop_data, rt.buf + mpmc_slot_data_offset(idx),
                    MPMC_SLOT_DATA_SIZE);
        *reinterpret_cast<uint64_t*>(rt.buf + mpmc_slot_turn_offset(idx))
            = pos + MPMC_QUEUE_CAPACITY;
        *reinterpret_cast<uint64_t*>(rt.buf + mpmc_head_offset()) = rt.head;

        ctx.op = MpmcMuOp::Pop;
        ctx.client_id = pop.client_id;
        ctx.req_id = pop.req_id;
        ctx.slot_idx = idx;

        replicate_pop(rt, mut_id, idx);
    }

    while (!rt.pending_pushes.empty()
           && (rt.tail - rt.head) < MPMC_QUEUE_CAPACITY) {
        auto mut_opt = alloc_mutation(rt);
        if (!mut_opt) break;

        auto push = rt.pending_pushes.front();
        rt.pending_pushes.pop_front();

        const uint64_t pos = rt.tail++;
        const uint32_t idx = static_cast<uint32_t>(pos & MPMC_QUEUE_MASK);

        std::memcpy(rt.buf + mpmc_slot_data_offset(idx), push.payload,
                    MPMC_SLOT_DATA_SIZE);
        *reinterpret_cast<uint64_t*>(rt.buf + mpmc_slot_turn_offset(idx))
            = pos + 1;
        *reinterpret_cast<uint64_t*>(rt.buf + mpmc_tail_offset()) = rt.tail;

        const uint32_t mut_id = *mut_opt;
        auto& ctx = rt.mutations[mut_id];
        ctx.op = MpmcMuOp::Push;
        ctx.client_id = push.client_id;
        ctx.req_id = push.req_id;
        ctx.slot_idx = idx;

        replicate_push(rt, mut_id, idx);
    }
}

void handle_recv(Runtime& rt, const ibv_wc& wc) {
    const uint16_t client_id = recv_client_id(wc.wr_id);
    const uint16_t recv_slot = recv_slot_index(wc.wr_id);

    const MpmcMuRequest req = rt.recv_buffers[
        static_cast<size_t>(client_id) * MPMC_MU_SERVER_RECV_RING + recv_slot];
    post_recv(rt, client_id, recv_slot);

    if (req.op == static_cast<uint8_t>(MpmcMuOp::Push)) {
        execute_push(rt, req.client_id, req.req_id, req.payload);
    } else if (req.op == static_cast<uint8_t>(MpmcMuOp::Pop)) {
        execute_pop(rt, req.client_id, req.req_id);
    }
}

void handle_repl(Runtime& rt, const ibv_wc& wc) {
    const uint32_t mut_id = repl_mut_id(wc.wr_id);
    if (mut_id >= rt.mutations.size()) return;

    auto& ctx = rt.mutations[mut_id];
    if (!ctx.in_use || ctx.generation != repl_gen(wc.wr_id)) return;
    if (ctx.pending_followers == 0) return;

    ctx.pending_followers--;
    if (ctx.pending_followers == 0)
        complete_mutation(rt, mut_id);
}

} // namespace

void MpmcMuLeader::pre_run() {
    auto* base = static_cast<uint8_t*>(buf_);

    *reinterpret_cast<volatile uint64_t*>(base + mpmc_tail_offset()) = 0;
    *reinterpret_cast<volatile uint64_t*>(base + mpmc_head_offset()) = 0;

    for (uint32_t i = 0; i < MPMC_QUEUE_CAPACITY; ++i) {
        *reinterpret_cast<volatile uint64_t*>(base + mpmc_slot_turn_offset(i)) = i;
        std::memset(base + mpmc_slot_data_offset(i), 0, MPMC_SLOT_DATA_SIZE);
    }

    std::cout << "[MpmcMuLeader " << node_id_ << "] Queue initialized ("
              << MPMC_QUEUE_CAPACITY << " slots)\n";
}

void MpmcMuLeader::run() {
    const uint32_t num_clients = expected_clients();

    Runtime rt{
        .node_id = node_id_,
        .buf = static_cast<uint8_t*>(buf_),
        .pd = pd_,
        .cq = cq_,
        .mr = mr_,
        .clients = clients_,
        .peers = peers_,
        .num_clients = num_clients,
        .recv_buffers = std::vector<MpmcMuRequest>(
            num_clients * MPMC_MU_SERVER_RECV_RING),
        .mutations = std::vector<MutCtx>(MPMC_MU_MUTATION_POOL),
        .client_send_signal_counts = std::vector<uint32_t>(num_clients, 0),
    };

    rt.free_mutations.resize(MPMC_MU_MUTATION_POOL);
    for (uint32_t i = 0; i < MPMC_MU_MUTATION_POOL; ++i)
        rt.free_mutations[i] = i;

    for (size_t i = 0; i < rt.peers.size(); ++i) {
        if (i == rt.node_id || rt.peers[i].cm_id == nullptr) continue;
        rt.follower_indices.push_back(i);
    }

    rt.recv_mr = ibv_reg_mr(
        rt.pd,
        rt.recv_buffers.data(),
        rt.recv_buffers.size() * sizeof(MpmcMuRequest),
        IBV_ACCESS_LOCAL_WRITE);
    if (!rt.recv_mr)
        throw std::runtime_error("MpmcMuLeader: recv_mr registration failed");

    for (uint16_t c = 0; c < num_clients; ++c) {
        for (uint16_t s = 0; s < MPMC_MU_SERVER_RECV_RING; ++s)
            post_recv(rt, c, s);
    }

    std::cout << "[MpmcMuLeader " << node_id_ << "] Ready — "
              << rt.follower_indices.size() << " followers, "
              << num_clients << " clients\n";

    ibv_wc wc[64];
    while (true) {
        const int n = ibv_poll_cq(rt.cq, 64, wc);
        if (n < 0)
            throw std::runtime_error("MpmcMuLeader: CQ poll failed");

        for (int i = 0; i < n; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    std::string("MpmcMuLeader: WC error ")
                    + ibv_wc_status_str(wc[i].status));
            }

            if ((wc[i].opcode & IBV_WC_RECV) != 0) {
                if (is_recv(wc[i].wr_id))
                    handle_recv(rt, wc[i]);
                continue;
            }

            if (is_resp(wc[i].wr_id)) continue;

            if (is_repl(wc[i].wr_id))
                handle_repl(rt, wc[i]);
        }
    }
}
