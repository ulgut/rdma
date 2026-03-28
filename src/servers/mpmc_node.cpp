#include "rdma/servers/mpmc_node.h"

#include <cstring>
#include <iostream>
#include <stdexcept>

void MpmcNode::pre_run() {
    auto* base = static_cast<uint8_t*>(buf_);

    *reinterpret_cast<volatile uint64_t*>(base + mpmc_tail_offset()) = 0;
    *reinterpret_cast<volatile uint64_t*>(base + mpmc_head_offset()) = 0;

    for (uint32_t i = 0; i < MPMC_QUEUE_CAPACITY; ++i) {
        *reinterpret_cast<volatile uint64_t*>(base + mpmc_slot_turn_offset(i)) = i;
        std::memset(base + mpmc_slot_data_offset(i), 0, MPMC_SLOT_DATA_SIZE);
    }

    std::cout << "[MpmcNode " << node_id_ << "] Queue initialized ("
              << MPMC_QUEUE_CAPACITY << " slots, "
              << mpmc_queue_total_size() << " bytes)\n";
}

void MpmcNode::run() {
    // Passive mode — one-sided ops bypass the server CPU entirely.
    ibv_wc wc[32];
    while (true) {
        const int n = ibv_poll_cq(cq_, 32, wc);
        if (n < 0) throw std::runtime_error("ibv_poll_cq failed");

        for (int i = 0; i < n; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                std::cerr << "[MpmcNode] WC error: "
                          << ibv_wc_status_str(wc[i].status)
                          << " opcode: " << wc[i].opcode << "\n";
            }
        }
    }
}
