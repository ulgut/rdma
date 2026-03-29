#include "rdma/servers/mpmc_mu_follower.h"

#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

void MpmcMuFollower::pre_run() {
    auto* base = static_cast<uint8_t*>(buf_);

    *reinterpret_cast<volatile uint64_t*>(base + mpmc_tail_offset()) = 0;
    *reinterpret_cast<volatile uint64_t*>(base + mpmc_head_offset()) = 0;

    for (uint32_t i = 0; i < MPMC_QUEUE_CAPACITY; ++i) {
        *reinterpret_cast<volatile uint64_t*>(base + mpmc_slot_turn_offset(i)) = i;
        std::memset(base + mpmc_slot_data_offset(i), 0, MPMC_SLOT_DATA_SIZE);
    }

    std::cout << "[MpmcMuFollower " << node_id_ << "] Queue initialized\n";
}

void MpmcMuFollower::run() {
    std::cout << "[MpmcMuFollower " << node_id_ << "] Passive mode\n";

    ibv_wc wc[32];
    while (true) {
        const int n = ibv_poll_cq(cq_, 32, wc);
        if (n < 0) throw std::runtime_error("MpmcMuFollower: CQ poll failed");

        for (int i = 0; i < n; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                throw std::runtime_error(
                    std::string("MpmcMuFollower: WC error ")
                    + ibv_wc_status_str(wc[i].status));
            }
        }
    }
}
