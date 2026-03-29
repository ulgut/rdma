#pragma once

// Client-side RPC pipeline for the MPMC MU (leader-based) design.
// Clients send push/pop requests via SEND to the leader and receive
// responses via RECV after the leader replicates to all followers.

#include <cstddef>
#include <cstdint>

class Client;

struct MpmcMuPipelineConfig {
    bool is_producer = true;
    size_t queue_capacity = 0;
    size_t active_window = 1;
    size_t cq_batch = 32;
    size_t num_ops = 0;
    uint32_t client_send_signal_every = 64;
};

[[nodiscard]] MpmcMuPipelineConfig load_mpmc_mu_pipeline_config();
[[nodiscard]] size_t mpmc_mu_pipeline_client_buffer_size(const MpmcMuPipelineConfig& config);

void run_mpmc_mu_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const MpmcMuPipelineConfig& config
);
