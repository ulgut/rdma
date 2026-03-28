#pragma once

// Replicated MPMC bounded queue (Rigtorp-style) over RDMA.
// FAA on primary, CAS-replicate counter to all replicas, then broadcast
// all subsequent phases. Configurable active window.

#include <cstddef>
#include <cstdint>

class Client;

struct MpmcSynraPipelineConfig {
    bool is_producer = true;
    size_t queue_capacity = 0;
    size_t active_window = 1;
    size_t cq_batch = 32;
    size_t num_ops = 0;
};

[[nodiscard]] MpmcSynraPipelineConfig load_mpmc_synra_pipeline_config();
[[nodiscard]] size_t mpmc_synra_pipeline_client_buffer_size(const MpmcSynraPipelineConfig& config);

void run_mpmc_synra_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const MpmcSynraPipelineConfig& config
);
