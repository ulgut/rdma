#pragma once

// Phase-based replicated MPMC queue pipeline (Synra-style).
// All RDMA ops broadcast to all replicas. Configurable active_window.
// active_window=1 is equivalent to the synchronous version.

#include <cstddef>
#include <cstdint>

class Client;

struct MpmcSynraPipelineConfig {
    bool is_producer = true;
    size_t queue_capacity = 0;
    size_t active_window = 1;
    size_t cq_batch = 32;
};

[[nodiscard]] MpmcSynraPipelineConfig load_mpmc_synra_pipeline_config();
[[nodiscard]] size_t mpmc_synra_pipeline_client_buffer_size(const MpmcSynraPipelineConfig& config);

void run_mpmc_synra_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const MpmcSynraPipelineConfig& config
);
