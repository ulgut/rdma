#pragma once

// Non-replicated MPMC bounded queue (Rigtorp-style) over RDMA.
// Single server, completion-driven state machine with configurable active window.

#include <cstddef>
#include <cstdint>

class Client;

struct MpmcSimplePipelineConfig {
    bool is_producer = true;
    size_t queue_capacity = 0;
    size_t active_window = 1;
    size_t cq_batch = 32;
    size_t num_ops = 0;
};

[[nodiscard]] MpmcSimplePipelineConfig load_mpmc_simple_pipeline_config();
[[nodiscard]] size_t mpmc_simple_pipeline_client_buffer_size(const MpmcSimplePipelineConfig& config);

void run_mpmc_simple_pipeline(
    Client& client,
    uint64_t* latencies,
    uint64_t* lock_counts,
    const MpmcSimplePipelineConfig& config
);
