#pragma once

// Shared message types for the MPMC MU (leader-based) pipeline.
// 64B request/response fits within MAX_INLINE_DEPTH for inline SEND.

#include <cstddef>
#include <cstdint>

enum class MpmcMuOp : uint8_t { Push = 1, Pop = 2 };
enum class MpmcMuStatus : uint8_t { Ok = 0, InternalError = 1 };

struct MpmcMuRequest {
    uint8_t op;
    uint8_t reserved;
    uint16_t client_id;
    uint32_t req_id;
    uint8_t payload[56];
};

struct MpmcMuResponse {
    uint8_t op;
    uint8_t status;
    uint16_t client_id;
    uint32_t req_id;
    uint8_t payload[56];
};

static_assert(sizeof(MpmcMuRequest) == 64);
static_assert(sizeof(MpmcMuResponse) == 64);

constexpr size_t MPMC_MU_SERVER_RECV_RING = 64;
constexpr size_t MPMC_MU_MUTATION_POOL = 512;
constexpr uint32_t MPMC_MU_SERVER_SEND_SIGNAL_EVERY = 128;
