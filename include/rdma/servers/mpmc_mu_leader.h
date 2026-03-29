#pragma once

#include "rdma/server.h"

class MpmcMuLeader final : public Server {
public:
    explicit MpmcMuLeader(uint32_t node_id) : Server(node_id) {}

protected:
    [[nodiscard]] uint32_t expected_clients() const override {
        return get_uint_env_or("CLIENTS_PER_MACHINE", NUM_CLIENTS_PER_MACHINE) * TOTAL_CLIENT_MACHINES;
    }
    void pre_run() override;
    void run() override;
};
