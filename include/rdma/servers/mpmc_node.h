#pragma once

#include "rdma/server.h"

class MpmcNode final : public Server {
public:
    explicit MpmcNode(const uint32_t node_id) : Server(node_id) {}

protected:
    [[nodiscard]] uint32_t expected_clients() const override {
        return get_uint_env_or("CLIENTS_PER_MACHINE", NUM_CLIENTS_PER_MACHINE) * TOTAL_CLIENT_MACHINES;
    }
    [[nodiscard]] size_t expected_servers() const override {
        const char* s = std::getenv("STRATEGY");
        if (s && std::string(s) == "mpmc_simple") return 1;
        return CLUSTER_NODES.size();
    }
    void pre_run() override;
    void run() override;
};
