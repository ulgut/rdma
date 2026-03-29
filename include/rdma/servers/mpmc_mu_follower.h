#pragma once

#include "rdma/server.h"

class MpmcMuFollower final : public Server {
public:
    explicit MpmcMuFollower(uint32_t node_id) : Server(node_id) {}

protected:
    [[nodiscard]] uint32_t expected_clients() const override { return 0; }
    void pre_run() override;
    void run() override;
};
