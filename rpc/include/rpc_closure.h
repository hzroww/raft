#pragma once

#include <google/protobuf/stubs/callback.h>
#include <functional>

// Self-deleting closure following protobuf's done-callback convention.
// Always allocate with `new`; never delete manually.
class RpcClosure : public google::protobuf::Closure {
public:
    explicit RpcClosure(std::function<void()> cb) : cb_(std::move(cb)) {}

    void Run() override {
        if (cb_) cb_();
        delete this;
    }

private:
    std::function<void()> cb_;
};
