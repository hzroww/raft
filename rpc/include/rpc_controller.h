#pragma once

#include <google/protobuf/service.h>
#include <string>

class RpcController : public google::protobuf::RpcController {
public:
    RpcController() : failed_(false) {}

    void Reset() override { failed_ = false; error_text_.clear(); }
    bool Failed() const override { return failed_; }
    std::string ErrorText() const override { return error_text_; }
    void StartCancel() override {}
    void SetFailed(const std::string& reason) override { failed_ = true; error_text_ = reason; }
    bool IsCanceled() const override { return false; }
    void NotifyOnCancel(google::protobuf::Closure* callback) override {}

private:
    bool failed_;
    std::string error_text_;
};
