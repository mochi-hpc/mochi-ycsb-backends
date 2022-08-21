#include "MochiYCSB.hpp"
#include <unordered_map>
#include <functional>
#include <string>

namespace mochi {
namespace ycsb {

Status::Status() = default;

Status::Status(std::string _name, std::string _description)
: name(std::move(_name)), description(std::move(_description)) {}

Status::Status(const Status&) = default;

Status::Status(Status&&) = default;

Status& Status::operator=(const Status&) = default;

Status& Status::operator=(Status&&) = default;

Status::~Status() = default;

Status Status::OK() {
    return Status("OK", "The operation completed successfully.");
}

Buffer::~Buffer() = default;

DB::~DB() = default;

static std::unordered_map<
    std::string,
    std::function<DB* ()>> dbFactory;

void RegisterDBType(const char* name,
                    std::function<DB*()> create) {
    dbFactory[name] = std::move(create);
}

DB* CreateDB(const char* name) {
    auto it = dbFactory.find(name);
    if(it == dbFactory.end()) return nullptr;
    else return (it->second)();
}

}
}
