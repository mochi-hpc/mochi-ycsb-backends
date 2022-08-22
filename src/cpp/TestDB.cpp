#include "MochiYCSB.hpp"

#include <map>
#include <unordered_map>

namespace mochi {
namespace ycsb {

class TestDB : public DB {

    struct Entry {

        std::string table;
        std::string key;

        bool operator==(const Entry& other) const {
            return table == other.table && key == other.key;
        }

        bool operator<(const Entry& other) const {
            return (table < other.table)
                || ((table == other.table) && key < other.key);
        }
    };

    using Record = std::unordered_map<std::string, std::string>;

    std::map<Entry, Record> m_data;

    public:

    TestDB(const Properties& properties) {
        (void)properties;
    }

    Status read(StringView table,
                StringView key,
                const std::vector<StringView>& fields,
                DB::Record& result) const override {
        auto entry = Entry{table, key};
        auto it = m_data.find(entry);
        if(it == m_data.end()) {
            return Status{"NOT_FOUND", "The requested record was not found."};
        }
        const auto& record = it->second;
        for(const auto& field : fields) {
            auto field_pair = record.find(std::string(field.data(), field.size()));
            if(field_pair != record.end()) {
                result.emplace_back(
                    field_pair->first,
                    std::make_unique<StringBuffer>(field_pair->second));
            }
        }
        return Status::OK();
    }

    Status read(StringView table,
                StringView key,
                DB::Record& result) const override {
        auto entry = Entry{table, key};
        auto it = m_data.find(entry);
        if(it == m_data.end()) {
            return Status{"NOT_FOUND", "The requested record was not found."};
        }
        const auto& record = it->second;
        for(const auto& p : record) {
            result.emplace_back(
                p.first,
                std::make_unique<StringBuffer>(p.second));
        }
        return Status::OK();
    }

    Status scan(StringView table,
                StringView startKey,
                int recordCount,
                const std::vector<StringView>& fields,
                std::vector<DB::Record>& result) const override {
        auto startEntry = Entry{table, startKey};
        auto it = m_data.lower_bound(startEntry);
        unsigned i = 0;
        for(unsigned i = 0; i < recordCount && it != m_data.end(); ++i, ++it) {
            auto& record = it->second;
            DB::Record result_record;
            for(const auto& field : fields) {
                auto field_pair = record.find(std::string(field.data(), field.size()));
                if(field_pair != record.end()) {
                    result_record.emplace_back(
                        field_pair->first,
                        std::make_unique<StringBuffer>(field_pair->second));
                }
            }
            result.push_back(std::move(result_record));
        }
        return Status::OK();
    }

    Status scan(StringView table,
                StringView startKey,
                int recordCount,
                std::vector<DB::Record>& result) const override {
        auto startEntry = Entry{table, startKey};
        auto it = m_data.lower_bound(startEntry);
        unsigned i = 0;
        for(unsigned i = 0; i < recordCount && it != m_data.end(); ++i, ++it) {
            auto& record = it->second;
            DB::Record result_record;
            for(auto& field_pair : record) {
                result_record.emplace_back(
                    field_pair.first,
                    std::make_unique<StringBuffer>(field_pair.second));
            }
            result.push_back(std::move(result_record));
        }
        return Status::OK();
    }

    Status update(StringView table,
                  StringView key,
                  const std::vector<StringView>& fields,
                  const std::vector<StringView>& values) override {
        auto entry = Entry{table, key};
        auto it = m_data.find(entry);
        if(it == m_data.end()) {
            return Status{"NOT_FOUND", "The requested record was not found."};
        }
        auto& record = it->second;
        unsigned i = 0;
        for(unsigned i=0; i < fields.size(); i++) {
            const auto& field = fields[i];
            const auto& value = values[i];
            record[(std::string)field] = value;
        }
        return Status::OK();
    }

    Status insert(StringView table,
                  StringView key,
                  const std::vector<StringView>& fields,
                  const std::vector<StringView>& values) override {
        auto entry = Entry{table, key};
        auto record = Record{};
        unsigned i = 0;
        for(unsigned i=0; i < fields.size(); i++) {
            const auto& field = fields[i];
            const auto& value = values[i];
            record[(std::string)field] = value;
        }
        m_data[entry] = std::move(record);
        return Status::OK();
    }

    Status erase(StringView table,
                 StringView key) override {
        auto entry = Entry{table, key};
        auto it = m_data.find(entry);
        if(it == m_data.end()) {
            return Status{"NOT_FOUND", "The requested record was not found."};
        }
        m_data.erase(it);
        return Status::OK();
    }

};

MOCHI_YCSB_REGISTER_DB_TYPE(test, TestDB);

}
}
