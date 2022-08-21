#include "MochiYCSB.hpp"

#include <map>
#include <unordered_map>
#include <iostream>

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

    Status read(StringView table,
                StringView key,
                const std::vector<std::string>& fields,
                FieldValueList& result) const override {
        auto entry = Entry{table, key};
        auto it = m_data.find(entry);
        if(it == m_data.end()) {
            return Status{"NOT_FOUND", "The requested record was not found."};
        }
        const auto& record = it->second;
        for(const auto& field : fields) {
            auto field_pair = record.find(field);
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
                FieldValueList& result) const override {
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
                const std::vector<std::string>& fields,
                std::vector<FieldValueList>& result) const override {
        auto startEntry = Entry{table, startKey};
        auto it = m_data.lower_bound(startEntry);
        unsigned i = 0;
        for(unsigned i = 0; i < recordCount && it != m_data.end(); ++i, ++it) {
            auto& record = it->second;
            FieldValueList field_values;
            for(const auto& field : fields) {
                auto field_pair = record.find(field);
                if(field_pair != record.end()) {
                    field_values.emplace_back(
                        field_pair->first,
                        std::make_unique<StringBuffer>(field_pair->second));
                }
            }
            result.push_back(std::move(field_values));
        }
        return Status::OK();
    }

    Status scan(StringView table,
                StringView startKey,
                int recordCount,
                std::vector<FieldValueList>& result) const override {
        auto startEntry = Entry{table, startKey};
        auto it = m_data.lower_bound(startEntry);
        unsigned i = 0;
        for(unsigned i = 0; i < recordCount && it != m_data.end(); ++i, ++it) {
            auto& record = it->second;
            FieldValueList field_values;
            for(auto& field_pair : record) {
                field_values.emplace_back(
                    field_pair.first,
                    std::make_unique<StringBuffer>(field_pair.second));
            }
            result.push_back(std::move(field_values));
        }
        return Status::OK();
    }

    Status update(StringView table,
                  StringView key,
                  const FieldValueList& values) override {
        auto entry = Entry{table, key};
        auto it = m_data.find(entry);
        if(it == m_data.end()) {
            return Status{"NOT_FOUND", "The requested record was not found."};
        }
        auto& record = it->second;
        for(const auto& p : values) {
            record[p.first] = std::string(p.second->data(), p.second->size());
        }
        return Status::OK();
    }

    Status insert(StringView table,
                  StringView key,
                  const FieldValueList& values) override {
        auto entry = Entry{table, key};
        auto record = Record{};
        for(const auto& p : values) {
            record[p.first] = std::string(p.second->data(), p.second->size());
            std::cout << "  " << p.first << " -> " << std::string(p.second->data(), p.second->size()) << std::endl;
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
