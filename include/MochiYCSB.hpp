#ifndef MOCHI_YCSB_HPP
#define MOCHI_YCSB_HPP

#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <cstring>
#include <unordered_map>

namespace mochi {
namespace ycsb {

/**
 * @brief C++ equivalent of java.site.ycsb.Status.
 */
struct Status {

    std::string name;
    std::string description;

    Status();
    Status(std::string _name, std::string _description);
    Status(const Status&);
    Status(Status&&);
    Status& operator=(const Status&);
    Status& operator=(Status&&);
    ~Status();

    static Status OK();
};

/**
 * @brief Abstract class for a buffer, which can
 * be implemented by the actual DB backend to, e.g.,
 * wrap a pre-allocated RDMA buffer with managed ownership.
 */
class Buffer {

    public:

    /**
     * @brief Destructor.
     */
    virtual ~Buffer();

    /**
     * @brief Get the internal data held by the Buffer.
     */
    virtual const char* data() const = 0;

    /**
     * @brief Get the size of the data.
     */
    virtual size_t size() const = 0;

};

/**
 * @brief Implementation of the Buffer interface
 * wrapping a simple std::string.
 */
class StringBuffer : public Buffer {

    std::string m_str;

    public:

    StringBuffer(std::string str)
    : m_str(std::move(str)) {}

    const char* data() const override {
        return m_str.data();
    }

    size_t size() const override {
        return m_str.size();
    }
};

/**
 * @brief Wrapper for a memory region.
 */
class StringView : public Buffer {

    const char* m_data;
    size_t      m_size;

    public:

    StringView(const char* data, size_t size)
    : m_data(data)
    , m_size(size) {}

    StringView(const char* data)
    : m_data(data)
    , m_size(std::strlen(data)) {}

    const char* data() const override {
        return m_data;
    }

    size_t size() const override {
        return m_size;
    }

    operator std::string() const {
        return std::string(m_data, m_size);
    }
};

/**
 * @brief C++ equivalent of the site.ycsb.DB Java class.
 */
class DB {

    public:

    virtual ~DB();

    using FieldValueList = std::vector<std::pair<std::string, std::unique_ptr<Buffer>>>;

    /**
     * Read a record from the database. Each field/value pair from the result
     * will be stored in the result vector.
     *
     * @param table The name of the table
     * @param key The record key of the record to read
     * @param fields The list of fields to read
     * @param result A list of field/value pairs for the result
     * @return The result of the operation
     */
    virtual Status read(StringView table,
                        StringView key,
                        const std::vector<StringView>& fields,
                        FieldValueList& result) const = 0;

    /**
     * Read a record from the database. All the field/value pairs from the result
     * will be stored in a vector.
     *
     * @param table The name of the table
     * @param key The record key of the record to read
     * @param result A list of field/value pairs for the result
     * @return The result of the operation
     */
    virtual Status read(StringView table,
                        StringView key,
                        FieldValueList& result) const = 0;

    /**
     * Perform a range scan for a set of records in the database.
     * Each field/value pair from the result will be stored
     * in a vector.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read
     * @param recordCount The number of records to read
     * @param fields The list of fields to read
     * @param result A vector of FieldValueList, where each FieldValueList
     *               is a list of field/value pairs for one record
     * @return The result of the operation
     */
    virtual Status scan(StringView table,
                        StringView startKey,
                        int recordCount,
                        const std::vector<StringView>& fields,
                        std::vector<FieldValueList>& result) const = 0;

    /**
     * Perform a range scan for a set of records in the database.
     * All the field/value pairs from the result will be stored
     * in a std::unordered_map.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read
     * @param recordcount The number of records to read
     * @param result A vector of FieldValueList, where each FieldValueList
     *               is a list of field/value pairs for one record
     * @return The result of the operation
     */
    virtual Status scan(StringView table,
                        StringView startKey,
                        int recordCount,
                        std::vector<FieldValueList>& result) const = 0;

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values std::unordered_map will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write
     * @param fields Fields to write
     * @param values Values corresponding to each field
     * @return The result of the operation
     */
    virtual Status update(StringView table,
                          StringView key,
                          const std::vector<StringView>& fields,
                          const std::vector<StringView>& values) = 0;

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * FieldValueList will be written into the record with the specified record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert
     * @param fields Fields to write
     * @param values Values corresponding to each field
     * @return The result of the operation
     */
    virtual Status insert(StringView table,
                          StringView key,
                          const std::vector<StringView>& fields,
                          const std::vector<StringView>& values) = 0;

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete
     * @return The result of the operation
     */
    virtual Status erase(StringView table,
                         StringView key) = 0;
};

using Properties = std::unordered_map<std::string, std::string>;

/**
 * @brief This function is used by the MochiDBRegistry
 * to automatically register a new DB implementation type
 * and associate it with a name.
 *
 * @param name Name of the backend
 * @param create Create function
 */
void RegisterDBType(const char* name,
                    std::function<DB* (const Properties&)> create);


/**
 * @brief Create a DB instance from the name of a backend.
 * If the backend doesn't exist, a null pointer will be returned.
 *
 * It is the responsibility of the caller to called delete on
 * the returned pointer when no longer needed.
 *
 * @param name Backend name
 * @param properties Properties
 * @return A pointer to a DB instance
 */
DB* CreateDB(const char* name,
             const Properties& properties);

template<typename T>
struct MochiDBRegistry {

    MochiDBRegistry(const char* name) {
        mochi::ycsb::RegisterDBType(name,
            [](const Properties& properties) -> mochi::ycsb::DB* {
                return new T(properties);
            });
    }

};

#define MOCHI_YCSB_REGISTER_DB_TYPE(__name__, __type__) \
    static ::mochi::ycsb::MochiDBRegistry<__type__> __name__##_registry_(#__name__)

}
}
#endif
