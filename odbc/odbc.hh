#pragma once
#include <optional>
#include <string>
#include <vector>
#include <map>
#include <sstream>
#include <functional>

#include <sql.h>
#include <sqlext.h>

#include "dialect.hh"
#include "odbc_helpers.hh"

struct BaseError : public std::runtime_error
{
public:
    template <class... Args>
    BaseError(Args... args)
        : std::runtime_error(to_str(args...))
    {
    }

private:
    template <class... Args>
    std::string to_str(Args... args)
    {
        std::ostringstream ss;
        ((ss << args << " "), ...);
        return ss.str();
    }
};

struct FatalError : public BaseError
{
    template <class... Args>
    FatalError(Args... args)
        : BaseError("[Fatal]", args...)
    {
    }
};

struct ResultBuffer
{
    struct Column
    {
        Column(size_t row_count, size_t buffer_sz, int type)
            : buffer_size(buffer_sz), buffers(row_count * buffer_size), indicators(row_count), buffer_type(type)
        {
        }

        bool is_null(int row) const;

        std::string to_string(int row) const;

        size_t buffer_size;
        int buffer_type;
        std::vector<uint8_t> buffers;
        std::vector<SQLLEN> indicators;
    };

    // TODO: Configurable?
    static constexpr size_t MAX_BATCH_ROWS = 1000000;
    static constexpr size_t BATCH_SIZE = 1024 * 1024 * 10;

    ResultBuffer(const std::vector<ColumnInfo> &infos);

    size_t buffer_size(const ColumnInfo &c) const;

    size_t row_count = 0;
    std::vector<Column> columns;
    std::vector<SQLUSMALLINT> row_status;
};

struct Output
{
    virtual bool send(const std::vector<ColumnInfo> &metadata, ResultBuffer &res, SQLULEN rows_fetched) = 0;
};

class ODBC : public Output
{
public:
    // Nulls are represented as empty std::optional values
    using Value = std::optional<std::string>;
    using Row = std::vector<Value>;
    using Result = std::vector<Row>;

    struct TextResult : public Output
    {
        bool send(const std::vector<ColumnInfo> &metadata, ResultBuffer &res, SQLULEN rows_fetched);
        Result result;
    };

    static constexpr size_t MAX_BATCH_SIZE = 0x10000;

    ODBC(std::string dsn);

    ~ODBC();

    bool connect();

    void disconnect();

    const std::string &error();

    std::optional<Result> query(const std::string &sql);

    bool prepare(const std::string &sql);

    bool stream(const std::string &query, Output *handler);

    bool commit();

    //
    // The Output API
    //

    bool send(const std::vector<ColumnInfo> &metadata, ResultBuffer &res, SQLULEN rows_fetched) override;

    //
    // Catalog functions
    //

    std::map<std::string, std::map<std::string, std::string>> drivers();

    std::optional<std::vector<CatalogStatistics>> statistics(std::string catalog, std::string schema, std::string table);

    std::optional<std::vector<CatalogPrimaryKey>> primary_keys(std::string catalog, std::string schema, std::string table);

    std::optional<Result> tables(std::string catalog, std::string schema, std::string table);

    std::optional<std::vector<CatalogColumn>> columns(std::string catalog, std::string schema, std::string table);

    //
    // Helpers
    //

    void print_columns(const std::vector<ColumnInfo> &columns);

    void print_result(const Result &result);

    std::optional<std::string> get_by_name(const Row &row, std::string name);

    //
    // Functions for table copying
    //

    void set_output(ODBC *other);

private:
    template <class Hndl>
    std::string get_error(int hndl_type, Hndl hndl)
    {
        std::ostringstream ss;
        SQLLEN n = 0;
        SQLGetDiagField(hndl_type, hndl, 0, SQL_DIAG_NUMBER, &n, 0, 0);

        for (int i = 0; i < n; i++)
        {
            SQLCHAR sqlstate[6];
            SQLCHAR msg[SQL_MAX_MESSAGE_LENGTH];
            SQLINTEGER native_error;
            SQLSMALLINT msglen = 0;

            if (SQLGetDiagRec(hndl_type, hndl, i + 1, sqlstate, &native_error,
                              msg, sizeof(msg), &msglen) != SQL_NO_DATA)
            {
                ss << "#" << sqlstate << ": " << native_error << ", " << msg;
            }
        }

        return ss.str();
    }

    template <class T>
    bool get_int_attr(int col, int attr, T *t)
    {
        bool ok = false;
        long value = 0;

        if (SQL_SUCCEEDED(SQLColAttribute(m_stmt, col, attr, nullptr, 0, nullptr, &value)))
        {
            *t = value;
            ok = true;
        }

        return ok;
    }

    template <class T>
    bool get_str_attr(int col, int attr, T *t)
    {
        bool ok = false;
        char value[256] = "";
        SQLSMALLINT valuelen = 0;

        if (SQL_SUCCEEDED(SQLColAttribute(m_stmt, col, attr, value, sizeof(value), &valuelen, nullptr)))
        {
            *t = std::string_view(value, valuelen);
            ok = true;
        }

        return ok;
    }

    std::optional<Result> read_response(SQLRETURN ret);
    bool process_response(SQLRETURN ret, Output *handler);
    std::vector<ColumnInfo> get_headers(int columns);
    bool get_normal_result(int columns, Output *handler);
    bool get_batch_result(int columns, Output *handler);
    bool data_truncation();
    bool can_batch();

    int64_t as_int(const Value &val) const;
    std::string as_string(const Value &val) const;

    SQLHENV m_env;
    SQLHDBC m_conn;
    SQLHSTMT m_stmt;
    std::string m_dsn;
    std::string m_error;
    ODBC *m_output = nullptr;

    std::unique_ptr<Translator> m_translator;

    std::vector<ColumnInfo> m_columns;
};

void copy_table(std::string src_dsn, std::string dest_dsn, std::unique_ptr<Translator> Translator, const std::vector<TableInfo> &tables);
