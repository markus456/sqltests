#include "odbc.hh"
#include "odbc_helpers.hh"
#include "dialect.hh"
#include "diagnostics.hh"
#include "string_utils.hh"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <utility>
#include <algorithm>
#include <chrono>

#include <unistd.h>

ColumnType sql_to_mariadb_type(int data_type, int size)
{
    switch (data_type)
    {
    case SQL_WCHAR:
    case SQL_CHAR:
        return ColumnType::CHAR;

    case SQL_GUID:
    case SQL_WVARCHAR:
    case SQL_VARCHAR:
        return ColumnType::VARCHAR;

    case SQL_WLONGVARCHAR:
    case SQL_LONGVARCHAR:
        if (size < 16384)
        {
            return ColumnType::VARCHAR;
        }
        else if (size < 65535)
        {
            return ColumnType::TEXT;
        }
        else if (size < 16777215)
        {
            return ColumnType::MEDIUMTEXT;
        }

        return ColumnType::LONGTEXT;

    case SQL_DECIMAL:
    case SQL_NUMERIC:
        return ColumnType::DECIMAL;

    case SQL_TINYINT:
        return ColumnType::TINYINT;

    case SQL_SMALLINT:
        return ColumnType::SMALLINT;

    case SQL_INTEGER:
        return ColumnType::INT;

    case SQL_BIGINT:
        return ColumnType::BIGINT;

    case SQL_FLOAT:
    case SQL_REAL:
        return ColumnType::FLOAT;

    case SQL_DOUBLE:
        return ColumnType::DOUBLE;

    case SQL_BIT:
        return ColumnType::BIT;

    case SQL_BINARY:
        return ColumnType::BINARY;

    case SQL_VARBINARY:
        return ColumnType::VARBINARY;

    case SQL_LONGVARBINARY:
        if (size < 16384)
        {
            return ColumnType::VARBINARY;
        }
        else if (size < 65535)
        {
            return ColumnType::BLOB;
        }
        else if (size < 16777215)
        {
            return ColumnType::MEDIUMBLOB;
        }
        else
        {
            return ColumnType::LONGBLOB;
        }
        break;

    case SQL_TYPE_DATE:
        return ColumnType::DATE;

#ifdef SQL_TYPE_UTCTIME
    case SQL_TYPE_UTCTIME:
#endif
    case SQL_TYPE_TIME:
        return ColumnType::TIME;

    case SQL_TYPE_TIMESTAMP:
        return ColumnType::TIMESTAMP;

#ifdef SQL_TYPE_UTCDATETIME
    case SQL_TYPE_UTCDATETIME:
#endif
    case SQL_INTERVAL_MONTH:
    case SQL_INTERVAL_YEAR:
    case SQL_INTERVAL_YEAR_TO_MONTH:
    case SQL_INTERVAL_DAY:
    case SQL_INTERVAL_HOUR:
    case SQL_INTERVAL_MINUTE:
    case SQL_INTERVAL_SECOND:
    case SQL_INTERVAL_DAY_TO_HOUR:
    case SQL_INTERVAL_DAY_TO_MINUTE:
    case SQL_INTERVAL_DAY_TO_SECOND:
    case SQL_INTERVAL_HOUR_TO_MINUTE:
    case SQL_INTERVAL_HOUR_TO_SECOND:
    case SQL_INTERVAL_MINUTE_TO_SECOND:
        return ColumnType::DATETIME;

    default:
        return ColumnType::UNKNOWN;
    }
}

std::string to_mariadb_type(const CatalogColumn &c)
{
    std::ostringstream ss;

    switch (c.DATA_TYPE)
    {
    case SQL_TINYINT:
        ss << "TINYINT";
        break;

    case SQL_SMALLINT:
        ss << "SMALLINT";
        break;

    case SQL_INTEGER:
        ss << "INT";
        break;

    case SQL_BIGINT:
        ss << "BIGINT";
        break;

    case SQL_FLOAT:
    case SQL_REAL:
        ss << "FLOAT";
        break;

    case SQL_DOUBLE:
        ss << "DOUBLE";
        break;

    case SQL_BIT:
        ss << "BIT";
        break;

    case SQL_WCHAR:
    case SQL_CHAR:
        ss << "CHAR(" << c.COLUMN_SIZE << ")";
        break;

    case SQL_GUID:
        ss << "CHAR(16)";
        break;

    case SQL_WVARCHAR:
    case SQL_VARCHAR:
        ss << "VARCHAR(" << c.COLUMN_SIZE << ")";
        break;

    case SQL_BINARY:
        ss << "BINARY(" << c.COLUMN_SIZE << ")";
        break;

    case SQL_VARBINARY:
        ss << "VARBINARY(" << c.COLUMN_SIZE << ")";
        break;

    case SQL_DECIMAL:
    case SQL_NUMERIC:
        ss << "DECIMAL(" << c.DECIMAL_DIGITS << "," << c.NUM_PREC_RADIX << ")";
        break;

    case SQL_WLONGVARCHAR:
    case SQL_LONGVARCHAR:
        if (c.CHAR_OCTET_LENGTH < 16384)
        {
            ss << "VARCHAR(" << c.COLUMN_SIZE << ")";
        }
        else if (c.CHAR_OCTET_LENGTH < 65535)
        {
            ss << "TEXT";
        }
        else if (c.CHAR_OCTET_LENGTH < 16777215)
        {
            ss << "MEDIUMTEXT";
        }
        else
        {
            ss << "LONGTEXT";
        }
        break;

    case SQL_LONGVARBINARY:
        if (c.CHAR_OCTET_LENGTH < 16384)
        {
            ss << "VARBINARY(" << c.CHAR_OCTET_LENGTH << ")";
        }
        else if (c.CHAR_OCTET_LENGTH < 65535)
        {
            ss << "BLOB";
        }
        else if (c.CHAR_OCTET_LENGTH < 16777215)
        {
            ss << "MEDIUMBLOB";
        }
        else
        {
            ss << "LONGBLOB";
        }
        break;

    case SQL_TYPE_DATE:
        ss << "DATE";
        break;

#ifdef SQL_TYPE_UTCTIME
    case SQL_TYPE_UTCTIME:
#endif
    case SQL_TYPE_TIME:
        ss << "TIME";
        break;

    case SQL_TYPE_TIMESTAMP:
        ss << "TIMESTAMP";
        break;

#ifdef SQL_TYPE_UTCDATETIME
    case SQL_TYPE_UTCDATETIME:
#endif
    case SQL_INTERVAL_MONTH:
    case SQL_INTERVAL_YEAR:
    case SQL_INTERVAL_YEAR_TO_MONTH:
    case SQL_INTERVAL_DAY:
    case SQL_INTERVAL_HOUR:
    case SQL_INTERVAL_MINUTE:
    case SQL_INTERVAL_SECOND:
    case SQL_INTERVAL_DAY_TO_HOUR:
    case SQL_INTERVAL_DAY_TO_MINUTE:
    case SQL_INTERVAL_DAY_TO_SECOND:
    case SQL_INTERVAL_HOUR_TO_MINUTE:
    case SQL_INTERVAL_HOUR_TO_SECOND:
    case SQL_INTERVAL_MINUTE_TO_SECOND:
        ss << "DATETIME";
        break;

    default:
        ss << "UNKNOWN";
        break;
    }

    ss << " " << (c.IS_NULLABLE == "YES" ? "NULL" : "NOT NULL");

    return ss.str();
}

ResultBuffer::ResultBuffer(const std::vector<ColumnInfo> &infos)
{
    size_t row_size = 0;

    for (const auto &i : infos)
    {
        row_size += buffer_size(i);
    }

    row_count = std::min(BATCH_SIZE / row_size, MAX_BATCH_ROWS);

    row_status.resize(row_count);
    columns.reserve(infos.size());

    for (const auto &i : infos)
    {
        debug("Column:", i.name, "Size:", i.size, "Buffer Size:", i.buffer_size, "Final Size:", buffer_size(i), "Type:", c_type_to_str(sql_to_c_type(i.data_type)));
        columns.emplace_back(row_count, buffer_size(i), sql_to_c_type(i.data_type));
    }
}

size_t ResultBuffer::buffer_size(const ColumnInfo &c) const
{
    // return std::min(1024UL * 1024, std::max(c.buffer_size, c.size) + 1);

    switch (c.data_type)
    {
    case SQL_BIT:
    case SQL_TINYINT:
        return sizeof(SQLCHAR);

    case SQL_SMALLINT:
        return sizeof(SQLSMALLINT);

    case SQL_INTEGER:
    case SQL_BIGINT:
        return sizeof(SQLINTEGER);

    case SQL_REAL:
        return sizeof(SQLREAL);

    case SQL_FLOAT:
    case SQL_DOUBLE:
        return sizeof(SQLDOUBLE);

    // Treat everything else as a string, keeps things simple. Also keep the buffer smaller than 1Mib, some varchars seems to be blobs in reality.
    default:
        return std::min(1024UL * 1024, std::max(c.buffer_size, c.size) + 1);
    }
}

bool ResultBuffer::Column::is_null(int row) const
{
    return indicators[row] == SQL_NULL_DATA;
}

std::string ResultBuffer::Column::to_string(int row) const
{
    std::string rval;
    const uint8_t *ptr = buffers.data() + buffer_size * row;
    const SQLLEN len = *(indicators.data() + row);

    switch (buffer_type)
    {
    case SQL_C_BIT:
    case SQL_C_UTINYINT:
        rval = std::to_string(*reinterpret_cast<const SQLCHAR *>(ptr));
        break;

    case SQL_C_USHORT:
        rval = std::to_string(*reinterpret_cast<const SQLUSMALLINT *>(ptr));
        break;

    case SQL_C_ULONG:
        rval = std::to_string(*reinterpret_cast<const SQLUINTEGER *>(ptr));
        break;

    case SQL_C_DOUBLE:
        rval = std::to_string(*reinterpret_cast<const SQLDOUBLE *>(ptr));
        break;

    // String, date, time et cetera. Keeps things simple as DATETIME structs are a little messy.
    default:
        if (len != SQL_NULL_DATA)
        {
            rval.assign((const char *)ptr, len);
        }
        else
        {
            rval = "<NULL>";
        }
        break;
    }

    return rval;
}

bool ODBC::TextResult::send(const std::vector<ColumnInfo> &metadata, ResultBuffer &res, SQLULEN rows_fetched)
{
    int columns = metadata.size();

    for (SQLULEN i = 0; i < rows_fetched; i++)
    {
        Row row(columns);
        debug("Row", i, "status:", row_status_to_str(res.row_status[i]));

        if (res.row_status[i] == SQL_ROW_SUCCESS || res.row_status[i] == SQL_ROW_SUCCESS_WITH_INFO)
        {
            for (int c = 0; c < columns; c++)
            {
                if (!res.columns[c].is_null(i))
                {
                    row[c] = res.columns[c].to_string(i);
                }

                debug("Row", i, "column", c, row[c].value_or("<NULL>"));
            }
        }

        result.push_back(std::move(row));
    }

    return true;
}

ODBC::ODBC(std::string dsn)
    : m_dsn(std::move(dsn))
{
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &m_env);
    SQLSetEnvAttr(m_env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
    // The DBC handler must be allocated after the ODBC version is set, otherwise the SQLConnect
    // function returns SQL_INVALID_HANDLE.
    SQLAllocHandle(SQL_HANDLE_DBC, m_env, &m_conn);
}

ODBC::~ODBC()
{
    SQLFreeHandle(SQL_HANDLE_STMT, m_stmt);
    SQLDisconnect(m_conn);
    SQLFreeHandle(SQL_HANDLE_DBC, m_conn);
    SQLFreeHandle(SQL_HANDLE_ENV, m_env);
}

bool ODBC::connect()
{
    SQLSetConnectAttr(m_conn, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0);
    SQLSetConnectAttr(m_conn, SQL_ATTR_TXN_ISOLATION, (SQLPOINTER)SQL_TXN_REPEATABLE_READ, 0);

    SQLCHAR outbuf[1024];
    SQLSMALLINT s2len;
    SQLRETURN ret = SQLDriverConnect(m_conn, nullptr, (SQLCHAR *)m_dsn.c_str(), m_dsn.size(),
                                     outbuf, sizeof(outbuf), &s2len, SQL_DRIVER_NOPROMPT);

    if (ret == SQL_ERROR)
    {
        m_error = get_error(SQL_HANDLE_DBC, m_conn);
    }
    else if (ret == SQL_SUCCESS_WITH_INFO)
    {
        debug("SQL_SUCCESS_WITH_INFO:", get_error(SQL_HANDLE_DBC, m_conn));
    }
    else
    {
        debug("SQLDriverConnect:", ret_to_str(ret));
        SQLAllocHandle(SQL_HANDLE_STMT, m_conn, &m_stmt);
    }

    return SQL_SUCCEEDED(ret);
}

void ODBC::disconnect()
{
    SQLDisconnect(m_conn);
}

const std::string &ODBC::error()
{
    return m_error;
}

std::map<std::string, std::map<std::string, std::string>> ODBC::drivers()
{
    std::map<std::string, std::map<std::string, std::string>> rval;
    SQLCHAR drv[512];
    std::vector<SQLCHAR> attr(1024);
    SQLSMALLINT drv_sz = 0;
    SQLSMALLINT attr_sz = 0;
    SQLUSMALLINT dir = SQL_FETCH_FIRST;
    SQLRETURN ret;

    while (SQL_SUCCEEDED(ret = SQLDrivers(m_env, dir, drv, sizeof(drv), &drv_sz,
                                          attr.data(), attr.size(), &attr_sz)))
    {
        if (ret == SQL_SUCCESS_WITH_INFO && data_truncation())
        {
            // The buffer was too small, need more space
            attr.resize(attr.size() * 2);
            dir = SQL_FETCH_FIRST;
        }
        else
        {
            dir = SQL_FETCH_NEXT;
            std::map<std::string, std::string> values;

            // The values are separated by nulls and terminated by nulls. Once we find an empty string, we've reached the end of the attribute list.
            for (char *ptr = (char *)attr.data(); *ptr; ptr += strlen(ptr) + 1)
            {
                if (auto tok = mxb::strtok(ptr, "="); tok.size() >= 2)
                {
                    values.emplace(std::move(tok[0]), std::move(tok[1]));
                }
            }

            for (auto kw : {"Driver", "Driver64"})
            {
                if (auto it = values.find(kw); it != values.end())
                {
                    if (access(it->second.c_str(), F_OK) == 0)
                    {
                        rval.emplace((char *)drv, std::move(values));
                        break;
                    }
                }
            }
        }
    }

    return rval;
}

SQLCHAR *to_ptr(std::string &str)
{
    return reinterpret_cast<SQLCHAR *>(str.empty() ? nullptr : str.data());
}

std::optional<std::vector<CatalogStatistics>> ODBC::statistics(std::string catalog, std::string schema, std::string table)
{
    SQLRETURN ret = SQLStatistics(m_stmt,
                                  to_ptr(catalog), catalog.size(),
                                  to_ptr(schema), schema.size(),
                                  to_ptr(table), table.size(),
                                  SQL_INDEX_ALL, SQL_QUICK);

    if (auto result = read_response(ret); result.has_value() && m_columns.size() >= 13)
    {
        if (debug_enabled())
        {
            print_result(*result);
        }

        std::vector<CatalogStatistics> rval;

        for (const auto &row : *result)
        {
            // Column Descriptions: https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlstatistics-function
            CatalogStatistics s;
            s.TABLE_CAT = as_string(row[0]);
            s.TABLE_SCHEM = as_string(row[1]);
            s.TABLE_NAME = as_string(row[2]);
            s.NON_UNIQUE = as_int(row[3]);
            s.INDEX_QUALIFIER = as_string(row[4]);
            s.INDEX_NAME = as_string(row[5]);
            s.TYPE = as_int(row[6]);
            s.ORDINAL_POSITION = as_int(row[7]);
            s.COLUMN_NAME = as_string(row[8]);
            s.ASC_OR_DESC = as_string(row[9]);
            s.CARDINALITY = as_int(row[10]);
            s.PAGES = as_int(row[11]);
            s.FILTER_CONDITION = as_string(row[12]);
            rval.push_back(std::move(s));
        }

        return rval;
    }
    else
    {
        debug("Columns:", m_columns.size());
    }

    return {};
}

std::optional<std::vector<CatalogPrimaryKey>> ODBC::primary_keys(std::string catalog, std::string schema, std::string table)
{
    SQLRETURN ret = SQLPrimaryKeys(m_stmt,
                                   to_ptr(catalog), catalog.size(),
                                   to_ptr(schema), schema.size(),
                                   to_ptr(table), table.size());

    if (auto result = read_response(ret); result.has_value() && m_columns.size() >= 6)
    {
        if (debug_enabled())
        {
            print_result(*result);
        }

        std::vector<CatalogPrimaryKey> rval;

        for (const auto &row : *result)
        {
            // Column Descriptions: https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlstatistics-function
            CatalogPrimaryKey k;
            k.TABLE_CAT = as_string(row[0]);
            k.TABLE_SCHEMA = as_string(row[1]);
            k.TABLE_NAME = as_string(row[2]);
            k.COLUMN_NAME = as_string(row[3]);
            k.KEY_SEQ = as_int(row[4]);
            k.PK_NAME = as_string(row[5]);
            rval.push_back(std::move(k));
        }

        return rval;
    }
    else
    {
        debug("Columns:", m_columns.size());
    }

    return {};
}

std::optional<ODBC::Result> ODBC::tables(std::string catalog, std::string schema, std::string table)
{
    SQLRETURN ret = SQLTables(m_stmt,
                              to_ptr(catalog), catalog.size(),
                              to_ptr(schema), schema.size(),
                              to_ptr(table), table.size(),
                              nullptr, 0);

    return read_response(ret);
}

std::optional<std::vector<CatalogColumn>> ODBC::columns(std::string catalog, std::string schema, std::string table)
{
    SQLRETURN ret = SQLColumns(m_stmt,
                               to_ptr(catalog), catalog.size(),
                               to_ptr(schema), schema.size(),
                               to_ptr(table), table.size(),
                               nullptr, 0);

    if (auto result = read_response(ret); result.has_value() && m_columns.size() >= 18)
    {
        std::vector<CatalogColumn> rval;

        for (const auto &row : *result)
        {
            // Column Descriptions: https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolumns-function
            CatalogColumn col;
            col.TABLE_CAT = as_string(row[0]);
            col.TABLE_SCHEM = as_string(row[1]);
            col.TABLE_NAME = as_string(row[2]);
            col.COLUMN_NAME = as_string(row[3]);
            col.DATA_TYPE = as_int(row[4]);
            col.TYPE_NAME = as_string(row[5]);
            col.COLUMN_SIZE = as_int(row[6]);
            col.BUFFER_LENGTH = as_int(row[7]);
            col.DECIMAL_DIGITS = as_int(row[8]);
            col.NUM_PREC_RADIX = as_int(row[9]);
            col.NULLABLE = as_int(row[10]);
            col.REMARKS = as_string(row[11]);
            col.COLUMN_DEF = as_string(row[12]);
            col.SQL_DATA_TYPE = as_int(row[13]);
            col.SQL_DATETIME_SUB = as_int(row[14]);
            col.CHAR_OCTET_LENGTH = as_int(row[15]);
            col.ORDINAL_POSITION = as_int(row[16]);
            col.IS_NULLABLE = as_string(row[17]);
            for (size_t i = 18; i < row.size(); i++)
            {
                col.extra_values.push_back(m_columns[i].name + ": " + row[i].value_or("<null>"));
            }
            rval.push_back(std::move(col));

            debug("Column outputs:", rval.size(), "Rows:", row.size());
        }

        // Sort the fields according to their ordinal position in case the driver does something stupid.
        // This guarantees that the order of the columns is always the same on both ends.
        std::sort(rval.begin(), rval.end(), [](const auto &l, const auto &r)
                  { return l.ORDINAL_POSITION < r.ORDINAL_POSITION; });

        if (debug_enabled())
        {
            print_result(*result);
        }

        return rval;
    }
    else
    {
        debug("Columns in SQLColumns:", m_columns.size(), "Return:", ret_to_str(ret), "Has value:", result.has_value() ? "Yes" : "No");
    }

    return {};
}

std::optional<ODBC::Result> ODBC::query(const std::string &query)
{
    SQLRETURN ret = SQLExecDirect(m_stmt, (SQLCHAR *)query.c_str(), query.size());
    return read_response(ret);
}

bool ODBC::commit()
{
    return SQL_SUCCEEDED(SQLEndTran(SQL_HANDLE_DBC, m_conn, SQL_COMMIT));
}

bool ODBC::stream(const std::string &query, Output *output)
{
    SQLRETURN ret = SQLExecDirect(m_stmt, (SQLCHAR *)query.c_str(), query.size());
    return process_response(ret, output);
}

void ODBC::print_columns(const std::vector<ColumnInfo> &columns)
{
    if (columns.empty())
    {
        return;
    }

    size_t len = get_len("Name", "Type", "Size", "Bytes", "Digits", "Nullable");

    for (const ColumnInfo &c : columns)
    {
        len = std::max(len, get_len(c.name, type_to_str(c.data_type), c.size, c.buffer_size, c.digits, c.nullable));
    }

    std::string line = "+";
    line.append(len * 6 + 6 + 1 - 2, '-');
    line += "+";

    std::cout << line << std::endl;

    std::cout << pretty_print(len, "Name", "Type", "Size", "Bytes", "Digits", "Nullable") << std::endl;

    std::cout << line << std::endl;

    for (const ColumnInfo &c : columns)
    {
        std::cout << pretty_print(len, c.name, type_to_str(c.data_type), c.size, c.buffer_size, c.digits, c.nullable) << std::endl;
    }

    std::cout << line << std::endl;
}

void ODBC::print_result(const Result &result)
{
    // Some drivers return zero-width resultsets which cause problems.
    if (m_columns.empty())
    {
        return;
    }

    size_t len = 0;

    for (const ColumnInfo &c : m_columns)
    {
        len = std::max(len, c.name.size());
    }

    for (const Row &row : result)
    {
        for (const Value &value : row)
        {
            len = std::max(len, value ? value->size() : 4);
        }
    }

    if (len * m_columns.size() > 140)
    {
        int rows = 1;

        for (const Row &row : result)
        {
            std::cout << "---------- Row " << rows++ << " ----------" << std::endl;
            for (size_t i = 0; i < row.size(); i++)
            {
                std::cout << m_columns[i].name << ": " << row[i].value_or("NULL") << std::endl;
            }
        }

        return;
    }

    std::string line = "+";
    line.append(len * m_columns.size() + m_columns.size() + 1 - 2, '-');
    line += "+";

    std::cout << line << std::endl;

    std::cout << '|';

    for (const ColumnInfo &c : m_columns)
    {
        std::cout << std::setw(len) << c.name << "|";
    }

    std::cout << std::endl;
    std::cout << line << std::endl;

    for (const Row &row : result)
    {
        std::cout << '|';

        for (const Value &value : row)
        {
            std::cout << std::setw(len) << (value ? *value : "NULL") << "|";
        }

        std::cout << std::endl;
    }

    std::cout << line << std::endl;
}

std::optional<std::string> ODBC::get_by_name(const Row &row, std::string name)
{
    for (size_t i = 0; i < m_columns.size(); i++)
    {
        if (strcasecmp(m_columns[i].name.c_str(), name.c_str()))
        {
            return row[i];
        }
    }

    return {};
}

int64_t ODBC::as_int(const Value &val) const
{
    debug("as_int:", val.value_or("<no value>"));
    return strtol(val.value_or("0").c_str(), nullptr, 10);
}

std::string ODBC::as_string(const Value &val) const
{
    debug("as_string:", val.value_or("<no value>"));
    return val.value_or("");
}

void copy_table(std::string src_dsn, std::string dest_dsn, std::unique_ptr<Translator> translator, const std::vector<TableInfo> &tables)
{
    bool ok = true;
    std::vector<std::tuple<std::unique_ptr<ODBC>, std::unique_ptr<ODBC>, TableInfo>> jobs;
    jobs.reserve(tables.size());

    ODBC coordinator(src_dsn);

    if (!coordinator.connect())
    {
        throw FatalError("Coordinator connect failed:", coordinator.error());
    }

    if (!translator->prepare(coordinator))
    {
        throw FatalError("Prepare failed:", coordinator.error());
    }
    else if (!translator->start(coordinator, tables))
    {
        throw FatalError("Start failed:", coordinator.error());
    }

    debug("Prepare successful");

    auto SQL_SETUP = "SET MAX_STATEMENT_TIME=0, SQL_MODE='ANSI_QUOTES', UNIQUE_CHECKS=0, FOREIGN_KEY_CHECKS=0, SQL_NOTES=0, AUTOCOMMIT=0";

    for (const auto &table : tables)
    {
        std::cout << "Dumping: " << table.name << std::endl;
        auto &[src, dest, tbl] = jobs.emplace_back(std::make_unique<ODBC>(src_dsn), std::make_unique<ODBC>(dest_dsn), table);

        if (!src->connect())
        {
            throw FatalError("Failed to connect to source:", src->error());
        }
        else if (!dest->connect())
        {
            throw FatalError("Failed to connect to destination:", dest->error());
        }
        else if (!translator->prepare(*src))
        {
            throw FatalError("Failed to prepare source:", dest->error());
        }
        else if (!translator->start_thread(*src, table))
        {
            throw FatalError("Failed to prepare worker thread:", src->error());
        }
        else if (!dest->query(SQL_SETUP))
        {
            throw FatalError("Failed to prepare destination connection:", dest->error());
        }

        auto create = translator->create_table(*src, table);

        if (create.empty())
        {
            throw FatalError("Could not get SQL for CREATE TABLE for table", table.name);
        }

        for (const auto &c : create)
        {
            debug("CREATE:", c);

            if (c.empty())
            {
                throw FatalError("Empty CREATE TABLE sql");
            }
            if (!dest->query(c))
            {
                throw FatalError("CREATE TABLE failed. (", c, ") :", dest->error());
            }
        }
    }

    if (!translator->threads_started(coordinator, tables))
    {
        throw FatalError("Post-start failed:", coordinator.error());
    }

    for (auto &[src, dest, tbl] : jobs)
    {
        auto sql = translator->select(*src, tbl);

        if (sql.empty())
        {
            throw FatalError("Could not get SQL for SELECT for table", tbl.name);
        }

        std::cout << "Starting dump of " << tbl.schema << "." << tbl.name << std::endl;
        auto start = std::chrono::steady_clock::now();

        debug("SELECT:", sql);

        auto insert = translator->insert(*src, tbl);

        if (insert.empty())
        {
            throw FatalError("Could not get SQL for INSERT for table", tbl.name);
        }
        else if (!dest->prepare(insert))
        {
            throw FatalError("Setup transfer failed:", dest->error());
        }
        else if (!src->stream(sql, dest.get()))
        {
            throw FatalError("Streaming data failed:", src->error(), dest->error());
        }
        else if (!dest->commit())
        {
            throw FatalError("Finish transfer failed:", dest->error());
        }

        auto end = std::chrono::steady_clock::now();
        auto dur = std::chrono::duration_cast<std::chrono::duration<float>>(end - start);
        start = end;

        std::cout << "Dump complete, took " << dur.count() << " seconds. Creating indexes..." << std::endl;

        for (auto idx_sql : translator->create_index(*src, tbl))
        {
            std::cout << idx_sql << std::endl;

            if (!dest->query(idx_sql))
            {
                throw FatalError("Index creation failed:", dest->error());
            }
        }

        end = std::chrono::steady_clock::now();
        dur = std::chrono::duration_cast<std::chrono::duration<float>>(end - start);
        std::cout << "Index creation complete, took " << dur.count() << " seconds." << std::endl;

        translator->stop_thread(*src, tbl);
        src->disconnect();
        dest->disconnect();
    }
}

std::optional<ODBC::Result> ODBC::read_response(SQLRETURN ret)
{
    std::optional<Result> rval;
    TextResult result;

    if (process_response(ret, &result))
    {
        rval = result.result;
    }
    else
    {
        debug("process_response failed");
    }

    return rval;
}

bool ODBC::process_response(SQLRETURN ret, ODBC::Output *handler)
{
    assert(handler);
    bool ok = false;
    debug("read_response:", ret_to_str(ret));

    if (SQL_SUCCEEDED(ret))
    {
        SQLSMALLINT columns = 0;
        SQLRETURN ret = SQLNumResultCols(m_stmt, &columns);
        debug("get_result", ret_to_str(ret), "columns", columns);

        if (columns == 0)
        {
            ok = true;
        }
        else if (columns > 0)
        {
            m_columns = get_headers(columns);
            debug("can_batch:", can_batch() ? "Yes" : "No");
            ok = can_batch() ? get_batch_result(columns, handler) : get_normal_result(columns, handler);
        }
    }
    else
    {
        m_error = get_error(SQL_HANDLE_STMT, m_stmt);
    }

    return ok;
}

bool ODBC::data_truncation()
{
    constexpr std::string_view truncated = "01004";
    SQLLEN n = 0;
    SQLGetDiagField(SQL_HANDLE_STMT, m_stmt, 0, SQL_DIAG_NUMBER, &n, 0, 0);

    for (int i = 0; i < n; i++)
    {
        SQLCHAR sqlstate[6];
        SQLCHAR msg[SQL_MAX_MESSAGE_LENGTH];
        SQLINTEGER native_error;
        SQLSMALLINT msglen = 0;

        if (SQLGetDiagRec(SQL_HANDLE_STMT, m_stmt, i + 1, sqlstate, &native_error,
                          msg, sizeof(msg), &msglen) != SQL_NO_DATA)
        {
            if ((const char *)sqlstate == truncated)
            {
                return true;
            }
        }
    }

    return false;
}

std::vector<ColumnInfo> ODBC::get_headers(int columns)
{
    std::vector<ColumnInfo> cols;
    cols.reserve(columns);

    for (SQLSMALLINT i = 0; i < columns; i++)
    {
        char name[256] = "";
        SQLSMALLINT namelen = 0;
        SQLSMALLINT data_type;
        SQLULEN colsize;
        SQLSMALLINT digits;
        SQLSMALLINT nullable;

        SQLRETURN ret = SQLDescribeCol(m_stmt, i + 1, (SQLCHAR *)name, sizeof(name), &namelen, &data_type, &colsize, &digits, &nullable);

        if (SQL_SUCCEEDED(ret))
        {
            ColumnInfo info;
            info.name = name;
            info.size = colsize;
            info.data_type = data_type;
            info.digits = digits;
            info.nullable = nullable;

            if (!get_int_attr(i + 1, SQL_DESC_OCTET_LENGTH, &info.buffer_size))
            {
                debug("Error in SQLColAttribute:", get_error(SQL_HANDLE_STMT, m_stmt));
            }

            cols.push_back(std::move(info));
        }
        else if (ret == SQL_ERROR)
        {
            m_error = get_error(SQL_HANDLE_STMT, m_stmt);
            SQLCloseCursor(m_stmt);
            return {};
        }
    }

    return cols;
}

bool ODBC::get_normal_result(int columns, ODBC::Output *handler)
{
    SQLRETURN ret;
    ResultBuffer res(m_columns);

    if (debug_enabled())
    {
        print_columns(m_columns);
    }

    bool ok = true;

    while (SQL_SUCCEEDED(ret = SQLFetch(m_stmt)))
    {
        Row row(columns);

        for (SQLSMALLINT i = 0; i < columns; i++)
        {
            auto &c = res.columns[i];
            ret = SQLGetData(m_stmt, i + 1, c.buffer_type, c.buffers.data(), c.buffers.size(), c.indicators.data());

            while (ret == SQL_SUCCESS_WITH_INFO && data_truncation())
            {
                auto old_size = c.buffers.size() - 1; // Minus one since these are null-terminated strings
                c.buffers.resize(c.indicators.front());
                c.buffer_size = c.buffers.size();
                ret = SQLGetData(m_stmt, i + 1, c.buffer_type, c.buffers.data() + old_size, c.buffers.size() - old_size, c.indicators.data());
            }

            if (ret == SQL_ERROR)
            {
                m_error = get_error(SQL_HANDLE_STMT, m_stmt);
                ok = false;
                break;
            }
        }

        if (!handler->send(m_columns, res, 1))
        {
            ok = false;
            break;
        }
    }

    SQLCloseCursor(m_stmt);

    if (ret == SQL_ERROR)
    {
        m_error = get_error(SQL_HANDLE_STMT, m_stmt);
        ok = false;
    }

    return ok;
}

bool ODBC::get_batch_result(int columns, ODBC::Output *handler)
{
    ResultBuffer res(m_columns);

    if (debug_enabled())
    {
        debug("get_batch_result", columns, "columns");
        print_columns(m_columns);
    }

    for (size_t i = 0; i < res.columns.size(); i++)
    {
        debug(i, res.columns[i].buffer_size);
    }

    SQLULEN rows_fetched = 0;
    SQLSetStmtAttr(m_stmt, SQL_ATTR_ROW_BIND_TYPE, (void *)SQL_BIND_BY_COLUMN, 0);
    SQLSetStmtAttr(m_stmt, SQL_ATTR_ROW_ARRAY_SIZE, (void *)res.row_count, 0);
    SQLSetStmtAttr(m_stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rows_fetched, 0);
    SQLSetStmtAttr(m_stmt, SQL_ATTR_ROW_STATUS_PTR, res.row_status.data(), 0);

    for (int i = 0; i < columns; i++)
    {
        SQLBindCol(m_stmt, i + 1, res.columns[i].buffer_type, res.columns[i].buffers.data(),
                   res.columns[i].buffer_size, res.columns[i].indicators.data());
    }

    bool ok = true;
    SQLRETURN ret;

    debug("BEFORE FETCH");

    while (SQL_SUCCEEDED(ret = SQLFetch(m_stmt)))
    {
        debug("Rows fetched:", rows_fetched);

        if (!handler->send(m_columns, res, rows_fetched))
        {
            ok = false;
            break;
        }
    }

    SQLCloseCursor(m_stmt);

    if (ret == SQL_ERROR)
    {
        m_error = get_error(SQL_HANDLE_STMT, m_stmt);
        if (m_error.empty())
        {
            m_error = get_error(SQL_HANDLE_DBC, m_conn);
        }
        
        debug("ERROR:", m_error);
        ok = false;
    }
    else if (ret == SQL_SUCCESS_WITH_INFO)
    {
        debug("Got warnings:", get_error(SQL_HANDLE_STMT, m_stmt));
    }
    else
    {
        debug("Something else:", ret_to_str(ret));
    }

    return ok;
}

bool ODBC::can_batch()
{

    for (const auto &i : m_columns)
    {
        size_t buffer_size = 0;

        switch (i.data_type)
        {
        // If the result has LOBs in it, the data should be retrieved one row at a time using
        // SQLGetData instead of using an array to fetch multiple rows at a time.
        case SQL_WLONGVARCHAR:
        case SQL_LONGVARCHAR:
        case SQL_LONGVARBINARY:
            return i.size < 16384;

        default:
            if (i.size == 0 || i.size > ODBC::MAX_BATCH_SIZE)
            {
                // The driver either doesn't know how big the value is or it is way too large to be batched.
                return false;
            }
        }
    }

    return true;
}

bool ODBC::prepare(const std::string &sql)
{
    SQLRETURN ret = SQLPrepare(m_stmt, (SQLCHAR *)sql.data(), sql.size());

    if (ret == SQL_SUCCESS_WITH_INFO)
    {
        debug("SQLPrepare:", get_error(SQL_HANDLE_STMT, m_stmt));
    }
    else if (ret == SQL_ERROR)
    {
        m_error = get_error(SQL_HANDLE_STMT, m_stmt);
    }

    return SQL_SUCCEEDED(ret);
}

bool ODBC::send(const std::vector<ColumnInfo> &metadata, ResultBuffer &res, SQLULEN rows_fetched)
{
    if (debug_enabled())
    {
        for (SQLULEN i = 0; i < rows_fetched; i++)
        {
            if (res.row_status[i] == SQL_ROW_SUCCESS || res.row_status[i] == SQL_ROW_SUCCESS_WITH_INFO)
            {
                std::ostringstream ss;

                for (int c = 0; c < metadata.size(); c++)
                {
                    ss << res.columns[c].to_string(i) << " ";
                }

                debug("Row", i, ss.str());
            }
        }
    }

    SQLLEN params_processed = 0;
    SQLSetStmtAttr(m_stmt, SQL_ATTR_PARAM_BIND_TYPE, (void *)SQL_PARAM_BIND_BY_COLUMN, 0);
    SQLSetStmtAttr(m_stmt, SQL_ATTR_PARAMSET_SIZE, (void *)rows_fetched, 0);
    SQLSetStmtAttr(m_stmt, SQL_ATTR_PARAM_STATUS_PTR, res.row_status.data(), 0);
    SQLSetStmtAttr(m_stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &params_processed, 0);

    for (size_t i = 0; i < metadata.size(); i++)
    {
        SQLBindParameter(m_stmt, i + 1, SQL_PARAM_INPUT, res.columns[i].buffer_type,
                         metadata[i].data_type, metadata[i].size, metadata[i].digits,
                         (SQLPOINTER *)res.columns[i].buffers.data(), res.columns[i].buffer_size,
                         res.columns[i].indicators.data());
    }

    SQLRETURN ret = SQLExecute(m_stmt);

    if (ret == SQL_ERROR)
    {
        m_error = get_error(SQL_HANDLE_STMT, m_stmt);
        debug("SQLExecute failed:", m_error);
    }
    else if (ret == SQL_SUCCESS_WITH_INFO)
    {
        debug("SQLExecute:", get_error(SQL_HANDLE_STMT, m_stmt));
    }

    return SQL_SUCCEEDED(ret);
}
