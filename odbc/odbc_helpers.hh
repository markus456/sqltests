#pragma once
#include <sql.h>
#include <sqlext.h>
#include <cstdio>

// The values from SQLColumns: https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolumns-function
struct CatalogColumn
{
    std::string TABLE_CAT;
    std::string TABLE_SCHEM;
    std::string TABLE_NAME;
    std::string COLUMN_NAME;
    int16_t DATA_TYPE;
    std::string TYPE_NAME;
    uint32_t COLUMN_SIZE;
    uint32_t BUFFER_LENGTH;
    int16_t DECIMAL_DIGITS;
    int16_t NUM_PREC_RADIX;
    int16_t NULLABLE;
    std::string REMARKS;
    std::string COLUMN_DEF;
    int16_t SQL_DATA_TYPE;
    int16_t SQL_DATETIME_SUB;
    uint32_t CHAR_OCTET_LENGTH;
    uint32_t ORDINAL_POSITION;
    std::string IS_NULLABLE;
    std::vector<std::string> extra_values;
};

// The values from SQLStatistics: https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlstatistics-function
struct CatalogStatistics
{
    std::string TABLE_CAT;
    std::string TABLE_SCHEM;
    std::string TABLE_NAME;
    int16_t NON_UNIQUE;
    std::string INDEX_QUALIFIER;
    std::string INDEX_NAME;
    int16_t TYPE;
    int16_t ORDINAL_POSITION;
    std::string COLUMN_NAME;
    std::string ASC_OR_DESC;
    uint32_t CARDINALITY;
    uint32_t PAGES;
    std::string FILTER_CONDITION;
};

// The values from SQLPrimaryKeys: https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlprimarykeys-function
struct CatalogPrimaryKey
{
    std::string TABLE_CAT;
    std::string TABLE_SCHEMA;
    std::string TABLE_NAME;
    std::string COLUMN_NAME;
    int KEY_SEQ;
    std::string PK_NAME;
};

ColumnType sql_to_mariadb_type(int data_type, int size);

std::string to_mariadb_type(const CatalogColumn &c);

//
// Helpers for converting ODBC return values to strings
//

inline const char *row_status_to_str(int ret)
{
    static char msg[256];

    switch (ret)
    {
    case SQL_ROW_SUCCESS:
        return "SQL_ROW_SUCCESS";
    case SQL_ROW_SUCCESS_WITH_INFO:
        return "SQL_ROW_SUCCESS_WITH_INFO";
    case SQL_ROW_ERROR:
        return "SQL_ROW_ERROR";
    case SQL_ROW_UPDATED:
        return "SQL_ROW_UPDATED";
    case SQL_ROW_DELETED:
        return "SQL_ROW_DELETED";
    case SQL_ROW_ADDED:
        return "SQL_ROW_ADDED";
    case SQL_ROW_NOROW:
        return "SQL_ROW_NOROW";
    default:
        snprintf(msg, sizeof(msg), "UNKNOWN: %d", ret);
        return msg;
    }
}

inline const char *ret_to_str(int ret)
{
    static char msg[256];

    switch (ret)
    {
    case SQL_SUCCESS:
        return "SQL_SUCCESS";
    case SQL_SUCCESS_WITH_INFO:
        return " SQL_SUCCESS_WITH_INFO";
    case SQL_NO_DATA:
        return "SQL_NO_DATA";
    case SQL_STILL_EXECUTING:
        return "SQL_STILL_EXECUTING";
    case SQL_ERROR:
        return "SQL_ERROR";
    case SQL_INVALID_HANDLE:
        return "SQL_INVALID_HANDLE";
    default:
        snprintf(msg, sizeof(msg), "UNKNOWN: %d", ret);
        return msg;
    }
}

inline const char *type_to_str(int type)
{
    static char msg[256];

    switch (type)
    {
    case SQL_CHAR:
        return "SQL_CHAR";
    case SQL_VARCHAR:
        return "SQL_VARCHAR";
    case SQL_LONGVARCHAR:
        return "SQL_LONGVARCHAR";
    case SQL_WCHAR:
        return "SQL_WCHAR";
    case SQL_WVARCHAR:
        return "SQL_WVARCHAR";
    case SQL_WLONGVARCHAR:
        return "SQL_WLONGVARCHAR";
    case SQL_DECIMAL:
        return "SQL_DECIMAL";
    case SQL_NUMERIC:
        return "SQL_NUMERIC";
    case SQL_SMALLINT:
        return "SQL_SMALLINT";
    case SQL_INTEGER:
        return "SQL_INTEGER";
    case SQL_REAL:
        return "SQL_REAL";
    case SQL_FLOAT:
        return "SQL_FLOAT";
    case SQL_DOUBLE:
        return "SQL_DOUBLE";
    case SQL_BIT:
        return "SQL_BIT";
    case SQL_TINYINT:
        return "SQL_TINYINT";
    case SQL_BIGINT:
        return "SQL_BIGINT";
    case SQL_BINARY:
        return "SQL_BINARY";
    case SQL_VARBINARY:
        return "SQL_VARBINARY";
    case SQL_LONGVARBINARY:
        return "SQL_LONGVARBINARY";
    case SQL_TYPE_DATE:
        return "SQL_TYPE_DATE";
    case SQL_TYPE_TIME:
        return "SQL_TYPE_TIME";
    case SQL_TYPE_TIMESTAMP:
        return "SQL_TYPE_TIMESTAMP";
#ifdef SQL_TYPE_UTCDATETIME
    case SQL_TYPE_UTCDATETIME:
        return "SQL_TYPE_UTCDATETIME";
#endif
#ifdef SQL_TYPE_UTCTIME
    case SQL_TYPE_UTCTIME:
        return "SQL_TYPE_UTCTIME";
#endif
    case SQL_INTERVAL_MONTH:
        return "SQL_INTERVAL_MONTH";
    case SQL_INTERVAL_YEAR:
        return "SQL_INTERVAL_YEAR";
    case SQL_INTERVAL_YEAR_TO_MONTH:
        return "SQL_INTERVAL_YEAR_TO_MONTH";
    case SQL_INTERVAL_DAY:
        return "SQL_INTERVAL_DAY";
    case SQL_INTERVAL_HOUR:
        return "SQL_INTERVAL_HOUR";
    case SQL_INTERVAL_MINUTE:
        return "SQL_INTERVAL_MINUTE";
    case SQL_INTERVAL_SECOND:
        return "SQL_INTERVAL_SECOND";
    case SQL_INTERVAL_DAY_TO_HOUR:
        return "SQL_INTERVAL_DAY_TO_HOUR";
    case SQL_INTERVAL_DAY_TO_MINUTE:
        return "SQL_INTERVAL_DAY_TO_MINUTE";
    case SQL_INTERVAL_DAY_TO_SECOND:
        return "SQL_INTERVAL_DAY_TO_SECOND";
    case SQL_INTERVAL_HOUR_TO_MINUTE:
        return "SQL_INTERVAL_HOUR_TO_MINUTE";
    case SQL_INTERVAL_HOUR_TO_SECOND:
        return "SQL_INTERVAL_HOUR_TO_SECOND";
    case SQL_INTERVAL_MINUTE_TO_SECOND:
        return "SQL_INTERVAL_MINUTE_TO_SECOND";
    case SQL_GUID:
        return "SQL_GUID";
    default:
        snprintf(msg, sizeof(msg), "UNKNOWN: %d", type);
        return msg;
    }
}

inline int sql_to_c_type(int data_type)
{
    switch (data_type)
    {
    case SQL_TINYINT:
        return SQL_C_UTINYINT;

    case SQL_SMALLINT:
        return SQL_C_USHORT;

    case SQL_INTEGER:
    case SQL_BIGINT:
        return SQL_C_ULONG;

    case SQL_REAL:
        return SQL_C_FLOAT;

    case SQL_FLOAT:
    case SQL_DOUBLE:
        return SQL_C_DOUBLE;

    case SQL_BINARY:
    case SQL_VARBINARY:
    case SQL_LONGVARBINARY:
        return SQL_C_BINARY;

    case SQL_BIT:
        return SQL_C_BIT;

    // String, date, time et cetera. Keeps things simple as DATETIME structs are a little messy.
    default:
        return SQL_C_CHAR;
    }
}

inline const char *index_type_to_str(int type)
{
    static char msg[256];

    switch (type)
    {
    case SQL_INDEX_CLUSTERED:
        return "SQL_INDEX_CLUSTERED";
    case SQL_INDEX_HASHED:
        return "SQL_INDEX_HASHED";
    case SQL_INDEX_OTHER:
        return "SQL_INDEX_OTHER";
    default:
        snprintf(msg, sizeof(msg), "UNKNOWN: %d", type);
        return msg;
    }
}

inline const char *c_type_to_str(int type)
{
    static char msg[256];

    switch (type)
    {
    case SQL_C_BINARY:
        return "SQL_C_BINARY";
    case SQL_C_BIT:
        return "SQL_C_BIT";
    case SQL_C_DATE:
        return "SQL_C_DATE";
    case SQL_C_GUID:
        return "SQL_C_GUID";
    case SQL_C_INTERVAL_DAY:
        return "SQL_C_INTERVAL_DAY";
    case SQL_C_INTERVAL_DAY_TO_HOUR:
        return "SQL_C_INTERVAL_DAY_TO_HOUR";
    case SQL_C_INTERVAL_DAY_TO_MINUTE:
        return "SQL_C_INTERVAL_DAY_TO_MINUTE";
    case SQL_C_INTERVAL_DAY_TO_SECOND:
        return "SQL_C_INTERVAL_DAY_TO_SECOND";
    case SQL_C_INTERVAL_HOUR:
        return "SQL_C_INTERVAL_HOUR";
    case SQL_C_INTERVAL_HOUR_TO_MINUTE:
        return "SQL_C_INTERVAL_HOUR_TO_MINUTE";
    case SQL_C_INTERVAL_HOUR_TO_SECOND:
        return "SQL_C_INTERVAL_HOUR_TO_SECOND";
    case SQL_C_INTERVAL_MINUTE:
        return "SQL_C_INTERVAL_MINUTE";
    case SQL_C_INTERVAL_MINUTE_TO_SECOND:
        return "SQL_C_INTERVAL_MINUTE_TO_SECOND";
    case SQL_C_INTERVAL_MONTH:
        return "SQL_C_INTERVAL_MONTH";
    case SQL_C_INTERVAL_SECOND:
        return "SQL_C_INTERVAL_SECOND";
    case SQL_C_INTERVAL_YEAR:
        return "SQL_C_INTERVAL_YEAR";
    case SQL_C_INTERVAL_YEAR_TO_MONTH:
        return "SQL_C_INTERVAL_YEAR_TO_MONTH";
    case SQL_C_SBIGINT:
        return "SQL_C_SBIGINT";
    case SQL_C_SLONG:
        return "SQL_C_SLONG";
    case SQL_C_SSHORT:
        return "SQL_C_SSHORT";
    case SQL_C_STINYINT:
        return "SQL_C_STINYINT";
    case SQL_C_TIME:
        return "SQL_C_TIME";
    case SQL_C_TIMESTAMP:
        return "SQL_C_TIMESTAMP";
    case SQL_C_TINYINT:
        return "SQL_C_TINYINT";
    case SQL_C_TYPE_DATE:
        return "SQL_C_TYPE_DATE";
    case SQL_C_TYPE_TIME:
        return "SQL_C_TYPE_TIME";
    case SQL_C_TYPE_TIMESTAMP:
        return "SQL_C_TYPE_TIMESTAMP";
    case SQL_C_UBIGINT:
        return "SQL_C_UBIGINT";
    case SQL_C_ULONG:
        return "SQL_C_ULONG";
    case SQL_C_USHORT:
        return "SQL_C_USHORT";
    case SQL_C_UTINYINT:
        return "SQL_C_UTINYINT";
    case SQL_C_CHAR:
        return "SQL_C_CHAR";
    case SQL_C_LONG:
        return "SQL_C_LONG";
    case SQL_C_SHORT:
        return "SQL_C_SHORT";
    case SQL_C_FLOAT:
        return "SQL_C_FLOAT";
    case SQL_C_DOUBLE:
        return "SQL_C_DOUBLE";
    case SQL_C_NUMERIC:
        return "SQL_C_NUMERIC";
    default:
        snprintf(msg, sizeof(msg), "UNKNOWN: %d", type);
        return msg;
    }
}