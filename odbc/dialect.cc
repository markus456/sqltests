#include "dialect.hh"
#include "odbc.hh"
#include "string_utils.hh"
#include "diagnostics.hh"

#include <mutex>
#include <utility>
#include <iostream>
#include <functional>

class MariaDBTranslator final : public Translator
{
public:
    bool prepare(ODBC &db)
    {
        return db.query("SET SQL_MODE='PIPES_AS_CONCAT'") && db.query("SET AUTOCOMMIT=0");
    }

    bool start(ODBC &source, const std::vector<TableInfo> &tables)
    {
        return !!source.query("FLUSH TABLES WITH READ LOCK");
    }

    bool start_thread(ODBC &source, const TableInfo &table)
    {
        return !!source.query("START TRANSACTION WITH CONSISTENT SNAPSHOT");
    }

    std::vector<std::string> create_table(ODBC &source, const TableInfo &table)
    {
        std::vector<std::string> sql;
        std::ostringstream ss;
        ss << "SHOW CREATE TABLE `" << table.catalog << "`.`" << table.name << "`";

        if (auto res = source.query(ss.str()))
        {
            sql.push_back((*res)[0][1].value());
        }

        return sql;
    }

    std::vector<std::string> create_index(ODBC &source, const TableInfo &table)
    {
        // Already a part of the CREATE TABLE statement
        return {};
    }

    bool threads_started(ODBC &source, const std::vector<TableInfo> &tables)
    {
        return !!source.query("UNLOCK TABLES");
    }

    std::string select(ODBC &source, const TableInfo &table)
    {
        const char *format = R"(
SELECT
  'SELECT ' || GROUP_CONCAT('`' || column_name || '`' ORDER BY ORDINAL_POSITION SEPARATOR ',') ||
  ' FROM `' || table_name || '`'
FROM information_schema.columns
WHERE table_schema = '%s' AND table_name = '%s' AND is_generated = 'NEVER'
GROUP BY table_schema, table_name;
)";
        std::string rval;

        if (auto res = source.query(mxb::string_printf(format, table.catalog.c_str(), table.name.c_str())))
        {
            rval = res->front().front().value();
        }

        return rval;
    }

    std::string insert(ODBC &source, const TableInfo &table)
    {
        const char *format = R"(
SELECT
  'INSERT INTO `' || TABLE_SCHEMA || '`.`' || TABLE_NAME ||
  '` (' || GROUP_CONCAT('`' || COLUMN_NAME || '`' ORDER BY ORDINAL_POSITION SEPARATOR ',') ||
  ') VALUES (' || GROUP_CONCAT('?' SEPARATOR ',') || ')'
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = '%s' AND table_name = '%s' AND is_generated = 'NEVER'
GROUP BY table_schema, table_name;
)";
        std::string rval;

        if (auto res = source.query(mxb::string_printf(format, table.catalog.c_str(), table.name.c_str())))
        {
            rval = res->front().front().value();
        }

        return rval;
    }

    bool stop_thread(ODBC &source, const TableInfo &table)
    {
        return true;
    }
};

class PostgresTranslator final : public Translator
{
public:
    bool prepare(ODBC &source)
    {
        return true;
        // return !!source.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    }

    bool start(ODBC &source, const std::vector<TableInfo> &tables)
    {
        std::lock_guard guard(m_lock);
        bool ok = false;

        if (auto res = source.query("SELECT pg_export_snapshot()"))
        {
            m_trx = res->front().front().value();
            debug("Snapshot:", m_trx);
            ok = true;
        }

        // TODO: Acquire shard lock on the tables

        return ok;
    }

    bool start_thread(ODBC &source, const TableInfo &table)
    {
        std::lock_guard guard(m_lock);
        return !!source.query("SET TRANSACTION SNAPSHOT '" + m_trx + "'");
    }

    std::vector<std::string> create_table(ODBC &source, const TableInfo &table)
    {
        const char *CREATE_SQL = R"(
WITH columndefs AS (
SELECT '`' || a.attname || '` ' ||
  CASE
    WHEN t.typname = 'jsonb' THEN 'JSON'
    WHEN t.typname LIKE 'timestamp%%' THEN 'TIMESTAMP'
    WHEN t.typname LIKE 'time%%' THEN 'TIME'
    WHEN t.typname IN ('line', 'lseg', 'box', 'circle') THEN 'TEXT'
    ELSE pg_catalog.format_type(a.atttypid, a.atttypmod)
  END ||
  CASE WHEN a.attnotnull THEN ' NOT NULL ' ELSE '' END ||
  CASE
    WHEN LEFT(pg_catalog.pg_get_expr(d.adbin, d.adrelid, true), 8) = 'nextval(' THEN 'AUTO_INCREMENT'
    ELSE COALESCE(CASE WHEN a.attgenerated = '' THEN ' DEFAULT ' ELSE ' AS ' END || '(' || pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) || ')', '')
  END col
FROM pg_class c
  JOIN pg_namespace n ON (n.oid = c.relnamespace)
  JOIN pg_attribute a ON (a.attrelid = c.oid)
  JOIN pg_type t ON (t.oid = a.atttypid)
  LEFT JOIN pg_attrdef d ON (d.adrelid = a.attrelid AND d.adnum = a.attnum)
WHERE a.attnum > 0
AND n.nspname = '%s'
AND c.relname = '%s'
ORDER BY a.attnum
), indexdefs AS(
SELECT
  CASE
    WHEN BOOL_OR(ix.indisprimary) THEN 'PRIMARY KEY'
    WHEN BOOL_OR(ix.indisunique) THEN 'UNIQUE KEY `' || i.relname || '`'
    ELSE 'KEY `' || i.relname || '`'
  END
  || '(' || STRING_AGG('`' || a.attname || '`', ', ') || ')' idx
FROM pg_class t, pg_class i, pg_index ix, pg_attribute a, pg_namespace n
WHERE
  t.oid = ix.indrelid
  AND i.oid = ix.indexrelid
  AND n.oid = t.relnamespace
  AND a.attrelid = t.oid
  AND a.attnum = ANY(ix.indkey)
  AND t.relkind = 'r'
  AND n.nspname = '%s'
  AND t.relname = '%s'
GROUP BY i.relname, t.relname
), columndefs_concat AS (
    SELECT STRING_AGG(col, ', ') c FROM columndefs
), indexdefs_concat AS (
    SELECT STRING_AGG(idx, ', ') i FROM indexdefs
)
SELECT 'CREATE OR REPLACE TABLE `%s` (' || c.c || COALESCE(', ' || i.i, '') || ')' FROM columndefs_concat c, indexdefs_concat i;
)";

        std::string sql = mxb::string_printf(CREATE_SQL, table.schema.c_str(), table.name.c_str(), table.schema.c_str(), table.name.c_str(), table.name.c_str());

        debug(sql);

        if (auto res = source.query(sql); res && !(*res).empty() && !(*res)[0].empty() && (*res)[0][0].has_value())
        {
            return {res.value()[0][0].value()};
        }
        else
        {
            std::cout << "Failed to read table definition: " << source.error() << std::endl;
        }

        return {};
    }

    // Get SQL for creating indexes on the given table
    std::vector<std::string> create_index(ODBC &source, const TableInfo &table)
    {
        return {};
    }

    bool threads_started(ODBC &source, const std::vector<TableInfo> &tables)
    {
        // TODO: Unlock the tables
        return true;
    }

    std::string select(ODBC &source, const TableInfo &table)
    {
        const char *format = R"(
SELECT
  'SELECT ' || STRING_AGG(
CASE
  WHEN data_type IN ('point', 'path', 'polygon') THEN 'ST_AsText(CAST(' || QUOTE_IDENT(column_name) || ' AS GEOMETRY))'
  WHEN data_type IN ('geometry') THEN 'ST_AsText(' || QUOTE_IDENT(column_name) || ')'
  ELSE QUOTE_IDENT(column_name)
END, ',' ORDER BY ordinal_position) ||
  ' FROM ' || QUOTE_IDENT(table_schema) || '.' || QUOTE_IDENT(table_name)
FROM information_schema.columns
WHERE table_schema = '%s' AND table_name = '%s' AND is_generated = 'NEVER'
GROUP BY table_schema, table_name
)";

        std::string rval;

        if (auto res = source.query(mxb::string_printf(format, table.schema.c_str(), table.name.c_str())))
        {
            rval = res->front().front().value();
        }

        return rval;
    }

    std::string insert(ODBC &source, const TableInfo &table)
    {
        const char *format = R"(
SELECT
  'INSERT INTO `' || table_name || '`(' ||
  STRING_AGG(QUOTE_IDENT(column_name), ',' ORDER BY ordinal_position) ||
  ') VALUES (' || STRING_AGG(
CASE
  WHEN data_type IN ('point', 'path', 'polygon', 'geometry') THEN 'ST_GeomFromText(?)'
  ELSE '?'
END, ',' ORDER BY ordinal_position) || ')'
FROM information_schema.columns
WHERE table_schema = '%s' AND table_name = '%s' AND is_generated = 'NEVER'
GROUP BY table_schema, table_name
)";

        std::string rval;
        std::string sql = mxb::string_printf(format, table.schema.c_str(), table.name.c_str());

        if (auto res = source.query(sql))
        {
            rval = res->front().front().value();
        }
        else
        {
            debug("INSERT", source.error(), ":", sql);
        }

        return rval;
    }

    bool stop_thread(ODBC &source, const TableInfo &table)
    {
        return true;
    }

private:
    std::string m_trx;
    std::mutex m_lock;
};

//
// A generic translator that uses the ODBC catalog functions to derive the column types and indexes
//
class GenericTranslator final : public Translator
{
public:
    bool prepare(ODBC &db)
    {
        return true;
    }

    bool start(ODBC &source, const std::vector<TableInfo> &tables)
    {
        return true;
    }

    bool start_thread(ODBC &source, const TableInfo &table)
    {
        return true;
    }

    // Should return the CREATE TABLE statement for the table. Should return an empty value (not an empty string) on error.
    std::vector<std::string> create_table(ODBC &source, const TableInfo &table)
    {
        if (auto res = source.columns(table.catalog, table.schema, table.name))
        {
            std::stringstream ss;
            bool more = false;

            for (const auto &c : *res)
            {
                if (std::exchange(more, true))
                {
                    ss << ",";
                }

                ss << "`" << c.COLUMN_NAME << "` " << to_mariadb_type(c);
            }

            return {"CREATE TABLE " + table.name + " (" + ss.str() + ")"};
        }

        return {};
    }

    std::vector<std::string> create_index(ODBC &source, const TableInfo &table)
    {
        struct Index
        {
            std::map<int, std::string> columns;
            bool unique = false;
            bool primary = false;
        };

        std::map<std::string, Index> indexes;

        if (auto res = source.statistics(table.catalog, table.schema, table.name))
        {
            for (const auto &idx : res.value())
            {
                if (!idx.INDEX_NAME.empty())
                {
                    auto &i = indexes[idx.INDEX_NAME];
                    i.columns[idx.ORDINAL_POSITION] = idx.COLUMN_NAME;

                    if (!idx.NON_UNIQUE)
                    {
                        i.unique = true;
                    }
                }
            }
        }

        if (auto res = source.primary_keys(table.catalog, table.schema, table.name))
        {
            for (const auto &pk : res.value())
            {
                if (auto it = indexes.find(pk.PK_NAME); it != indexes.end())
                {
                    it->second.primary = true;
                }
            }
        }
        else
        {
            std::cout << "PRIMARY KEYS FAILED: " << source.error() << std::endl;
        }

        std::vector<std::string> rval;
        std::ostringstream ss;
        ss << "ALTER TABLE \"" << table.catalog << "\".\"" << table.name << "\" ";

        for (const auto &[name, idx] : indexes)
        {
            ss << "ADD ";

            if (idx.primary)
            {
                ss << "PRIMARY KEY ";
            }
            else if (idx.unique)
            {
                ss << "UNIQUE INDEX ";
            }
            else
            {
                ss << "INDEX";
            }

            ss << "(" << mxb::transform_join(idx.columns, std::mem_fn(&decltype(idx.columns)::value_type::second), ",") << ")";

            rval.push_back(ss.str());
        }

        return rval;
    }

    bool threads_started(ODBC &source, const std::vector<TableInfo> &tables)
    {
        return true;
    }

    std::string select(ODBC &source, const TableInfo &table)
    {
        return "SELECT * FROM " + table.schema + "." + table.name;
    }

    std::string insert(ODBC &source, const TableInfo &table)
    {
        auto res = source.columns(table.catalog, table.schema, table.name);

        if (!res)
        {
            return "";
        }

        std::string fields;
        std::string values;
        bool multiple = false;

        for (const auto &c : *res)
        {
            if (std::exchange(multiple, true))
            {
                fields += ",";
                values += ",";
            }

            fields += c.COLUMN_NAME;
            values += "?";
        }

        return "INSERT INTO " + table.catalog + "." + table.name + "(" + fields + ") VALUES (" + values + ")";
    }

    bool stop_thread(ODBC &source, const TableInfo &table)
    {
        return true;
    }
};

std::unique_ptr<Translator> create_translator(Source type)
{
    switch (type)
    {
    case Source::MARIADB:
        return std::make_unique<MariaDBTranslator>();

    case Source::POSTGRES:
        return std::make_unique<PostgresTranslator>();

    case Source::GENERIC:
        return std::make_unique<GenericTranslator>();
    }

    return nullptr;
}
