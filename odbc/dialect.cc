#include "dialect.hh"
#include "odbc.hh"
#include "string_utils.hh"
#include "diagnostics.hh"

#include <utility>
#include <iostream>
#include <functional>

class MariaDBTranslator final : public Translator
{
public:
    bool prepare(ODBC &db)
    {
        return db.query("SET SQL_MODE='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'") && db.query("SET AUTOCOMMIT=0");
    }

    bool start(ODBC &source, const std::vector<TableInfo> &tables)
    {
        return !!source.query("FLUSH TABLES WITH READ LOCK");
    }

    bool start_thread(ODBC &source, const TableInfo &table)
    {
        return !!source.query("START TRANSACTION WITH CONSISTENT SNAPSHOT");
    }

    // Should return the CREATE TABLE statement for the table. Should return an empty value (not an empty string) on error.
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

    // Get SQL for creating indexes on the given table
    std::vector<std::string> create_index(ODBC &source, const TableInfo &table)
    {
        return {};
    }

    // Called for the main connection after all threads have been successfully started
    bool threads_started(ODBC &source, const std::vector<TableInfo> &tables)
    {
        return !!source.query("UNLOCK TABLES");
    }

    // Should return the SQL needed to read the data from the source.
    std::string select(ODBC &source, const TableInfo &table)
    {
        return "SELECT * FROM `" + table.catalog + "`.`" + table.name + "`";
    }

    // Called when the dump has finished.
    bool stop_thread(ODBC &source, const TableInfo &table)
    {
        return true;
    }
};

// Generic ODBC translator, simply looks at the datatypes reported by ODBC and uses them to create the table.
class PostgresTranslator final : public Translator
{
public:
    bool prepare(ODBC &source)
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

    std::vector<std::string> create_table(ODBC &source, const TableInfo &table)
    {
        const char *CREATE_SQL = R"(
WITH coldefs AS (
  SELECT
    QUOTE_IDENT(column_name) || ' ' ||
    CASE
      WHEN data_type LIKE 'timestamp%%' THEN 'TIMESTAMP'
      WHEN data_type LIKE 'time%%' THEN 'TIME'
      WHEN data_type = 'jsonb' THEN 'JSON'
      WHEN column_default LIKE 'nextval%%' THEN 'SERIAL'
      ELSE data_type
    END ||
    COALESCE('(' ||
      CASE
        WHEN data_type = 'numeric' THEN numeric_precision || ',' || numeric_scale
        ELSE CAST(character_maximum_length AS text)
      END
    || ')', '') ||
    CASE
      WHEN is_nullable = 'YES' THEN ''
      ELSE ' NOT NULL'
    END ||
    CASE
      WHEN column_default LIKE 'nextval%%' THEN ''
      WHEN is_generated = 'ALWAYS' THEN ' AS ' || generation_expression
      ELSE COALESCE(' DEFAULT ' || CASE WHEN data_type = 'text' THEN LEFT(column_default, -6) ELSE column_default END, '')
    END column_def
  FROM information_schema.columns
  WHERE table_schema = '%s' AND table_name = '%s'
  ORDER BY ordinal_position
)
SELECT 'CREATE TABLE `%s` (' || STRING_AGG(column_def, ', ') || ');' create_sql FROM coldefs;
)";

        std::string sql = mxb::string_printf(CREATE_SQL, table.schema.c_str(), table.name.c_str(), table.name.c_str());

        debug(sql);

        // std::ostringstream ss;

        // ss << "WITH cols AS"
        //    << "(SELECT "
        //    << " QUOTE_IDENT(column_name) || ' ' || "
        //    << " CASE WHEN data_type LIKE 'timestamp%' THEN 'TIMESTAMP' WHEN data_type LIKE 'time%' THEN 'TIME' ELSE data_type END || "
        //    << " COALESCE('(' || CASE WHEN data_type = 'numeric' THEN numeric_precision || ',' || numeric_scale ELSE CAST(character_maximum_length AS text) END || ')', '') || "
        //    << " CASE WHEN is_nullable = 'YES' THEN '' ELSE ' NOT NULL' END || "
        //    << " CASE WHEN is_generated = 'ALWAYS' THEN ' AS ' || generation_expression ELSE COALESCE(' DEFAULT ' || CASE WHEN data_type = 'text' THEN LEFT(column_default, -6) ELSE column_default END, '') END AS column_def"
        //    << " FROM information_schema.columns WHERE "
        //    << " table_schema = '" << table.schema << "' AND table_name = '" << table.name << "' "
        //    << " ORDER BY ordinal_position) "
        //    << "SELECT 'CREATE TABLE \"" << table.catalog << "\".\"" << table.name + "\"(' || STRING_AGG(column_def, ', ') || ')' FROM cols;";

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
        const char *INDEX_SQL = R"(
  SELECT 
    'ALTER TABLE `%s` ADD ' ||
    CASE
    WHEN BOOL_OR(ix.indisprimary)
      THEN 'PRIMARY KEY'
    WHEN BOOL_OR(ix.indisunique)
      THEN 'UNIQUE INDEX ' || i.relname
      ELSE 'INDEX ' || i.relname
    END
    || '(' || STRING_AGG(a.attname, ',') || ');' index_def
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
)";

        std::string sql = mxb::string_printf(INDEX_SQL, table.name.c_str(), table.schema.c_str(), table.name.c_str());
        std::vector<std::string> rval;

        debug(sql);

        if (auto res = source.query(sql); res && !res->empty() && !res->front().empty())
        {
            for (auto &val : res.value())
            {
                if (val.front().has_value())
                {
                    rval.push_back(std::move(val.front().value()));
                }
            }
        }
        else
        {
            std::cout << "Failed to read index definitions: " << source.error() << std::endl;
        }

        return rval;

        //
        // A "generic" approach that uses the ODBC catalog functions to derive the indexes
        //

        // struct Index
        // {
        //     std::map<int, std::string> columns;
        //     bool unique = false;
        //     bool primary = false;
        // };

        // std::map<std::string, Index> indexes;

        // if (auto res = source.statistics(table.catalog, table.schema, table.name))
        // {
        //     for (const auto &idx : res.value())
        //     {
        //         if (!idx.INDEX_NAME.empty())
        //         {
        //             auto &i = indexes[idx.INDEX_NAME];
        //             i.columns[idx.ORDINAL_POSITION] = idx.COLUMN_NAME;

        //             if (!idx.NON_UNIQUE)
        //             {
        //                 i.unique = true;
        //             }
        //         }
        //     }
        // }

        // if (auto res = source.primary_keys(table.catalog, table.schema, table.name))
        // {
        //     for (const auto &pk : res.value())
        //     {
        //         if (auto it = indexes.find(pk.PK_NAME); it != indexes.end())
        //         {
        //             it->second.primary = true;
        //         }
        //     }
        // }
        // else
        // {
        //     std::cout << "PRIMARY KEYS FAILED: " << source.error() << std::endl;
        // }

        // std::vector<std::string> rval;
        // std::ostringstream ss;
        // ss << "ALTER TABLE \"" << table.catalog << "\".\"" << table.name << "\" ";

        // for (const auto &[name, idx] : indexes)
        // {
        //     ss << "ADD ";

        //     if (idx.primary)
        //     {
        //         ss << "PRIMARY KEY ";
        //     }
        //     else if (idx.unique)
        //     {
        //         ss << "UNIQUE INDEX ";
        //     }
        //     else
        //     {
        //         ss << "INDEX";
        //     }

        //     ss << "(" << mxb::transform_join(idx.columns, std::mem_fn(&decltype(idx.columns)::value_type::second), ",") << ")";

        //     rval.push_back(ss.str());
        // }

        // return rval;
    }

    // Called for the main connection after all threads have been successfully started
    bool threads_started(ODBC &source, const std::vector<TableInfo> &tables)
    {
        return true;
    }

    std::string select(ODBC &source, const TableInfo &table)
    {
        return "SELECT * FROM \"" + table.catalog + "\".\"" + table.schema + "\".\"" + table.name + "\"";
    }

    // Called when the dump has finished.
    bool stop_thread(ODBC &source, const TableInfo &table)
    {
        return true;
    }

private:
    std::string m_trx;
};

std::unique_ptr<Translator> create_translator(Source type)
{
    switch (type)
    {
    case Source::MARIADB:
        return std::make_unique<MariaDBTranslator>();

    case Source::POSTGRES:
        return std::make_unique<PostgresTranslator>();
    }

    return nullptr;
}
