#include "common.hh"
#include "diagnostics.hh"
#include "odbc.hh"
#include "odbc_helpers.hh"
#include "string_utils.hh"

#include "dialect.hh"

#include <algorithm>
#include <cassert>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <sstream>
#include <vector>
#include <set>
#include <string>
#include <string_view>
#include <functional>
#include <utility>

#include <unistd.h>

int main(int argc, char **argv)
{
    auto cnf = parse(argc, argv);

    if (cnf.args.empty())
    {
        std::cout << "Usage: " << argv[0] << "[OPTIONS] { query | tables | columns | statistics }" << std::endl;
        return 1;
    }

    set_debug(cnf.debug);

    std::string cmd;

    if (cnf.args.size() > 0)
    {
        cmd = cnf.args[0];
    }

    std::set<std::string> info_commands = {"columns", "tables", "statistics", "primary_keys"};

    if (cmd == "query" && cnf.args.size() < 2)
    {
        std::cout << "Usage: " << argv[0] << "[OPTIONS] query SQL " << std::endl;
        return 1;
    }
    else if (auto it = info_commands.find(cmd); it != info_commands.end() && cnf.args.size() < 4)
    {
        std::cout << "Usage: " << argv[0] << "[OPTIONS] " << *it << " CATALOG SCHEMA TABLE " << std::endl;
        return 1;
    }
    else if (cmd == "copy" && cnf.args.size() < 2)
    {
        std::cout << "Usage: " << argv[0] << "[OPTIONS] copy TABLE ... " << std::endl;
        return 1;
    }

    ODBC odbc(cnf.dsn1);

    if (cmd == "drivers")
    {
        for (const auto &[driver, params] : odbc.drivers())
        {
            std::cout << driver << std::endl;

            for (const auto &[k, v] : params)
            {
                std::cout << k << "=" << v << std::endl;
            }

            std::cout << std::endl;
        }
    }
    else if (odbc.connect())
    {
        std::cout << "Connection successful" << std::endl;

        std::optional<ODBC::Result> res;

        if (cmd == "tables")
        {
            res = odbc.tables(cnf.args[1], cnf.args[2], cnf.args[3]);
        }
        else if (cmd == "columns")
        {
            if (auto cols = odbc.columns(cnf.args[1], cnf.args[2], cnf.args[3]); cols)
            {
                std::cout << "COLUMNS: " << cols->size() << std::endl;
                for (const auto &c : *cols)
                {
                    std::cout << "TABLE_CAT: " << c.TABLE_CAT << std::endl;
                    std::cout << "TABLE_SCHEM: " << c.TABLE_SCHEM << std::endl;
                    std::cout << "TABLE_NAME: " << c.TABLE_NAME << std::endl;
                    std::cout << "COLUMN_NAME: " << c.COLUMN_NAME << std::endl;
                    std::cout << "DATA_TYPE: " << type_to_str(c.DATA_TYPE) << "[" << c.DATA_TYPE << "]" << std::endl;
                    std::cout << "TYPE_NAME: " << c.TYPE_NAME << std::endl;
                    std::cout << "COLUMN_SIZE: " << c.COLUMN_SIZE << std::endl;
                    std::cout << "BUFFER_LENGTH: " << c.BUFFER_LENGTH << std::endl;
                    std::cout << "DECIMAL_DIGITS: " << c.DECIMAL_DIGITS << std::endl;
                    std::cout << "NUM_PREC_RADIX: " << c.NUM_PREC_RADIX << std::endl;
                    std::cout << "NULLABLE: " << c.NULLABLE << std::endl;
                    std::cout << "REMARKS: " << c.REMARKS << std::endl;
                    std::cout << "COLUMN_DEF: " << c.COLUMN_DEF << std::endl;
                    std::cout << "SQL_DATA_TYPE: " << c.SQL_DATA_TYPE << std::endl;
                    std::cout << "SQL_DATETIME_SUB: " << c.SQL_DATETIME_SUB << std::endl;
                    std::cout << "CHAR_OCTET_LENGTH: " << c.CHAR_OCTET_LENGTH << std::endl;
                    std::cout << "ORDINAL_POSITION: " << c.ORDINAL_POSITION << std::endl;
                    std::cout << "IS_NULLABLE: " << c.IS_NULLABLE << std::endl;
                    int extra = 1;
                    for (auto &v : c.extra_values)
                    {
                        std::cout << "Extra " << extra++ << ": " << v << std::endl;
                    }
                    std::cout << std::endl;
                }
            }
            else
            {
                std::cout << "Error: " << odbc.error() << std::endl;
            }
        }
        else if (cmd == "statistics")
        {
            if (auto stats = odbc.statistics(cnf.args[1], cnf.args[2], cnf.args[3]); stats)
            {
                std::cout << "STATISTICS: " << stats->size() << std::endl;

                for (const auto &s : *stats)
                {
                    std::cout << "TABLE_CAT: " << s.TABLE_CAT << std::endl;
                    std::cout << "TABLE_SCHEM: " << s.TABLE_SCHEM << std::endl;
                    std::cout << "TABLE_NAME: " << s.TABLE_NAME << std::endl;
                    std::cout << "NON_UNIQUE: " << s.NON_UNIQUE << std::endl;
                    std::cout << "INDEX_QUALIFIER: " << s.INDEX_QUALIFIER << std::endl;
                    std::cout << "INDEX_NAME: " << s.INDEX_NAME << std::endl;
                    std::cout << "TYPE: " << index_type_to_str(s.TYPE) << s.TYPE << std::endl;
                    std::cout << "ORDINAL_POSITION: " << s.ORDINAL_POSITION << std::endl;
                    std::cout << "COLUMN_NAME: " << s.COLUMN_NAME << std::endl;
                    std::cout << "ASC_OR_DESC: " << s.ASC_OR_DESC << std::endl;
                    std::cout << "CARDINALITY: " << s.CARDINALITY << std::endl;
                    std::cout << "PAGES: " << s.PAGES << std::endl;
                    std::cout << "FILTER_CONDITION: " << s.FILTER_CONDITION << std::endl;
                    std::cout << std::endl;
                }
            }
            else
            {
                std::cout << "Error: " << odbc.error() << std::endl;
            }
        }
        else if (cmd == "primary_keys")
        {
            if (auto keys = odbc.primary_keys(cnf.args[1], cnf.args[2], cnf.args[3]); keys)
            {
                std::cout << "PRIMARY KEYS: " << keys->size() << std::endl;

                for (const auto &k : *keys)
                {
                    std::cout << "TABLE_CAT: " << k.TABLE_CAT << std::endl;
                    std::cout << "TABLE_SCHEM: " << k.TABLE_SCHEMA << std::endl;
                    std::cout << "TABLE_NAME: " << k.TABLE_NAME << std::endl;
                    std::cout << "COLUMN_NAME: " << k.COLUMN_NAME << std::endl;
                    std::cout << "KEY_SEQ: " << k.KEY_SEQ << std::endl;
                    std::cout << "PK_NAME: " << k.PK_NAME << std::endl;
                    std::cout << std::endl;
                }
            }
            else
            {
                std::cout << "Error: " << odbc.error() << std::endl;
            }
        }
        else if (cmd == "query")
        {
            res = odbc.query(cnf.args[1]);
        }
        else if (cmd == "create_table")
        {
            TableInfo t{cnf.args[1], cnf.args[2], cnf.args[3]};
            auto translator = create_translator(cnf.dsn1.find("Postgres") != std::string::npos ? Source::POSTGRES : Source::MARIADB);
            translator->prepare(odbc);
            translator->start(odbc, {t});

            for (auto create : translator->create_table(odbc, t))
            {
                std::cout << create << std::endl;
            }
        }
        else if (cmd == "copy")
        {
            Source source_type = Source::MARIADB;

            if (cnf.dsn1.find("Postgres") != std::string::npos)
            {
                source_type = Source::POSTGRES;
                std::cout << "Source is Postgres" << std::endl;
            }

            std::vector<TableInfo> tables;

            for (auto it = cnf.args.begin() + 1; it != cnf.args.end(); ++it)
            {
                std::cout << *it << std::endl;
                TableInfo t;
                auto tok = mxb::strtok(*it, ".");

                if (tok.size() == 3)
                {
                    t.catalog = tok[0];
                    t.schema = tok[1];
                    t.name = tok[2];
                }
                else if (tok.size() == 2)
                {
                    t.catalog = tok[0];
                    t.name = tok[1];
                }
                else
                {
                    std::cout << "Bad table name: " << *it << std::endl;
                    return 1;
                }

                auto res = odbc.columns(t.catalog, t.schema, t.name);

                if (!res || res->empty())
                {
                    std::cout << "Failed to fetch column information" << odbc.error() << std::endl;
                    return 1;
                }

                for (auto col : *res)
                {
                    ColumnInfo c;
                    c.name = col.COLUMN_NAME;
                    c.data_type = col.DATA_TYPE;
                    c.type = sql_to_mariadb_type(col.DATA_TYPE, col.COLUMN_SIZE);
                    c.size = col.COLUMN_SIZE;
                    c.buffer_size = std::max({col.BUFFER_LENGTH, col.CHAR_OCTET_LENGTH, col.COLUMN_SIZE});
                    c.digits = col.NUM_PREC_RADIX;
                    c.nullable = col.IS_NULLABLE == "YES";

                    //t.columns.push_back(std::move(c));
                }

                tables.push_back(std::move(t));
            }

            auto translator = create_translator(source_type);

            if (!odbc.copy_table(cnf.dsn1, cnf.dsn2, std::move(translator), tables))
            {
                std::cout << "Error: " << odbc.error() << std::endl;
            }
        }

        if (res)
        {
            odbc.print_result(res.value());
        }
        else if (auto error = odbc.error(); !error.empty())
        {
            std::cout << "Error: " << error << std::endl;
        }

        odbc.disconnect();
    }
    else
    {
        std::cout << "Connection failed: " << odbc.error() << std::endl;
    }

    return 0;
}
