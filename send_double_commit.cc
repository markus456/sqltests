#include <mysql.h>
#include <string>
#include <iostream>
#include "common.hh"

void sync_query(MYSQL* c, std::initializer_list<const char*> sync_queries)
{
    for (auto query : sync_queries)
    {
        std::cout << "Query: " << query << std::endl;
        if (mysql_query(c, query))
        {
            std::cout << query << ": " << mysql_error(c) << std::endl;
        }
        std::cout << "Result: " << query << std::endl;
    }
}

void async_query(MYSQL* c, std::initializer_list<const char*> async_queries)
{

    for (std::string query : async_queries) {
        std::cout << "Query: " << query << std::endl;
        if (mysql_send_query(c, query.c_str(), query.length()))
        {
            std::cout << "mysql_send_query: " << mysql_error(c) << std::endl;
        }
    }

    for (auto query : async_queries) {
        std::cout << "Result: " << query << std::endl;
        if (mysql_read_query_result(c))
        {
            std::cout << "mysql_read_query_result: " << mysql_error(c) << std::endl;
        }
        else
        {
            mysql_free_result(mysql_use_result(c));
        }
    }
}

int main(int argc, char** argv)
{
    Config cnf = parse(argc, argv);
    MYSQL* c = mysql_init(nullptr);

    if (!mysql_real_connect(c, cnf.host, cnf.user, cnf.password, cnf.db, cnf.port, nullptr, CLIENT_MULTI_STATEMENTS|CLIENT_MULTI_RESULTS|CLIENT_SESSION_TRACKING))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {

        // async_query(c, {
        //         "BEGIN",
        //         "SELECT @@server_id",
        //         "COMMIT"
        //     });

        async_query(c, {
                "START TRANSACTION READ ONLY",
                "INSERT INTO test.t1 VALUES (1)",
                //"COMMIT"
            });
        
        async_query(c, {
                // "START TRANSACTION READ ONLY",
                // "INSERT INTO test.t1 VALUES (1)",
                "COMMIT"
            });
    }

    mysql_close(c);
    return 0;
}
