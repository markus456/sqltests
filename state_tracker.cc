#include "common.hh"

void print_status_changes(MYSQL* c)
{
    const char* ptr = nullptr;
    size_t len = 0;

    if (mysql_session_track_get_first(c, SESSION_TRACK_SYSTEM_VARIABLES, &ptr, &len) == 0)
    {
        std::cout << std::string_view(ptr, len) << std::endl;

        while (mysql_session_track_get_next(c, SESSION_TRACK_SYSTEM_VARIABLES, &ptr, &len) == 0)
        {
            std::cout << std::string_view(ptr, len) << std::endl;
        }
    }
}

int main(int argc, char** argv)
{
    Config cnf = parse(argc, argv);
    MYSQL* c = mysql_init(nullptr);

    if (!mysql_real_connect(c, cnf.host, cnf.user, cnf.password, cnf.db, cnf.port, nullptr, CLIENT_MULTI_STATEMENTS|CLIENT_MULTI_RESULTS))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {
        auto queries = {
            "SELECT 1",
            "SET autocommit=0",
            "SET autocommit=1,tx_isolation='REPEATABLE-READ'",
            "CREATE OR REPLACE TABLE test.t1(id INT)",
            "INSERT INTO test.t1 VALUES (1)",
            "SET @myvar=1,autocommit=0,sql_notes=0",
            "SELECT 1",
            "COMMIT",
        };
        
        for (auto query : queries)
        {
            std::cout << query << std::endl;
            mysql_query(c, query);
            auto res = mysql_store_result(c);
            print_status_changes(c);
            mysql_free_result(res);
            std::cout << std::endl;
            sleep(1);
        }
    }

    mysql_close(c);
    return 0;
}
