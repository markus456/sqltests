#include "common.hh"

const char* user = "maxuser";
const char* password = "maxpwd";
const char* db = "test";
const char* host = "127.0.0.1";
int port = 4006;
int MAX_ERRORS = 10;
Config cnf;

void one_connection(std::string id)
{
    MYSQL* c = mysql_init(nullptr);

    if (!mysql_real_connect(c, cnf.host, cnf.user, cnf.password, cnf.db, cnf.port, nullptr, 0))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {
        bool ok = true;
        
        while (ok)
        {
            std::vector<std::string> queries {
                "START TRANSACTION",
                "INSERT INTO test.t1 VALUES (" + id + ", 'hello')",
                "SELECT * FROM test.t1 WHERE id = " + id + " FOR UPDATE",
                "UPDATE test.t1 SET data = 'world' WHERE id = " + id,
                "COMMIT",
                
                "START TRANSACTION READ ONLY",
                "SELECT * FROM test.t1 WHERE id = " + id,
                "SELECT * FROM test.t1 WHERE id = " + id,
                "SELECT * FROM test.t1 WHERE id = " + id,
                "SELECT * FROM test.t1 WHERE id = " + id,
                "COMMIT",

                "START TRANSACTION",
                "SELECT * FROM test.t1 WHERE id = " + id + " FOR UPDATE",
                "DELETE FROM test.t1 WHERE id = " + id,
                "COMMIT",
            };

            for (std::string q : queries)
            {
                if (mysql_query(c, q.c_str()))
                {
                    std::cout << "Query failed: " << mysql_error(c) << ": " << q << std::endl;
                    ok = false;
                    break;
                }
                else if (auto* res = mysql_store_result(c))
                {
                    while (auto row = mysql_fetch_row(res))
                    {
                    }

                    mysql_free_result(res);
                }
                
            }
        }
    }

    mysql_close(c);    
}

void func(std::string id)
{
    int errors = 0;
    
    while (errors++ < MAX_ERRORS)
    {
        one_connection(id);
    }
}

int main(int argc, char** argv)
{
    cnf = parse(argc, argv);
    MYSQL* c = mysql_init(nullptr);
    
    if (!mysql_real_connect(c, host, user, password, db, port, nullptr, 0))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else if (mysql_query(c, "CREATE OR REPLACE TABLE test.t1(id BIGINT PRIMARY KEY, data VARCHAR(50)) ENGINE=MEMORY"))
    {
        std::cout << "CREATE failed: " << mysql_error(c) << std::endl;
    }
    else
    {
        std::vector<std::thread> threads;
    
        for (int i = 0; i < 50; i++)
        {
            threads.emplace_back(func, std::to_string(i));
        }

        for (auto& thr : threads)
        {
            thr.join();
        }
    }

    mysql_close(c);

    return 0;
}
