#include "common.hh"
#include <mysql.h>
#include <string>
#include <iostream>
#include <unistd.h>
#include <chrono>
#include <thread>

int main(int argc, char** argv)
{
    using namespace std::chrono_literals;
    auto cnf = parse(argc, argv);
    bool ok = true;
    auto queries = {"SET @a = 1", "SELECT 1"};

    for (int i = 0; i < 100; i++)
    {
        auto tick = 80ms;

        while (tick < 95ms)
        {
            MYSQL* c = mysql_init(nullptr);

            if (!mysql_real_connect(c, cnf.host, cnf.user, cnf.password, nullptr, cnf.port, nullptr, 0))
            {
                std::cout << "Connect: " << mysql_error(c) << std::endl;
                ok = false;
            }
            else
            {
                std::cout << "Delay: " << tick.count() << std::endl;

                for (auto query : queries)
                {
                    if (mysql_query(c, query))
                    {
                        std::cout << "Query failed: " << mysql_error(c) << std::endl;
                        ok = false;
                    }

                    mysql_free_result(mysql_use_result(c));
                    std::this_thread::sleep_for(tick);
                }

                tick += 1ms;
            }
        
            mysql_close(c);
        
        }
    }

    return 0;
}
