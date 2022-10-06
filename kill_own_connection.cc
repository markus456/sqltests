#include <mysql.h>
#include <string>
#include <iostream>
#include <sstream>
#include <thread>
#include <chrono>
#include <vector>
#include <unistd.h>

const char* user = "maxuser";
const char* password = "maxpwd";
const char* db = "test";
const char* host = "127.0.0.1";
int port = 4006;

int main(int argc, char** argv)
{
    constexpr int LIMIT = 10000;
    constexpr int THREADS = 20;

    std::vector<std::thread> threads;

    for (int t = 0; t < THREADS; t++)
    {
        auto fn = [&, t](){            
            for (int i = 0; i < LIMIT; i++)
            {
                MYSQL* c = mysql_init(nullptr);

                int timeout = 10;
                mysql_optionsv(c, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
                mysql_optionsv(c, MYSQL_OPT_READ_TIMEOUT, &timeout);
                mysql_optionsv(c, MYSQL_OPT_WRITE_TIMEOUT, &timeout);
        
                if (!mysql_real_connect(c, host, user, password, db, port, nullptr, 0))
                {
                    std::cout << "Connect: " << mysql_error(c) << std::endl;
                }
                else
                {
                    if (t != 0)
                    {
                        sleep(1);
                    }

                    if (mysql_query(c, "SELECT 1"))
                    {
                        std::cout << "Error: " << mysql_error(c) << std::endl;
                    }

                    mysql_free_result(mysql_use_result(c));
            
                    auto id = mysql_thread_id(c);
                    std::ostringstream ss;
                    ss << "KILL " << id;

                    if (mysql_query(c, ss.str().c_str()))
                    {
                        std::cout << "Error: " << mysql_error(c) << std::endl;
                    }
                }

                mysql_close(c);
            }
        };
        
        threads.emplace_back(fn);
    }

    for (auto& t : threads)
    {
        t.join();
    }
    
    return 0;
}
