#include <mysql.h>
#include <atomic>
#include <string>
#include <iostream>
#include <sstream>
#include <thread>
#include <chrono>

const char* user = "maxuser";
const char* password = "maxpwd";
const char* db = "test";
const char* host = "127.0.0.1";
int port = 4006;

using Clock = std::chrono::steady_clock;
using namespace std::literals::chrono_literals;
using namespace std::chrono;

int main(int argc, char** argv)
{
    MYSQL* c = mysql_init(nullptr);
        
    int timeout = 60;
    mysql_optionsv(c, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    mysql_optionsv(c, MYSQL_OPT_READ_TIMEOUT, &timeout);
    mysql_optionsv(c, MYSQL_OPT_WRITE_TIMEOUT, &timeout);
        
    if (!mysql_real_connect(c, host, user, password, db, port, nullptr, 0))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {
        if (mysql_query(c, "set autocommit=1, session_track_schema=1, "
                        "session_track_system_variables='auto_increment_increment' "
                        ", sql_mode = concat(@@sql_mode,',STRICT_TRANS_TABLES')"))
        {
            std::cout << "First SET Error: " << mysql_error(c) << std::endl;
        }

        for (int i = 0; i < 700; i++)
        {
            if (mysql_query(c, "SET autocommit=0") || mysql_query(c, "SET autocommit=1"))
            {
                std::cout << "SET Error: " << mysql_error(c) << std::endl;
                break;
            }
        }

        std::cout << "SET statements done, press enter to continue..." << std::endl;
        std::string line;
        std::getline(std::cin, line);

        for (int i = 0; i < 1000; i++)
        {
            if (mysql_query(c, "SELECT 1"))
            {
                std::cout << "SELECT Error: " << mysql_error(c) << std::endl;
                break;
            }
            else if (auto res = mysql_use_result(c))
            {
                mysql_free_result(res);
            }

            std::cout << i << std::endl;
            std::this_thread::sleep_for(1s);
        }
    }

    mysql_close(c);
    
    return 0;
}
