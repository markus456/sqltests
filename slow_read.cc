#include <mysql.h>
#include <string>
#include <iostream>
#include <unistd.h>

const char* user = "maxuser";
const char* password = "maxpwd";
const char* db = "test";
const char* host = "127.0.0.1";
int port = 4006;

int main(int argc, char** argv)
{
    if (argc < 2)
    {
        std::cout << "USAGE: QUERY [SLEEP]" << std::endl;;
    }

    int dur = argc < 3 ? 1 : atoi(argv[2]);
    MYSQL* c = mysql_init(nullptr);

    if (!mysql_real_connect(c, host, user, password, db, port, nullptr, 0))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {
        if (mysql_query(c, argv[1]))
        {
            std::cout << "Query failed: " << mysql_error(c) << std::endl;
        }
        else if (auto* res = mysql_use_result(c))
        {
            std::cout << "Got result, reading one row per second" << std::endl;

            while (auto row = mysql_fetch_row(res))
            {
                sleep(dur);
            }
                
            mysql_free_result(res);
        }
        else
        {
            std::cout << "No result: " << mysql_error(c) << std::endl;
        }
    }

    mysql_close(c);
    return 0;
}
