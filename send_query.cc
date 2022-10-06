#include <mysql.h>
#include <string>
#include <iostream>

const char* user = "maxuser";
const char* password = "maxpwd";
const char* db = "test";
const char* host = "127.0.0.1";
int port = 3000;

#define USE_ASYNC 0

int main(int argc, char** argv)
{
    MYSQL* c = mysql_init(nullptr);

    if (!mysql_real_connect(c, host, user, password, db, port, nullptr, 0))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {
        if (mysql_query(c, "BEGIN"))
        {
            std::cout << "BEGIN: " << mysql_error(c) << std::endl;
        }

        std::string query = "SELECT * FROM test.t1 FOR UPDATE";

#if USE_ASYNC
        if (mysql_send_query(c, query.c_str(), query.length()))
        {
            std::cout << "mysql_send_query: " << mysql_error(c) << std::endl;
        }

        std::cout << "Press enter to continue..." << std::endl;
        std::string line;
        std::getline(std::cin, line);

        if (mysql_read_query_result(c))
        {
            std::cout << "mysql_read_query_result: " << mysql_error(c) << std::endl;
        }
        else
        {
            mysql_free_result(mysql_use_result(c));
        }
#else
        if (mysql_real_query(c, query.c_str(), query.length()))
        {
            std::cout << "mysql_real_query: " << mysql_error(c) << std::endl;
        }
        else
        {
            if (auto* res = mysql_use_result(c))
            {
                int i = 0;
                
                while (auto row = mysql_fetch_row(res))
                {
                    ++i;
                }

                std::cout << "Got result (" << i << " rows): " << mysql_errno(c) << ", " << mysql_error(c) << std::endl;
                
                mysql_free_result(res);
            }
            else
            {
                std::cout << "No result: " << mysql_error(c) << std::endl;
            }
        }
#endif
    }

    mysql_close(c);
    return 0;
}
