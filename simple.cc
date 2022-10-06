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
    MYSQL* c = mysql_init(nullptr);

    if (!mysql_real_connect(c, host, user, password, db, port, nullptr, 0))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {
        while (true)
        {
            if (mysql_query(c, "INSERT INTO test.t1 VALUES (1)"))
            {
                std::cout << "Query failed: " << mysql_error(c) << std::endl;
                break;
            }
            else
            {
                //std::cout << "Press enter to continue..." << std::endl;
                //std::string line;
                //std::getline(std::cin, line);
                
                if (mysql_query(c, "SELECT * FROM test.t1"))
                {
                    std::cout << "Query failed: " << mysql_error(c) << std::endl;
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
    return 0;
}
