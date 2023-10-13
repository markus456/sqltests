#include <mysql.h>
#include <string>
#include <iostream>
#include <unistd.h>

const char* user = "maxuser";
const char* password = "maxpwd";
const char* db = "test";
const char* host = "127.0.0.1";
int port = 3000;

int main(int argc, char** argv)
{
    MYSQL* c = mysql_init(nullptr);
    unsigned long map = 16546876;
    mysql_optionsv(c, MYSQL_OPT_MAX_ALLOWED_PACKET, &map);

    if (!mysql_real_connect(c, host, user, password, db, port, nullptr, 0))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {
        for (std::string query; std::getline(std::cin, query);)
        {
            std::cout << "Query: " << query << std::endl;

            if (mysql_query(c, query.c_str()))
            {
                std::cout << "Query failed: " << mysql_error(c) << std::endl;
                break;
            }
            else if (auto* res = mysql_store_result(c))
            {
                while (auto row = mysql_fetch_row(res))
                {
                    for (int i = 0; i < mysql_num_fields(res); i++)
                    {
                        std::cout << row[i] << " ";
                    }

                    std::cout << std::endl;
                }

                mysql_free_result(res);
            }

            if (mysql_errno(c))
            {
                std::cout << "Error after result: " << mysql_error(c) << std::endl;
            }
        }
    }

    mysql_close(c);
    return 0;
}
