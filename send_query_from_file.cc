#include <mysql.h>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>

const char* user = "maxuser";
const char* password = "maxpwd";
const char* db = "test";
const char* host = "127.0.0.1";
int port = 4006;

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
        int results = 0;
        std::ifstream file(argv[1]);
        std::string line;
        std::vector<std::string> queries;

        while (std::getline(file, line))
        {
            queries.push_back(line);

            results++;
        }

        for (const auto& q : queries)
        {
            if (mysql_send_query(c, q.c_str(), q.length()))
            {
                std::cout << "mysql_send_query: " << mysql_error(c) << std::endl;
                return 1;
            }
        }

        for (const auto& q : queries)
        {
            if (mysql_read_query_result(c))
            {
                std::cout << "mysql_read_query_result: " << mysql_error(c) << std::endl;
                return 1;
            }
            else if (auto res = mysql_use_result(c))
            {
                while (auto row = mysql_fetch_row(res))
                {
                    for (int i = 0; i < mysql_num_fields(res); i++)
                    {
                        std::cout << row[i] << "\t";
                    }

                    std::cout << std::endl;
                }

                mysql_free_result(res);
            }
        }

        /*
        mysql_close(c);
        c = mysql_init(nullptr);
        if (!mysql_real_connect(c, host, user, password, db, port, nullptr, 0))
        {
            std::cout << "Connect: " << mysql_error(c) << std::endl;
            return 1;
        }


        for (const auto& q : queries)
        {
            if (mysql_query(c, q.c_str()))
            {
                std::cout << "mysql_real_query: " << mysql_error(c) << std::endl;
                return 1;
            }
            else
            {
                mysql_free_result(mysql_use_result(c));
            }
        }
        */
    }

    mysql_close(c);
    return 0;
}
