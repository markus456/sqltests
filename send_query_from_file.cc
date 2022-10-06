#include <mysql.h>
#include <string>
#include <iostream>
#include <fstream>

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

        while (std::getline(file, line))
        {
            if (mysql_send_query(c, line.c_str(), line.length()))
            {
                std::cout << "mysql_send_query: " << mysql_error(c) << std::endl;
                break;
            }
            
            results++;
        }


        for (int i = 0; i < results; i++)
        {
            if (mysql_read_query_result(c))
            {
                std::cout << "mysql_read_query_result: " << mysql_error(c) << std::endl;
                break;
            }
            else
            {
                mysql_free_result(mysql_use_result(c));
            }
        }
    }

    mysql_close(c);
    return 0;
}
