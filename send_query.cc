#include <mysql.h>
#include <string>
#include <iostream>
#include "common.hh"

const char* user = "maxuser";
const char* password = "maxpwd";
const char* db = "test";
const char* host = "127.0.0.1";
int port = 3000;

#define USE_ASYNC 1
int NUM_CLIENTS = 1;
int NUM_QUERIES = 400000;

int main(int argc, char** argv)
{
    auto cnf = parse(argc, argv);
    std::vector<MYSQL*> clients;

    for (int i = 0; i < NUM_CLIENTS; i++)
    {
        MYSQL* c = mysql_init(nullptr);

        if (!mysql_real_connect(c, cnf.host, cnf.user, cnf.password, cnf.db, cnf.port, nullptr, 0))
        {
            std::cout << "Failed to connect: " << mysql_error(c) << std::endl;
            mysql_close(c);
        }
        else
        {
            clients.push_back(c);
        }
    }

    int results = 0;

    for (MYSQL* c : clients)
    {
        mysql_query(c, "SET autocommit=0");

        std::string query = "INSERT INTO test.t1 VALUES ('hello')";

        for (int i = 0; i < NUM_QUERIES / 1000; i++)
        {
            for (int b = 0; b < 1000; b++)
            {
                if (mysql_send_query(c, query.c_str(), query.length()))
                {
                    std::cout << "mysql_send_query: " << mysql_error(c) << std::endl;
                }
            }

            std::string commit = "COMMIT";
            mysql_send_query(c, commit.c_str(), commit.size());

            for (int b = 0; b < 1001; b++)
            {
                if (mysql_read_query_result(c))
                {
                    std::cout << "mysql_read_query_result: " << mysql_error(c) << std::endl;
                }
                else
                {
                    mysql_free_result(mysql_use_result(c));
                }
            }

        }
    }

    for (MYSQL* c : clients)
    {
        mysql_close(c);
    }

    return 0;
}
