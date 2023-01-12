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
int NUM_CLIENTS = 100;
int NUM_QUERIES = 1000;

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

    for (MYSQL* c : clients)
    {
        std::string query = "SELECT 1";

        for (int i = 0; i < NUM_QUERIES; i++)
        {
            if (mysql_send_query(c, query.c_str(), query.length()))
            {
                std::cout << "mysql_send_query: " << mysql_error(c) << std::endl;
            }
        }
    }

    for (MYSQL* c : clients)
    {
        for (int i = 0; i < NUM_QUERIES; i++)
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
    
    for (MYSQL* c : clients)
    {
        mysql_close(c);
    }
    
    return 0;
}
