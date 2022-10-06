#include <mysql.h>
#include <unistd.h>
#include <string>
#include <iostream>
#include <vector>
#include <cstring>

const char* user = "maxusers";
const char* password = "maxpwds";
const char* db = "test";
//const char* host = "127.0.0.1";
const char* host = "192.168.1.48";
int port = 3000;

int main(int argc, char** argv)
{
    if (argc < 2)
    {
        std::cout << "Usage: NUM_CONNECTIONS [SLEEP]" << std::endl;
        return 1;
    }
    
    int n = atoi(argv[1]);
    std::vector<MYSQL*> connections;

    int time_to_sleep = argc > 2 ? atoi(argv[2]) : 5;
    
    for (int i = 0; i < n; i++)
    {
        connections.push_back(mysql_init(nullptr));

        //char* buf = strdup("PROXY TCP4 192.168.0.1 192.168.0.11 56324 443\r\n");
        //mysql_optionsv(connections.back(), MARIADB_OPT_PROXY_HEADER, buf, strlen(buf));
        //free(buf);
    
        if (!mysql_real_connect(connections.back(), host, user, password, db, port, nullptr, 0))
        {
            std::cout << "Connection " << i + 1 << " failed: " << mysql_error(connections.back()) << std::endl;
        }
        else if (mysql_query(connections.back(), "SELECT 1"))
        {
            std::cout << "Query " << i + 1 << " failed: " << mysql_error(connections.back()) << std::endl;
        }
        else
        {
            mysql_free_result(mysql_use_result(connections.back()));
        }
    }


    sleep(time_to_sleep);

    for (auto* c : connections)
    {
        mysql_close(c);
    }
 
    return 0;
}
