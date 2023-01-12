#include <mysql.h>
#include <unistd.h>
#include <string>
#include <iostream>
#include <vector>
#include <cstring>
#include "common.hh"

int main(int argc, char** argv)
{
    auto cnf = parse(argc, argv);

    if (cnf.args.empty())
    {
        std::cout << "Usage: NUM_CONNECTIONS [SLEEP]" << std::endl;
        return 1;
    }
    
    int n = atoi(cnf.args[0].c_str());
    std::vector<MYSQL*> connections;

    int time_to_sleep = cnf.args.size() > 1 ? atoi(cnf.args[1].c_str()) : 5;

    std::cout << "Creating " << n << " connections" << std::endl;
    
    for (int i = 0; i < n; i++)
    {
        connections.push_back(mysql_init(nullptr));

        //char* buf = strdup("PROXY TCP4 192.168.0.1 192.168.0.11 56324 443\r\n");
        //mysql_optionsv(connections.back(), MARIADB_OPT_PROXY_HEADER, buf, strlen(buf));
        //free(buf);
    
        if (!mysql_real_connect(connections.back(), cnf.host, cnf.user, cnf.password, cnf.db, cnf.port, nullptr, 0))
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

    std::cout << "Sleeping for " << time_to_sleep << " seconds" << std::endl;

    sleep(time_to_sleep);

    for (auto* c : connections)
    {
        mysql_close(c);
    }
 
    return 0;
}
