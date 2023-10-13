#include "common.hh"

int main(int argc, char** argv)
{
    Config cnf = parse(argc, argv);
    MYSQL* c = mysql_init(nullptr);

    std::string query = "SELECT '";
    query += std::string(15.5 * 1024.0 * 1024.0, 'a');
    //query += std::string(1.5 * 1024.0 * 1024.0, 'a');
    query += "'";

    if (!mysql_real_connect(c, cnf.host, cnf.user, cnf.password, cnf.db, cnf.port, nullptr, CLIENT_MULTI_STATEMENTS|CLIENT_MULTI_RESULTS))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {
        if (mysql_send_query(c, query.c_str(), query.size()))
        {
            std::cout << "mysql_send_query: " << mysql_error(c) << std::endl;
        }

        exit(0);
    }

    return 0;
}
