#include "common.hh"

int main(int argc, char** argv)
{
    Config cnf = parse(argc, argv);
    int NUM = cnf.args.empty() ? 1000 : std::stoi(cnf.args[0]);
    MYSQL* c = mysql_init(nullptr);
    int num_bytes = 0;
    int i = 0;

    while (num_bytes < NUM)
    {
        std::string key = "Hello" + std::to_string(i);
        std::string value = "World" + std::to_string(i);
        ++i;
        num_bytes += key.size() + value.size();

        if (mysql_optionsv(c, MYSQL_OPT_CONNECT_ATTR_ADD, key.c_str(), value.c_str()))
        {
            std::cout << "Attr at " << i << ": " << mysql_error(c) << std::endl;
            break;
        }
    }
    
    if (!mysql_real_connect(c, cnf.host, cnf.user, cnf.password, cnf.db, cnf.port, nullptr, CLIENT_MULTI_STATEMENTS|CLIENT_MULTI_RESULTS))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else if (mysql_query(c, "SELECT 1"))
    {
        std::cout << "Query: " << mysql_error(c) << std::endl;
    }
    else
    {
        mysql_free_result(mysql_use_result(c));
    }

    mysql_close(c);
    return 0;
}
