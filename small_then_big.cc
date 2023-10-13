#include "common.hh"

std::string make_query(std::string_view value, int num)
{
    std::string_view prefix = "SELECT ";
    std::string_view delimiter = ", ";

    std::string query;
    query += prefix;
    
    for (int i = 0; i < num; i++)
    {
        if (i != 0)
        {
            query += delimiter;
        }

        query += value;
    }

    return query;
}

int main(int argc, char** argv)
{
    
    Config cnf = parse(argc, argv);
    MYSQL* c = mysql_init(nullptr);

    if (!mysql_real_connect(c, cnf.host, cnf.user, cnf.password, cnf.db, cnf.port, nullptr, CLIENT_MULTI_STATEMENTS|CLIENT_MULTI_RESULTS))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {
        const unsigned int START = 10;
        const unsigned int END = 4000000;
        unsigned int sz = START;

        if (mysql_query(c, "SET autocommit=0"))
        {
            std::cout << "SET autocommit=0: " << mysql_error(c) << std::endl;
        }

        while (true)
        {
            auto query = make_query("1", sz);
            sz += END / 5 + 1;
            sz %= END;

            std::cout << " Query size: " << query.size() << std::endl;
            
            if (mysql_real_query(c, query.c_str(), query.size()))
            {
                std::cout << "mysql_query: " << mysql_error(c) << std::endl;
                break;
            }
            else
            {
                mysql_free_result(mysql_use_result(c));
            }

            //sleep(1);
        }
    }

    return 0;
}
