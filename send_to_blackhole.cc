#include "common.hh"

int main(int argc, char** argv)
{
    auto cnf = parse(argc, argv);

    int NUM_QUERIES = 100000;

    if (!cnf.args.empty())
    {
        NUM_QUERIES = std::stoi(cnf.args[0]);
    }
    
    MYSQL* c = mysql_init(nullptr);

    if (!mysql_real_connect(c, cnf.host, cnf.user, cnf.password, cnf.db, cnf.port, nullptr, 0))
    {
        std::cout << "Failed to connect: " << mysql_error(c) << std::endl;
        mysql_close(c);
    }
    else if (mysql_query(c, "CREATE OR REPLACE TABLE test.blackhole(id INT) ENGINE=BLACKHOLE"))
    {
        std::cout << "mysql_query: " << mysql_error(c) << std::endl;
    }
    else
    {
        std::string query = "INSERT INTO test.blackhole VALUES (1)";

        for (int i = 0; i < NUM_QUERIES; i++)
        {
            if (mysql_send_query(c, query.c_str(), query.length()))
            {
                std::cout << "mysql_send_query: " << mysql_error(c) << std::endl;
            }
            else
            {
                std::cout << "Send: " << i << "\n";
            }
        }

        for (int i = 0; i < NUM_QUERIES; i++)
        {     
            if (mysql_read_query_result(c))
            {
                std::cout << "mysql_read_query_result: " << mysql_error(c) << std::endl;
            }
            else
            {
                std::cout << "Read: " << i << "\n";
            }
        }
    }
    
    mysql_close(c);
    return 0;
}
