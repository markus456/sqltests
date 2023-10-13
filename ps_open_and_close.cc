#include "common.hh"

void do_execute(MYSQL_STMT* stmt, MYSQL* c)
{   

}

void do_close(MYSQL_STMT* stmt)
{

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
        // std::string query = "SELECT id FROM test.t1 WHERE id IN (?";
        std::string query = "SELECT ?";

        // for (int i = 0; i < 50000; i++)
        // {
        //     query += ",?";
        // }

        // query += ")";

        for (int i = 0; i < 3; i++)
        {
            MYSQL_STMT* stmt = mysql_stmt_init(c);
            mysql_stmt_prepare(stmt, query.c_str(), query.size());
            mysql_stmt_reset(stmt);
            mysql_stmt_close(stmt);
        }
    }

    mysql_close(c);
    return 0;
}
