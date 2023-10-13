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
        //std::vector<std::string> queries = {"INSERT INTO test.t1 VALUES (1)", "START TRANSACTION READ ONLY"};

        //std::vector<std::string> queries = {"SET @a = 1; SELECT (select engine from information_schema.engines)"};
        std::vector<std::string> queries = {"SELECT id FROM test.t1"};

        for (int i = 0; i < 10; i++)
        {
            for (auto query : queries)
            {
                MYSQL_STMT* stmt = mysql_stmt_init(c);

                std::cout << query << std::endl;

                if (mysql_stmt_prepare(stmt, query.c_str(), query.length()))
                {
                    std::cout << "Prepare: " << mysql_error(c) << std::endl;
                }
                if (mysql_stmt_execute(stmt))
                {
                    std::cout << "Execute: " << mysql_error(c) << std::endl;
                }

                while (mysql_stmt_fetch(stmt) == 0)
                {
                }

                mysql_stmt_close(stmt);
            }
        }
    }

    mysql_close(c);
    return 0;
}
