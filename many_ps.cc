#include "common.hh"

void do_prepare(std::vector<MYSQL_STMT*>& stmts, MYSQL* c)
{
    MYSQL_STMT* stmt = mysql_stmt_init(c);
    std::string query = "DO 1";

    if (mysql_stmt_prepare(stmt, query.c_str(), query.length()))
    {
        std::cout << "Prepare: " << mysql_error(c) << std::endl;
    }
    else
    {
        stmts.push_back(stmt);
    }
}

void do_execute(MYSQL_STMT* stmt, MYSQL* c)
{   
    if (mysql_stmt_execute(stmt))
    {
        std::cout << "Prepare: " << mysql_error(c) << std::endl;
    }
}

void do_close(MYSQL_STMT* stmt)
{
    mysql_stmt_close(stmt);
}

int main(int argc, char** argv)
{
    Config cnf = parse(argc, argv);
    MYSQL* c = mysql_init(nullptr);

    constexpr int LIMIT = 10000;
    
    if (!mysql_real_connect(c, cnf.host, cnf.user, cnf.password, cnf.db, cnf.port, nullptr, 0))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {
        std::vector<MYSQL_STMT*> stmts;

        for (int i = 0; i < LIMIT; i++)
        {
            do_prepare(stmts, c);
        }

        for (auto stmt : stmts)
        {
            do_execute(stmt, c);
        }

        for (auto stmt : stmts)
        {
            do_close(stmt);
        }

        stmts.clear();

        for (int i = 0; i < LIMIT; i++)
        {
            do_prepare(stmts, c);
            do_execute(stmts[0], c);
            do_close(stmts[0]);
            stmts.clear();
        }
    }

    mysql_close(c);
    return 0;
}
