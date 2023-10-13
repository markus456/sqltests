#include "common.hh"
#include <vector>

int main(int argc, char** argv)
{
    Config cnf = parse(argc, argv);
    MYSQL* c = mysql_init(nullptr);
    std::vector<MYSQL_STMT*> stmts;
    
    if (!mysql_real_connect(c, cnf.host, cnf.user, cnf.password, cnf.db, cnf.port, nullptr, CLIENT_MULTI_STATEMENTS|CLIENT_MULTI_RESULTS))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {
        int i = 0;
        auto prev = std::chrono::steady_clock::now();
        
        while (true)
        {
            std::string query = "SELECT @@version, @@version_comment";
            MYSQL_STMT* stmt = mysql_stmt_init(c);
            stmts.push_back(stmt);

            auto start = std::chrono::steady_clock::now();
            
            if (mysql_stmt_prepare(stmt, query.c_str(), query.length()))
            {
                std::cout << "Prepare: " << mysql_error(c) << std::endl;
                break;
            }

            auto end = std::chrono::steady_clock::now();

            if (i % 1000 == 0)
            {
                auto d = std::chrono::duration_cast<std::chrono::duration<float>>(end - start);
                std::cout << query << ": " << d.count() << std::endl;
            }

            if (mysql_stmt_execute(stmt))
            {
                std::cout << "Execute: " << mysql_error(c) << std::endl;
                break;
            }

            while (mysql_stmt_fetch(stmt) == 0)
            {
            }
        }

        for (auto stmt : stmts)
        {
            mysql_stmt_close(stmt);
        }
    }

    mysql_close(c);
    return 0;
}
