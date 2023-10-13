#include "common.hh"
#include <array>

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
        std::array<MYSQL_STMT*, 10> stmt{};
        std::array<int, 10> rows{};
 
        for (int i = 0; i < 10; i++)
        {
            std::string query = "SELECT " + std::to_string(i) + " FROM seq_0_to_10";
            stmt[i] = mysql_stmt_init(c);
            
            if (mysql_stmt_prepare(stmt[i], query.c_str(), query.length()))
            {
                std::cout << "Prepare: " << mysql_error(c) << std::endl;
            }
        }
        
        for (int i = 0; i < 10; i++)
        {
            unsigned long cursor_type = CURSOR_TYPE_READ_ONLY;
            unsigned long rows = i;
            mysql_stmt_attr_set(stmt[i], STMT_ATTR_CURSOR_TYPE, &cursor_type);
            mysql_stmt_attr_set(stmt[i], STMT_ATTR_PREFETCH_ROWS, &rows);

            if (mysql_stmt_execute(stmt[i]))
            {
                std::cout << "Execute: " << mysql_error(c) << std::endl;
            }
        }

        bool more = true;

        while (more)
        {
            for (int i = 0; i < 10; i++)
            {
                more = false;
                if (mysql_stmt_fetch(stmt[i]) == 0)
                {
                    more = true;
                    std::cout << "Row " << rows[i]++ << " for " << i << std::endl;
                }
            }
        }

        for (int i = 0; i < 10; i++)
        {
            mysql_stmt_close(stmt[i]);
        }
    }

    mysql_close(c);
    return 0;
}
