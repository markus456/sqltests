#include "common.hh"

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
        {
            sleep(1);
            MYSQL_STMT* stmt = mysql_stmt_init(c);
            mariadb_stmt_execute_direct(stmt, "SELECT * FROM some.thing", -1);
            mysql_stmt_close(stmt);
        }

        for (int i = 0; i < 1; i++)
        {
            //mysql_query(c, "SELECT 1");
            //mysql_free_result(mysql_use_result(c));

            MYSQL_STMT* stmt = mysql_stmt_init(c);
            //std::string query = "SELECT * FROM mysql.user WHERE CURRENT_USER() LIKE user";
            std::string query = "SELECT 1";

            mariadb_stmt_execute_direct(stmt, query.c_str(), query.size());;

            char buffer[100] = "";
            my_bool err = false;
            my_bool isnull = false;
            MYSQL_BIND bind = {};

            bind.buffer_length = sizeof(buffer);
            bind.buffer = buffer;
            bind.error = &err;
            bind.is_null = &isnull;

            mysql_stmt_bind_result(stmt, &bind);
            mysql_stmt_fetch(stmt);
            mysql_stmt_close(stmt);
            
            //mysql_query(c, "SELECT 2");
            //mysql_free_result(mysql_use_result(c));

        }
    }

    mysql_close(c);
    return 0;
}
