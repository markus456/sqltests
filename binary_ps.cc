#include <mysql.h>
#include <string>
#include <iostream>

const char* user = "maxuser";
const char* password = "maxpwd";
const char* db = "test";
const char* host = "127.0.0.1";
int port = 4006;

int main(int argc, char** argv)
{
    MYSQL* c = mysql_init(nullptr);

    if (!mysql_real_connect(c, host, user, password, db, port, nullptr, 0))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {
        if (mysql_query(c, "CREATE OR REPLACE PROCEDURE sp() SELECT 1"))
        {
            std::cout << "CREATE: " << mysql_error(c) << std::endl;
        }

        MYSQL_STMT* stmt = mysql_stmt_init(c);
        std::string query = "CALL sp()";

        if (mysql_stmt_prepare(stmt, query.c_str(), query.length()))
        {
               std::cout << "Prepare: " << mysql_error(c) << std::endl;
        }

        unsigned long cursor_type = CURSOR_TYPE_READ_ONLY;
        unsigned long rows = 1;

        if (mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, &cursor_type))
        {
            std::cout << "Attr set: " << mysql_error(c) << std::endl;
        }

        if (mysql_stmt_execute(stmt))
        {
            std::cout << "Prepare: " << mysql_error(c) << std::endl;
        }

        mysql_stmt_close(stmt);

        if (mysql_query(c, "DROP PROCEDURE sp"))
        {
            std::cout << "DROP: " << mysql_error(c) << std::endl;
        }
    }

    mysql_close(c);
    return 0;
}
