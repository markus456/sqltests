#include <mysql.h>
#include <string>
#include <iostream>

const char* user = "maxuser";
const char* password = "maxpwd";
const char* db = "test";
const char* host = "127.0.0.1";
int port = 4006;

std::string create_query(int bytes)
{
    const std::string prefix = "SELECT '";
    const std::string suffix = "'";
    std::string query;
    query.reserve(bytes);
    query += prefix;
    query.insert(query.end(), bytes - prefix.size() - suffix.size(), 'a');
    query += suffix;
    return query;
}

int main(int argc, char** argv)
{
    MYSQL* c = mysql_init(nullptr);

    int bytes = 0xffffff;

    if (argc > 1)
    {
        bytes = strtol(argv[1], nullptr, 0);
    }

    bytes = std::max(bytes - 1, 0);
    
    int timeout = 20;
    mysql_optionsv(c, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    mysql_optionsv(c, MYSQL_OPT_READ_TIMEOUT, &timeout);
    mysql_optionsv(c, MYSQL_OPT_WRITE_TIMEOUT, &timeout);

    if (!mysql_real_connect(c, host, user, password, db, port, nullptr, 0))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {

        MYSQL_STMT* stmt = mysql_stmt_init(c);
        std::string query = create_query(bytes);

        if (mysql_stmt_prepare(stmt, query.c_str(), query.length()))
        {
               std::cout << "Prepare: " << mysql_error(c) << std::endl;
        }

        if (mysql_stmt_execute(stmt))
        {
            std::cout << "Execute: " << mysql_error(c) << std::endl;
        }

        mysql_stmt_close(stmt);
    }

    mysql_close(c);
    return 0;
}
