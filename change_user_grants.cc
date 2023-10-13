#include "common.hh"

void query(MYSQL* c, std::string_view query)
{
    if (mysql_real_query(c, query.data(), query.size()))
    {
        std::cout << "Query failed: " << mysql_error(c) << std::endl;
    }
    else
    {
        mysql_free_result(mysql_use_result(c));
        std::cout << "OK: " << query << std::endl;
    }
}

void change_user(MYSQL* c, const char* user, const char* pw, const char* db)
{
    if (mysql_change_user(c, user, pw, db))
    {
        std::cout << "Change user: " << mysql_error(c) << std::endl;
    }
    else
    {
        std::cout << "OK: CHANGE USER" << std::endl;
    }
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
        query(c, "CREATE DATABASE IF NOT EXISTS "s + cnf.db);
        query(c, "CREATE OR REPLACE TABLE t1(id INT)");
        query(c, "DROP USER IF EXISTS 'user'@'%'");
        query(c, "CREATE USER 'user'@'%' IDENTIFIED BY 'pass'");
        query(c, "GRANT SELECT ON test.t1 TO 'user'@'%'");
        query(c, "INSERT INTO test.t1 VALUES (1)");
        query(c, "SELECT * FROM test.t1");
        change_user(c, "user", "pass", cnf.db);
        query(c, "INSERT INTO test.t1 VALUES (1)");
        query(c, "SELECT * FROM test.t1");
        change_user(c, "user", "wrong_pw", cnf.db);
        query(c, "INSERT INTO test.t1 VALUES (1)");
        query(c, "SELECT * FROM test.t1");
        change_user(c, cnf.user, cnf.password, cnf.db);
        query(c, "INSERT INTO test.t1 VALUES (1)");
        query(c, "SELECT * FROM test.t1");
    }

    mysql_close(c);
    return 0;
}
