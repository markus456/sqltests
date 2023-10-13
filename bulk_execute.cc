#include "common.hh"

static int show_mysql_error(MYSQL* mysql)
{
    printf("Error(%d) [%s] \"%s\"\n",
           mysql_errno(mysql),
           mysql_sqlstate(mysql),
           mysql_error(mysql));
    return 1;
}

static int show_stmt_error(MYSQL_STMT* stmt)
{
    printf("Error(%d) [%s] \"%s\"\n",
           mysql_stmt_errno(stmt),
           mysql_stmt_sqlstate(stmt),
           mysql_stmt_error(stmt));
    return 1;
}

int main(int argc, char** argv)
{
    Config cnf = parse(argc, argv);
    MYSQL* mysql = mysql_init(nullptr);
    
    if (!mysql_real_connect(mysql, cnf.host, cnf.user, cnf.password, cnf.db, cnf.port, nullptr, CLIENT_MULTI_STATEMENTS|CLIENT_MULTI_RESULTS))
    {
        std::cout << "Connect: " << mysql_error(mysql) << std::endl;
    }
    else
    {

        MYSQL_STMT* stmt;
        MYSQL_BIND bind[1];

        struct st_data
        {
            char          name[30];
            char          name_ind;
        };

        struct st_data data[] = {
            {"a", STMT_INDICATOR_NTS},
            {"b", STMT_INDICATOR_NTS},
            {"c", STMT_INDICATOR_NTS},
            {"a", STMT_INDICATOR_NTS},
            {"d", STMT_INDICATOR_NTS},
            {"e", STMT_INDICATOR_NTS }
        };

        unsigned int array_size = 5;
        size_t row_size = sizeof(struct st_data);

        if (mysql_query(mysql, "CREATE OR REPLACE TABLE t1 (id CHAR(30) NOT NULL PRIMARY KEY)"))
        {
            show_mysql_error(mysql);
        }

        stmt = mysql_stmt_init(mysql);

        if (mysql_stmt_prepare(stmt, "INSERT INTO t1 VALUES (?)", -1))
        {
            show_stmt_error(stmt);
        }

        memset(bind, 0, sizeof(bind));

        bind[0].buffer = &data[0].name;
        bind[0].buffer_type = MYSQL_TYPE_STRING;
        bind[0].u.indicator = &data[0].name_ind;

        /* set array size */
        mysql_stmt_attr_set(stmt, STMT_ATTR_ARRAY_SIZE, &array_size);

        /* set row size */
        mysql_stmt_attr_set(stmt, STMT_ATTR_ROW_SIZE, &row_size);

        /* bind parameter */
        mysql_stmt_bind_param(stmt, bind);

        /* execute */
        if (mysql_stmt_execute(stmt))
        {
            show_stmt_error(stmt);
        }

        mysql_stmt_close(stmt);
    }

    mysql_close(mysql);
}
