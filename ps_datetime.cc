#include "common.hh"

const char* user = "maxuser";
const char* password = "maxpwd";
const char* db = "test";
const char* host = "127.0.0.1";
int port = 3000;

int main(int argc, char** argv)
{
    Config cnf = parse(argc, argv);
    MYSQL* c = mysql_init(nullptr);

    if (!mysql_real_connect(c, cnf.host, cnf.user, cnf.password, cnf.db, cnf.port, nullptr, 0))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {
            MYSQL_STMT* stmt = mysql_stmt_init(c);
            std::string query = "SELECT a FROM test.t1";

            if (mysql_stmt_prepare(stmt, query.c_str(), query.length()))
            {
                std::cout << "Prepare: " << mysql_error(c) << std::endl;
                mysql_stmt_close(stmt);
            }

            std::vector<MYSQL_BIND> binds;
            std::vector<MYSQL_TIME> buffers;
            std::vector<unsigned long> lengths;
            std::vector<char> errors;
            std::vector<char> nulls;

            if (mysql_stmt_execute(stmt))
            {
                std::cout << "Execute: " << mysql_error(c) << std::endl;
                mysql_stmt_close(stmt);
            }
            

            binds.resize(mysql_stmt_field_count(stmt));
            buffers.resize(binds.size());
            lengths.resize(binds.size());
            errors.resize(binds.size());
            nulls.resize(binds.size());

            for (size_t i = 0; i < binds.size(); i++)
            {
                binds[i].buffer = &buffers[i];
                binds[i].buffer_length = sizeof(MYSQL_TIME);
                binds[i].length = &lengths[i];
                binds[i].error = &errors[i];
                binds[i].is_null = &nulls[i];
            }

            std::cout << "Result:" << std::endl;
            
            if (mysql_stmt_bind_result(stmt, binds.data()) == 0)
            {
                while (mysql_stmt_fetch(stmt) == 0)
                {
                    for (size_t i = 0; i < binds.size(); i++)
                    {
                        if (nulls[i])
                        {
                            std::cout << "NULL" << std::endl;
                        }
                        else if (errors[i])
                        {
                            std::cout << "ERROR" << std::endl;
                        }
                        else
                        {
                            std::cout << buffers[i].year << "-" << buffers[i].month << "-" << buffers[i].day
                                      << " " << buffers[i].hour << ":" << buffers[i].minute << ":" << buffers[i].second
                                      << "." << buffers[i].second_part << std::endl;
                        }
                    }
                }
            }

            mysql_stmt_close(stmt);
    }

    mysql_close(c);
    return 0;
}
