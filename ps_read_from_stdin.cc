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
        std::cout << "Enter query:" << std::endl;

        for (std::string query; std::getline(std::cin, query);)
        {
            MYSQL_STMT* stmt = mysql_stmt_init(c);

            if (mysql_stmt_prepare(stmt, query.c_str(), query.length()))
            {
                std::cout << "Prepare: " << mysql_error(c) << std::endl;
                mysql_stmt_close(stmt);
                continue;
            }

            std::vector<MYSQL_BIND> binds;
            std::vector<std::array<char, 256>> buffers;
            std::vector<unsigned long> lengths;
            std::vector<char> errors;
            std::vector<char> nulls;

            binds.resize(mysql_stmt_param_count(stmt));
            buffers.resize(binds.size());
            lengths.resize(binds.size());
            errors.resize(binds.size());
            nulls.resize(binds.size());
            
            for (size_t i = 0; i < binds.size(); i++)
            {
                std::string value;
                std::cout << "Enter value:" << std::endl;
                std::getline(std::cin, value);
                memcpy(buffers[i].data(), value.c_str(), value.size());
                lengths[i] = value.size();

                binds[i].buffer_type = MYSQL_TYPE_STRING;
                binds[i].buffer = buffers[i].data();
                binds[i].buffer_length = buffers[i].size();
                binds[i].length = &lengths[i];
                binds[i].error = &errors[i];
                binds[i].is_null = &nulls[i];
            }

            mysql_stmt_bind_param(stmt, binds.data());
            
            if (mysql_stmt_execute(stmt))
            {
                std::cout << "Execute: " << mysql_error(c) << std::endl;
                mysql_stmt_close(stmt);
                continue;
            }

            binds.resize(mysql_stmt_field_count(stmt));
            buffers.resize(binds.size());
            lengths.resize(binds.size());
            errors.resize(binds.size());
            nulls.resize(binds.size());

            for (size_t i = 0; i < binds.size(); i++)
            {
                binds[i].buffer = buffers[i].data();
                binds[i].buffer_length = buffers[i].size();
                binds[i].length = &lengths[i];
                binds[i].error = &errors[i];
                binds[i].is_null = &nulls[i];
            }

            std::cout << "Result:" << std::endl;
            
            if (mysql_stmt_bind_result(stmt, binds.data()) == 0)
            {
                while (mysql_stmt_fetch(stmt) == 0)
                {
                    for (const auto& b : buffers)
                    {
                        std::cout << b.data() << "\t";
                    }
                    
                    std::cout << std::endl;
                }
            }

            mysql_stmt_close(stmt);
            
            std::cout << "Enter query:" << std::endl;
        }
    }

    mysql_close(c);
    return 0;
}
