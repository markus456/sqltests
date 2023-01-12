#include "common.hh"

int main(int argc, char** argv)
{
    auto cnf = parse(argc, argv);

    if (cnf.args.empty())
    {
        std::cout << "USAGE: " << argv[0] << " QUERY [CLIENTS]" << std::endl;
        return 1;
    }
    
    std::string query = cnf.args[0];
    int THREADS = cnf.args.empty() ? 20 : atoi(cnf.args[1].c_str());

    std::vector<std::thread> threads;

    for (int t = 0; t < THREADS; t++)
    {
        auto fn = [&, t](){            
            MYSQL* c = mysql_init(nullptr);

            int timeout = cnf.timeout;
            mysql_optionsv(c, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
            mysql_optionsv(c, MYSQL_OPT_READ_TIMEOUT, &timeout);
            mysql_optionsv(c, MYSQL_OPT_WRITE_TIMEOUT, &timeout);
        
            if (!mysql_real_connect(c, cnf.host, cnf.user, cnf.password, cnf.db, cnf.port, nullptr, 0))
            {
                std::cout << "Connect: " << mysql_error(c) << std::endl;
            }
            else
            {
                while (true)
                {
                    if (mysql_query(c, query.c_str()))
                    {
                        std::cout << "Error: " << mysql_error(c) << std::endl;
                        break;
                    }

                    if (auto res = mysql_use_result(c))
                    {
                        mysql_free_result(res);
                    }
                }
            }

            mysql_close(c);
        };
        
        threads.emplace_back(fn);
    }

    for (auto& t : threads)
    {
        t.join();
    }
    
    return 0;
}
