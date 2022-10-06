#include <mysql.h>
#include <atomic>
#include <string>
#include <iostream>
#include <sstream>
#include <thread>
#include <chrono>

const char* user = "maxuser";
const char* password = "maxpwd";
const char* db = "test";
const char* host = "127.0.0.1";
int port = 4006;

using Clock = std::chrono::steady_clock;
using namespace std::literals::chrono_literals;
using namespace std::chrono;

int main(int argc, char** argv)
{
    constexpr int LIMIT = 100;

    for (int i = 0; i < LIMIT; i++)
    {
        MYSQL* c = mysql_init(nullptr);
        MYSQL* c2 = mysql_init(nullptr);
        
        int timeout = 10;
        mysql_optionsv(c, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
        mysql_optionsv(c, MYSQL_OPT_READ_TIMEOUT, &timeout);
        mysql_optionsv(c, MYSQL_OPT_WRITE_TIMEOUT, &timeout);
        mysql_optionsv(c2, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
        mysql_optionsv(c2, MYSQL_OPT_READ_TIMEOUT, &timeout);
        mysql_optionsv(c2, MYSQL_OPT_WRITE_TIMEOUT, &timeout);
        
        if (!mysql_real_connect(c, host, user, password, db, port, nullptr, 0)
            || !mysql_real_connect(c2, host, user, password, db, port, nullptr, 0))
        {
            std::cout << "Connect: " << mysql_error(c) <<  mysql_error(c2) << std::endl;
        }
        else
        {
            
            auto id = mysql_thread_id(c);
            std::atomic<bool> running {true};
            
            std::thread thr([&](){
                const char* q1 = R"(
BEGIN NOT ATOMIC 
  DECLARE v1 INT DEFAULT 5; 
  CREATE OR REPLACE TABLE test.t1(id INT) ENGINE=MEMORY; 
  SET @a = NOW();
  WHILE TIME_TO_SEC(TIMEDIFF(NOW(), @a)) < 30 DO 
    INSERT INTO test.t1 VALUES (1); 
    SET v1 = (SELECT COUNT(*) FROM test.t1); 
    DELETE FROM test.t1;
  END WHILE;
END
)";
                while (running)
                {
                    auto start = Clock::now();
                    if (mysql_query(c, q1))
                    {
                        std::string err = mysql_error(c);
                        
                        if (err.find("Query execution was interrupted") == std::string::npos)
                        {
                            std::cout << "1 Error: " << err << std::endl;
                            running = false;
                        }
                    }
                    else
                    {
                        auto end = Clock::now();
                        auto dur = duration_cast<seconds>(end - start);
                        std::cout << "Query did not fail but took " << dur.count() << " seconds" << std::endl;
                        running = false;
                    }
                }
            });


            auto start = Clock::now();
            std::ostringstream ss;
            ss << "KILL QUERY " << id;
            std::string q2 = ss.str();

            while(running)
            {
                if (mysql_query(c2, q2.c_str()))
                {
                    std::cout << "2 Error: " << mysql_error(c) << std::endl;
                    running = false;
                }
            }

            running = false;
            thr.join();
        }

        mysql_close(c);
    }
    
    return 0;
}
