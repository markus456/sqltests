#include <mysql.h>
#include <string>
#include <iostream>
#include <chrono>

const char* user = "maxuser";
const char* password = "maxpwd";
const char* db = "test";
const char* host = "127.0.0.1";
int port = 3000;
int LIMIT = 250'000;

using float_s = std::chrono::duration<double, std::ratio<1, 1>>;
using float_ms = std::chrono::duration<double, std::ratio<1, 1000>>;
using Clock = std::chrono::steady_clock;

int main(int argc, char** argv)
{
    MYSQL* c = mysql_init(nullptr);

    if (!mysql_real_connect(c, host, user, password, db, port, nullptr, 0))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {
        std::chrono::nanoseconds total(0);
        
        for (int i = 0; i < LIMIT; i++)
        {
            auto start = Clock::now();
            mysql_change_user(c, user, password, db);
            auto end = Clock::now();
            total += end - start;
        }


        auto total_s = std::chrono::duration_cast<float_s>(total).count();
        auto avg_s = std::chrono::duration_cast<float_ms>(total).count() / LIMIT;
        std::cout << LIMIT << " COM_CHANGE_USERs took " << total_s  << " seconds (avg: " << std::fixed << avg_s << ")" << std::endl;

        total = std::chrono::nanoseconds(0);
        
        for (int i = 0; i < LIMIT; i++)
        {
            auto start = Clock::now();
            mysql_reset_connection(c);
            auto end = Clock::now();
            total += end - start;
        }

        
        total_s = std::chrono::duration_cast<float_s>(total).count();
        avg_s = std::chrono::duration_cast<float_ms>(total).count() / LIMIT;
        std::cout << LIMIT << " COM_RESET_CONNECTIONs took " << total_s  << " seconds (avg: " << std::fixed << avg_s << ")" << std::endl;
    }

    mysql_close(c);
    return 0;
}
