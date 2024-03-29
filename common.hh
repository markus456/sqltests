#include <mysql.h>
#include <string>
#include <iostream>
#include <vector>
#include <thread>
#include <getopt.h>
#include <cstring>
#include <array>

using namespace std::literals::string_literals;

struct Config
{
    const char *user = "maxuser";
    const char *password = "maxpwd";
    const char *db = "test";
    const char *host = "127.0.0.1";
    int port = 4006;
    int timeout = 30;
    bool debug = false;
    std::string dsn1;
    std::string dsn2;

    std::vector<std::string> args;
};

static inline Config parse(int argc, char **argv)
{
    Config c;
    int opt;
    const char accepted_opts[] = "?u:p:h:D:P:d1:2:t:";

    while ((opt = getopt(argc, argv, accepted_opts)) != -1)
    {
        switch (opt)
        {
        case 'u':
            c.user = optarg;
            break;
        case 'p':
            c.password = optarg;
            break;
        case 'h':
            c.host = optarg;
            break;
        case 'D':
            c.db = optarg;
            break;

        case 'P':
            c.port = atoi(optarg);
            break;

        case 'd':
            c.debug = true;
            break;

        case '1':
            c.dsn1 = optarg;
            break;

        case '2':
            c.dsn2 = optarg;
            break;

        case 't':
            c.timeout = atoi(optarg);
            break;
            
        case '?':
        default:
            std::cout << "Unknown option: " << (char)opt << std::endl;
            std::cout <<
                R"(Options:

-u Username
-p Password
-h Host
-P Port
-D Database
-d Debug mode
)"
                      << std::endl;
            exit(0);
            break;
        }
    }

    for (int i = optind; i < argc; i++)
    {
        c.args.push_back(argv[i]);
    }

    return c;
}
