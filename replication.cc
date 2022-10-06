#include <mysql.h>
#include <mariadb_rpl.h>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>

const char* user = "maxuser";
const char* password = "maxpwd";
const char* db = "test";
const char* host = "127.0.0.1";
int port = 3000;

int main(int argc, char** argv)
{
    MYSQL* c = mysql_init(nullptr);

    if (!mysql_real_connect(c, host, user, password, db, port, nullptr, 0))
    {
        std::cout << "Connect: " << mysql_error(c) << std::endl;
    }
    else
    {
        std::string gtid_str = argc >= 2 ? argv[1] : "";

        std::vector<std::string> queries =
            {
                "SET @master_heartbeat_period=1000000000",
                "SET @master_binlog_checksum = @@global.binlog_checksum",
                "SET @mariadb_slave_capability=4",
                "SET @slave_connect_state='" + gtid_str + "'",
                "SET @slave_gtid_strict_mode=1",
                "SET @slave_gtid_ignore_duplicates=1",
                "SET NAMES latin1"
            };

        for (const auto& sql : queries)
        {
            if (mysql_query(c, sql.c_str()))
            {
                std::cout << "Query error: " << mysql_error(c) << std::endl;
            }
        }
    
    
        if (auto rpl = mariadb_rpl_init(c))
        {
            unsigned int server_id = 1234;
            mariadb_rpl_optionsv(rpl, MARIADB_RPL_SERVER_ID, server_id);
            mariadb_rpl_optionsv(rpl, MARIADB_RPL_START, 4);
            mariadb_rpl_optionsv(rpl, MARIADB_RPL_FLAGS, MARIADB_RPL_BINLOG_SEND_ANNOTATE_ROWS);

            if (mariadb_rpl_open(rpl))
            {
                std::cout << "mariadb_rpl_open: " << mysql_error(c) << std::endl;
            }
            else
            {
                while (auto ev = mariadb_rpl_fetch(rpl, nullptr))
                {
                    bool print = false;
                    auto type = ev->event_type;
                    std::ostringstream ss;

                    ss
                        << "event          " << type << "\n"
                        << "timestamp      " << ev->timestamp << "\n"
                        << "server_id      " << ev->server_id << "\n"
                        << "event_len      " << ev->event_length << "\n"
                        << "next_pos       " << ev->next_event_pos << "\n"
                        << "flags          " << ev->flags << "\n";
                                                
                    if (type == FORMAT_DESCRIPTION_EVENT)
                    {
                        print = true;
                        
                        ss
                            << "format         " << ev->event.format_description.format << "\n"
                            << "server_version " << ev->event.format_description.server_version << "\n"
                            << "timestamp      " << ev->event.format_description.timestamp << "\n"
                            << "header_len     " << ev->event.format_description.header_len << "\n";
                    }
                    else if (type == ROTATE_EVENT)
                    {
                        print = true;
                        std::string binlog(ev->event.rotate.filename.str,
                                           ev->event.rotate.filename.str + (ev->event_length - 19 - 8));
                        ss
                            << "position       " << ev->event.rotate.position << "\n"
                            << "binlog         " << binlog << "\n"
                            << "name len       " << ev->event.rotate.filename.length << "\n";
                    }
                    
                    ss << "-------------------------------------\n";

                    if (print)
                    {
                        std::cout << ss.str() << std::endl;
                    }
                    
                    mariadb_free_rpl_event(ev);
                }
            }

            mariadb_rpl_close(rpl);
        }
        else
        {
            std::cout << "mariadb_rpl_init: " << mysql_error(c) << std::endl;
        }
    }
    mysql_close(c);
    return 0;
}
