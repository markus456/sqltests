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

const char* event_type(int event)
{
    switch(event)
    {
    case UNKNOWN_EVENT: return "UNKNOWN_EVENT";
    case START_EVENT_V3: return "START_EVENT_V3";
    case QUERY_EVENT: return "QUERY_EVENT";
    case STOP_EVENT: return "STOP_EVENT";
    case ROTATE_EVENT: return "ROTATE_EVENT";
    case INTVAR_EVENT: return "INTVAR_EVENT";
    case LOAD_EVENT: return "LOAD_EVENT";
    case SLAVE_EVENT: return "SLAVE_EVENT";
    case CREATE_FILE_EVENT: return "CREATE_FILE_EVENT";
    case APPEND_BLOCK_EVENT: return "APPEND_BLOCK_EVENT";
    case EXEC_LOAD_EVENT: return "EXEC_LOAD_EVENT";
    case DELETE_FILE_EVENT: return "DELETE_FILE_EVENT";
    case NEW_LOAD_EVENT: return "NEW_LOAD_EVENT";
    case RAND_EVENT: return "RAND_EVENT";
    case USER_VAR_EVENT: return "USER_VAR_EVENT";
    case FORMAT_DESCRIPTION_EVENT: return "FORMAT_DESCRIPTION_EVENT";
    case XID_EVENT: return "XID_EVENT";
    case BEGIN_LOAD_QUERY_EVENT: return "BEGIN_LOAD_QUERY_EVENT";
    case EXECUTE_LOAD_QUERY_EVENT: return "EXECUTE_LOAD_QUERY_EVENT";
    case TABLE_MAP_EVENT : return "TABLE_MAP_EVENT ";
    case PRE_GA_WRITE_ROWS_EVENT : return "PRE_GA_WRITE_ROWS_EVENT ";
    case PRE_GA_UPDATE_ROWS_EVENT : return "PRE_GA_UPDATE_ROWS_EVENT ";
    case PRE_GA_DELETE_ROWS_EVENT : return "PRE_GA_DELETE_ROWS_EVENT ";
    case WRITE_ROWS_EVENT_V1 : return "WRITE_ROWS_EVENT_V1 ";
    case UPDATE_ROWS_EVENT_V1 : return "UPDATE_ROWS_EVENT_V1 ";
    case DELETE_ROWS_EVENT_V1 : return "DELETE_ROWS_EVENT_V1 ";
    case INCIDENT_EVENT: return "INCIDENT_EVENT";
    case HEARTBEAT_LOG_EVENT: return "HEARTBEAT_LOG_EVENT";
    case IGNORABLE_LOG_EVENT: return "IGNORABLE_LOG_EVENT";
    case ROWS_QUERY_LOG_EVENT: return "ROWS_QUERY_LOG_EVENT";
    case WRITE_ROWS_EVENT : return "WRITE_ROWS_EVENT ";
    case UPDATE_ROWS_EVENT : return "UPDATE_ROWS_EVENT ";
    case DELETE_ROWS_EVENT : return "DELETE_ROWS_EVENT ";
    case GTID_LOG_EVENT: return "GTID_LOG_EVENT";
    case ANONYMOUS_GTID_LOG_EVENT: return "ANONYMOUS_GTID_LOG_EVENT";
    case PREVIOUS_GTIDS_LOG_EVENT: return "PREVIOUS_GTIDS_LOG_EVENT";
    case TRANSACTION_CONTEXT_EVENT: return "TRANSACTION_CONTEXT_EVENT";
    case VIEW_CHANGE_EVENT: return "VIEW_CHANGE_EVENT";
    case XA_PREPARE_LOG_EVENT: return "XA_PREPARE_LOG_EVENT";
    case PARTIAL_UPDATE_ROWS_EVENT : return "PARTIAL_UPDATE_ROWS_EVENT ";
    case ANNOTATE_ROWS_EVENT: return "ANNOTATE_ROWS_EVENT";
    case BINLOG_CHECKPOINT_EVENT: return "BINLOG_CHECKPOINT_EVENT";
    case GTID_EVENT: return "GTID_EVENT";
    case GTID_LIST_EVENT: return "GTID_LIST_EVENT";
    case START_ENCRYPTION_EVENT: return "START_ENCRYPTION_EVENT";
    case QUERY_COMPRESSED_EVENT : return "QUERY_COMPRESSED_EVENT ";
    case WRITE_ROWS_COMPRESSED_EVENT_V1 : return "WRITE_ROWS_COMPRESSED_EVENT_V1 ";
    case UPDATE_ROWS_COMPRESSED_EVENT_V1 : return "UPDATE_ROWS_COMPRESSED_EVENT_V1 ";
    case DELETE_ROWS_COMPRESSED_EVENT_V1 : return "DELETE_ROWS_COMPRESSED_EVENT_V1 ";
    case WRITE_ROWS_COMPRESSED_EVENT : return "WRITE_ROWS_COMPRESSED_EVENT ";
    case UPDATE_ROWS_COMPRESSED_EVENT : return "UPDATE_ROWS_COMPRESSED_EVENT ";
    case DELETE_ROWS_COMPRESSED_EVENT : return "DELETE_ROWS_COMPRESSED_EVENT ";
    }

    return "Undefined event";
}

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
                //"SET @rpl_semi_sync_slave=1",
                "SET NAMES latin1",
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
                        << "event          " << type << " " << event_type(type) << "\n"
                        << "timestamp      " << ev->timestamp << std::hex << " (0x" << ev->timestamp << ")" << std::dec << "\n"
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

                    if (print || true)
                    {
                        std::cout << ss.str() << std::endl;
                    }
                    
                    mariadb_free_rpl_event(ev);
                }

                if (const char* err = mysql_error(c))
                {
                    std::cout << "Error: " << err << std::endl;
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
