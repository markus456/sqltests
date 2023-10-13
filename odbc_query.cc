#include "common.hh"
#include <sql.h>
#include <sqlext.h>
#include <sstream>

template <class... Args>
void debug(Args... args)
{
    ((std::cout << args << " "), ...) << std::endl;
}

template <class Hndl>
std::string get_error(int hndl_type, Hndl hndl)
{
    std::ostringstream ss;
    SQLLEN n = 0;
    SQLRETURN ret = SQLGetDiagField(hndl_type, hndl, 0, SQL_DIAG_NUMBER, &n, 0, 0);

    for (int i = 0; i < n; i++)
    {
        SQLCHAR sqlstate[6];
        SQLCHAR msg[SQL_MAX_MESSAGE_LENGTH];
        SQLINTEGER native_error;
        SQLSMALLINT msglen = 0;

        if (SQLGetDiagRec(hndl_type, hndl, i + 1, sqlstate, &native_error,
                          msg, sizeof(msg), &msglen) != SQL_NO_DATA)
        {
            ss << "#" << sqlstate << ": " << native_error << ", " << msg;
        }
    }

    return ss.str();
}

int main(int argc, char** argv)
{
    Config cnf = parse(argc, argv);

    if (cnf.args.empty())
    {
        debug("Missing DSN string");
        return 1;
    }

    std::string dsn = cnf.args[0];
    SQLHENV env {SQL_NULL_HANDLE};
    SQLHDBC conn {SQL_NULL_HANDLE};
    SQLHSTMT stmt {SQL_NULL_HANDLE};

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

    SQLSetConnectAttr(conn, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
    //SQLSetConnectAttr(conn, SQL_ATTR_TXN_ISOLATION, (SQLPOINTER)SQL_TXN_REPEATABLE_READ, 0);

    SQLCHAR outbuf[1024];
    SQLSMALLINT s2len;
    SQLRETURN ret = SQLDriverConnect(conn, nullptr, (SQLCHAR *)dsn.c_str(), dsn.size(),
                                     outbuf, sizeof(outbuf), &s2len, SQL_DRIVER_NOPROMPT);
    if (ret == SQL_ERROR)
    {
        debug("Error", get_error(SQL_HANDLE_DBC, conn));
    }
    else if (ret == SQL_SUCCESS_WITH_INFO)
    {
        debug("SQL_SUCCESS_WITH_INFO:", get_error(SQL_HANDLE_DBC, conn));
    }
    else
    {
        debug("SQLDriverConnect:", ret);
        SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);
    }


    SQLCHAR query[1024];
    strcpy((char*)query, "SELECT 1");

    ret = SQLExecDirect(stmt, query, SQL_NTS);
    if (ret == SQL_SUCCESS)
    {
        debug("OK");
    }
    else if (ret == SQL_SUCCESS_WITH_INFO)
    {
        debug("OK:", get_error(SQL_HANDLE_STMT, stmt));
    }
    else
    {
        debug("Error:", get_error(SQL_HANDLE_STMT, stmt));
    }
    
    
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    SQLDisconnect(conn);
    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    
    return 0;
}
