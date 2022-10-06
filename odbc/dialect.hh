#pragma once
#include <string_view>
#include <memory>
#include <vector>
#include <string>

enum class Source
{
    POSTGRES,
    MARIADB,
};

// The list of MariaDB-specific types that the source tables must be mapped to
enum class ColumnType
{
    UNKNOWN,

    // Integers
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    BIT,

    // Decimal types
    DECIMAL,

    // Floating point types
    DOUBLE,
    FLOAT,

    // Character and binary strings
    CHAR,
    BINARY,
    VARCHAR,
    VARBINARY,

    // Date and time types
    TIME,
    TIMESTAMP,
    DATE,
    DATETIME,

    // LOB types
    TEXT,
    MEDIUMTEXT,
    LONGTEXT,
    BLOB,
    MEDIUMBLOB,
    LONGBLOB,
};

struct ColumnInfo
{
    std::string name;
    int data_type = 0;                     // ODBC data type
    ColumnType type = ColumnType::UNKNOWN; // MariaDB native type
    size_t size = 0;
    size_t buffer_size = 0;
    int digits = 0;
    int nullable = 0;
};

struct TableInfo
{
    std::string catalog;
    std::string schema;
    std::string name;
};

class ODBC;

struct Translator
{
    virtual ~Translator() = default;

    // Prepares a connection for use. Used to initialize the session state of all connections. Called once for each ODBC connection before any other functions are called.
    virtual bool prepare(ODBC &source) = 0;

    // Called when the data dump is first started and before any threads have been created.
    virtual bool start(ODBC &source, const std::vector<TableInfo> &tables) = 0;

    // Called whenever a thread is created for dumping data. The DB given to the function
    // will be the same instance for the whole lifetime of the thread and thus its state
    // does not need to be initialized when the other functions are called.
    virtual bool start_thread(ODBC &source, const TableInfo &table) = 0;

    /**
     * Get the CREATE TABLE SQL for the given table
     *
     * @param source Connection to source database
     * @param table  Target table
     *
     * @return The SQL statement needed to create the table. It needs to use SQL that is fully understood by MariaDB.
     */
    virtual std::vector<std::string> create_table(ODBC &source, const TableInfo &table) = 0;

    // Get SQL for creating indexes on the given table
    virtual std::vector<std::string> create_index(ODBC &source, const TableInfo &table) = 0;

    // Called for the main connection after all threads have been successfully started and the CREATE TABLE and CREATE INDEX statements have been read.
    virtual bool threads_started(ODBC &source, const std::vector<TableInfo> &tables) = 0;

    // Should return the SQL needed to read the data from the source.
    virtual std::string select(ODBC &source, const TableInfo &table) = 0;

    // Called when the dump has finished and all data has been loaded. This step should be used to handle index creation.
    virtual bool stop_thread(ODBC &source, const TableInfo &table) = 0;
};

std::unique_ptr<Translator> create_translator(Source type);
