cmake_minimum_required(VERSION 3.16)
set (CMAKE_CXX_STANDARD 17)

set(CMAKE_INSTALL_PREFIX ${CMAKE_BINARY_DIR} CACHE PATH "Prefix prepended to install directories.")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")

project(SqlTest)

include(ExternalProject)

add_subdirectory(odbc)

include(cmake/BuildMariaDBConnector.cmake)
include_directories(BEFORE ${MARIADB_CONNECTOR_INCLUDE_DIR})

function(add_program name source)
  add_executable(${name} ${source})
  target_link_libraries(${name} ${MARIADB_CONNECTOR_LIBRARIES} crypto crypt ssl)
endfunction()


add_program(binary_ps binary_ps.cc)
add_program(create_connections create_connections.cc)
add_program(send_query send_query.cc)
add_program(send_query_from_file send_query_from_file.cc)
add_program(slow_read slow_read.cc)
add_program(simple simple.cc)
add_program(change_user change_user.cc)
add_program(kill_own_connection kill_own_connection.cc)
add_program(kill_query kill_query.cc)
add_program(long_ps long_ps.cc)
add_program(many_ps many_ps.cc)
add_program(ps ps.cc)
add_program(session_track session_track.cc)
add_program(replication replication.cc)
add_program(lots_of_sescmd lots_of_sescmd.cc)
add_program(read_from_stdin read_from_stdin.cc)
add_program(trx trx.cc)
add_program(ps_read_from_stdin ps_read_from_stdin.cc)
add_program(lag_between_cmds lag_between_cmds.cc)
add_program(send_double_commit send_double_commit.cc)
add_program(parallel_query_loop parallel_query_loop.cc)
