#add_compile_options(-fsanitize=address)
add_executable(odbc_basic odbc_basic.cc diagnostics.cc odbc.cc dialect.cc)
target_link_libraries(odbc_basic asan  ${MARIADB_CONNECTOR_LIBRARIES} crypto crypt ssl odbc)
