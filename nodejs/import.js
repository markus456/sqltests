const axios = require("axios")
const json_diff = require("json-diff")
const opts = {auth: {username: "admin", password: "mariadb"}}

async function exec_query(conn, token, sql){
  console.log(conn + "queries" + token)
  var res = await axios.post(conn + "queries" + token, {sql: sql}, opts)
  return res.data
}

async function print_query(conn, token, sql){
  var data = await exec_query(conn, token, sql)
  console.log(JSON.stringify(data, null, 2))
}

async function test_query(conn, token){
  await print_query(conn, token, process.argv[3])
}

async function compare_results(conn, token, second_conn, second_token, tables){
  for (var obj of tables){
    const sql = `SELECT * FROM ${obj.schema}.${obj.table} limit 10`
    console.log(sql)
    var res1 = await exec_query(conn, token, sql)
    var res2 = await exec_query(second_conn, second_token, sql)
    console.log("From source")
    console.log(JSON.stringify(res1.data.attributes.results, null, 2))
    console.log("From dest")
    console.log(JSON.stringify(res2.data.attributes.results, null, 2))
    const diff = json_diff.diff(res1.data.attributes.results,res2.data.attributes.results)
    console.log("Diff")
    console.log(JSON.stringify(diff, null, 2))
    break
  }
}

async function compare_existing_results(conn, token){
  const conn_payload = {
    target: "server1",
    user: "maxuser",
    password: "maxpwd",
    db: "test"
  }
    
  var res = await axios.post("http://localhost:8989/sql/", conn_payload, opts)
  const second_token = "?token=" + res.data.meta.token
  const second_conn = res.data.links.self
  const second_id = res.data.data.id
  console.log("second token", second_token)
  console.log("second_conn", second_conn)
  console.log("second_id", second_id)

  console.log("conn", conn)
  console.log("token", token)
  console.log("QUERY")
  const schema = "bookings"
  const pg_sql = `SELECT JSON_AGG(JSON_BUILD_OBJECT('schema', TABLE_SCHEMA, 'table', TABLE_NAME)) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '${schema}' AND TABLE_TYPE = 'BASE TABLE';`
  const maria_sql = `SELECT JSON_ARRAYAGG(JSON_OBJECT('schema', TABLE_SCHEMA, 'table', TABLE_NAME)) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '${schema}' AND TABLE_TYPE = 'BASE TABLE'`
  var res = await exec_query(conn, token, pg_sql)
  const tables = JSON.parse(res.data.attributes.results[0].data[0][0])  
  console.log("TABLES")
  await compare_results(conn, token, second_conn, second_token, tables)
  
  await axios.delete(second_conn + second_token, opts)

}

async function test_import_with_connection(conn, token){
  const conn_payload = {
    target: "server1",
    user: "maxuser",
    password: "maxpwd",
    db: "test"
  }
    
  var res = await axios.post("http://localhost:8989/sql/", conn_payload, opts)
  const second_token = "?token=" + res.data.meta.token
  const target_token = "&target_token=" + res.data.meta.token
  const second_conn = res.data.links.self
  const second_id = res.data.data.id
  console.log("second token", second_token)
  console.log("second_conn", second_conn)
  console.log("second_id", second_id)

  var res = null
  var tables = []
  
  if (process.argv[3]){
    tables = JSON.parse(process.argv[3])
  } else {
    const schema = "bookings"
    const pg_sql = `SELECT JSON_AGG(JSON_BUILD_OBJECT('schema', TABLE_SCHEMA, 'table', TABLE_NAME)) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '${schema}' AND TABLE_NAME NOT IN ('flights') AND TABLE_TYPE = 'BASE TABLE';`
    const maria_sql = `SELECT JSON_ARRAYAGG(JSON_OBJECT('schema', TABLE_SCHEMA, 'table', TABLE_NAME)) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '${schema}' AND TABLE_TYPE = 'BASE TABLE'`
    res = await exec_query(conn, token, pg_sql)
    var tables = JSON.parse(res.data.attributes.results[0].data[0][0])
  }
 
  console.log(tables)
  
  const payload = {
    type: "mariadb",
    //type: "postgresql",
    //type: "generic",
    target: second_id,
    tables: tables,
    catalog: "demo"
  }
  
  console.log("Prepare")
  res = await axios.post(conn + "etl/prepare" + token + target_token, payload, opts)

  self = res.data.links.self

  while (res.status == 202)
  {
    await new Promise((res) => setTimeout(res, 1000));
    res = await axios.get(self + token, opts)
  }

  for (const t of res.data.data.attributes.results.tables) {
    if (t.error){
      console.log(t.schema, t.table, ": ", t.error)
    } else {
      console.log(t.create)
      console.log(t.select)
      console.log(t.insert)
    }
  }
  
  console.log(JSON.stringify(res.data, null, 2))
  payload.tables = res.data.data.attributes.results.tables

  console.log("Start")
  res = await axios.post(conn + "etl/start" + token + target_token, payload, opts)

  self = res.data.links.self

  while (res.status == 202)
  {
    await new Promise((res) => setTimeout(res, 1000));
    res = await axios.get(self + token, opts)
  }

  console.log(JSON.stringify(res.data, null, 2))

  for (const t of res.data.data.attributes.results.tables) {
    if (t.error){
      console.log(t.schema, t.table, ": ", t.error)
    }
  }

  //await compare_results(conn, token, second_conn, second_token, payload.tables)
  
  await axios.delete(second_conn + second_token, opts)

}

async function test_import_cancel(conn, token){
  const conn_payload = {
    target: "server1",
    user: "maxuser",
    password: "maxpwd",
    db: "test"
  }
    
  var res = await axios.post("http://localhost:8989/sql/", conn_payload, opts)
  const second_token = "?token=" + res.data.meta.token
  const target_token = "&target_token=" + res.data.meta.token
  const second_conn = res.data.links.self
  const second_id = res.data.data.id
  console.log("second token", second_token)
  console.log("second_conn", second_conn)
  console.log("second_id", second_id)

  const schema = "bookings"
  const pg_sql = `SELECT JSON_AGG(JSON_BUILD_OBJECT('schema', TABLE_SCHEMA, 'table', TABLE_NAME)) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '${schema}' AND TABLE_NAME NOT IN ('flights') AND TABLE_TYPE = 'BASE TABLE';`
  const maria_sql = `SELECT JSON_ARRAYAGG(JSON_OBJECT('schema', TABLE_SCHEMA, 'table', TABLE_NAME)) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '${schema}' AND TABLE_TYPE = 'BASE TABLE'`
  var res = await exec_query(conn, token, pg_sql)
  var tables = JSON.parse(res.data.attributes.results[0].data[0][0])
  
  if (process.argv[3]){
    console.log("Using tables from command line")
    tables = JSON.parse(process.argv[3])
  }

  console.log(tables)
  
  const payload = {
    //type: "mariadb",
    type: "postgresql",
    //type: "generic",
    target: second_id,
    tables: tables,
    catalog: "demo"
  }
  
  console.log("Prepare")
  res = await axios.post(conn + "etl/prepare" + token + target_token, payload, opts)

  self = res.data.links.self

  while (res.status == 202)
  {
    await new Promise((res) => setTimeout(res, 1000));
    res = await axios.get(self + token, opts)
  }

  for (const t of res.data.data.attributes.results.tables) {
    if (t.error){
      console.log(t.schema, t.table, ": ", t.error)
    } else {
      console.log(t.create)
      console.log(t.select)
      console.log(t.insert)
    }
  }
  
  console.log(JSON.stringify(res.data, null, 2))
  payload.tables = res.data.data.attributes.results.tables

  console.log("Start")
  res = await axios.post(conn + "etl/start" + token + target_token, payload, opts)

  self = res.data.links.self

  await new Promise((res) => setTimeout(res, 5000));
  res = await axios.get(self + token, opts)

  console.log("Cancel")
  res = await axios.post(conn + "cancel" + token + target_token, payload, opts)
  console.log(JSON.stringify(res.data, null, 2))

  res = await axios.get(self + token, opts)
  
  while (res.status == 202)
  {
    await new Promise((res) => setTimeout(res, 1000));
    res = await axios.get(self + token, opts)
  }

  console.log("After cancel")
  
  console.log(JSON.stringify(res.data, null, 2))

  for (const t of res.data.data.attributes.results.tables) {
    if (t.error){
      console.log(t.schema, t.table, ": ", t.error)
    }
  }

  //await compare_results(conn, token, second_conn, second_token, payload.tables)
  
  await axios.delete(second_conn + second_token, opts)

}

async function test_import_prepare_only(conn, token){
  const conn_payload = {
    target: "server1",
    user: "maxuser",
    password: "maxpwd",
    db: "test"
  }
    
  var res = await axios.post("http://localhost:8989/sql/", conn_payload, opts)
  const second_token = "?token=" + res.data.meta.token
  const target_token = "&target_token=" + res.data.meta.token
  const second_conn = res.data.links.self
  const second_id = res.data.data.id
  console.log("second token", second_token)
  console.log("second_conn", second_conn)
  console.log("second_id", second_id)

  const schema = "bookings"
  const pg_sql = `SELECT JSON_AGG(JSON_BUILD_OBJECT('schema', TABLE_SCHEMA, 'table', TABLE_NAME)) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '${schema}' AND TABLE_TYPE = 'BASE TABLE';`
  const maria_sql = `SELECT JSON_ARRAYAGG(JSON_OBJECT('schema', TABLE_SCHEMA, 'table', TABLE_NAME)) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '${schema}' AND TABLE_TYPE = 'BASE TABLE'`
  var res = await exec_query(conn, token, pg_sql)
  var tables = JSON.parse(res.data.attributes.results[0].data[0][0])
  
  if (process.argv[3]){
    tables = JSON.parse(process.argv[3])
  }

  console.log(tables)
  
  const payload = {
    //type: "mariadb",
    type: "postgresql",
    //type: "generic",
    catalog: "demo",
    target: second_id,
    tables: tables
  }
  
  console.log("Prepare")
  res = await axios.post(conn + "etl/prepare" + token + target_token, payload, opts)

  self = res.data.links.self

  while (res.status == 202)
  {
    await new Promise((res) => setTimeout(res, 1000));
    res = await axios.get(self + token, opts)
  }

  for (const t of res.data.data.attributes.results.tables) {
    console.log(t.create)
    console.log(t.select)
    console.log(t.insert)
  }
  
  console.log(JSON.stringify(res.data, null, 2))
  payload.tables = res.data.data.attributes.results.tables
  
  await axios.delete(second_conn + second_token, opts)

}

async function test_import_with_server(conn, token){  
  const payload = {
    type: "mariadb",
    target: "server4",
    user: "maxuser",
    password: "maxpwd",
    tables: [
      {table: "t1", schema: "test"},
      {table: "t2", schema: "test"},
    ]
  }
  
  console.log("Prepare")
  res = await axios.post(conn + "etl/prepare" + token, payload, opts)

  self = res.data.links.self

  while (res.status == 202)
  {
    await new Promise((res) => setTimeout(res, 1000));
    res = await axios.get(self + token, opts)
  }

  res.data.data.attributes.
  console.log(JSON.stringify(res.data, null, 2))
  payload.tables = res.data.data.attributes.results.tables
  
  console.log("Start")
  res = await axios.post(conn + "etl/start" + token, payload, opts)

  self = res.data.links.self

  while (res.status == 202)
  {
    await new Promise((res) => setTimeout(res, 1000));
    res = await axios.get(self + token, opts)
  }

  console.log(JSON.stringify(res.data, null, 2))
}

async function test_sql_data_types(conn, token){
  const data_types_sql =`
CREATE OR REPLACE TABLE test.t1(
a TINYINT,
b TINYINT UNSIGNED,
c SMALLINT,
d SMALLINT UNSIGNED,
e INT,
f INT UNSIGNED,
g BIGINT,
h BIGINT UNSIGNED,
i FLOAT,
j DOUBLE,
k DECIMAL(65, 38),
l CHAR(10),
m VARCHAR(10),
n TEXT,
o TIME,
p DATE,
q DATETIME,
r TIMESTAMP);

INSERT IGNORE INTO test.t1 SELECT seq, seq, seq, seq, seq, seq, seq, seq, seq * 0.123, seq * 0.234, seq * 0.345, seq, seq, seq, seq, NOW(), NOW(), NOW() FROM seq_0_to_1000000_step_10000;
SELECT * FROM test.t1;
COMMIT;
`
  print_query(conn, token, data_types_sql)
}

async function test_async_query(conn, token){  
  var res = await axios.post(conn + "queries" + token + "&async=true", {sql: "SELECT SLEEP(1)"}, opts)
  self = res.data.links.self
  var loops = 0

  while (res.status == 202)
  {
    await new Promise((res) => setTimeout(res, 500));
    res = await axios.get(self + token, opts)
    loops++
  }

  console.log(JSON.stringify(res.data, null, 2))
  console.log("Loops: ", loops)

  await new Promise((res) => setTimeout(res, 500));
  res = await axios.get(self + token, opts)

  console.log("Reading again after a successful read")
  console.log(JSON.stringify(res.data, null, 2))

  await axios.delete(self + token, opts)

  console.log("Reading after deleting")
  res = await axios.get(self + token, opts)
  console.log(JSON.stringify(res.data, null, 2))
}

async function main() {
  const payload = {
    target: "odbc",
    connection_string: process.argv[2]
  }
  //var res = await axios.get("http://localhost:8989/sql/odbc/drivers", opts)
  //console.log(JSON.stringify(res.data, null, 2))
  
  var res = await axios.post("http://localhost:8989/sql/", payload, opts)
  const token = "?token=" + res.data.meta.token
  const connection = res.data.links.self
  console.log(connection);

  //await test_query(connection, token)
  //await test_sql_data_types(connection, token)
  //await test_async_query(connection, token)
  //await test_import_prepare_only(connection, token)
  await test_import_with_connection(connection, token)
  //await test_import_cancel(connection, token)
  //await test_import_with_server(connection, token)
  //await compare_existing_results(connection, token)
  
  await axios.delete(connection + token, opts)
  return "ok"
}

main()
  .then(console.log)
  .catch((err) => {
    if (err.response){
      if (err.response.data) {
        console.log("err.response.data");
        console.log(JSON.stringify(err.response.data, null, 2))
      } else{
        console.log("err.response")
        console.log(err.response)
      }
    } else {
      console.log("Something else")
      console.log(err)
    }
  })
