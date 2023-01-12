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

async function main() {
  const payload = {
    target: "odbc",
    connection_string: process.argv[2]
  }
  
  var res = await axios.post("http://localhost:8989/sql/", payload, opts)
  const token = "?token=" + res.data.meta.token
  const connection = res.data.links.self

  await test_query(connection, token)
  
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
