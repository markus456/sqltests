const axios = require("axios")
const json_diff = require("json-diff")
const opts = {auth: {username: "admin", password: "mariadb"}}

async function kill_query(payload) {
  var res = await axios.post("http://localhost:8989/sql/", payload, opts)
  const token = "?token=" + res.data.meta.token
  const connection = res.data.links.self

  console.time("query")
  var res = axios.post(connection + "queries" + token, {sql: process.argv[3]}, opts)
  await new Promise((res) => setTimeout(res, 1000));

  await axios.post(connection + "cancel/" + token, {}, opts)
  const data = await res
  console.log(JSON.stringify(data.data, null, 2))
  console.timeEnd("query");
  
  await axios.delete(connection + token, opts)
  return "ok"
}

async function main() {
  const payload_odbc = {
    target: "odbc",
    connection_string: process.argv[2]
  }
   const payload_mariadb = {
    target: "server1",
    user: "maxuser",
    password: "maxpwd",
    db: "test"
   }

  await kill_query(payload_odbc)
  await kill_query(payload_mariadb)
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
