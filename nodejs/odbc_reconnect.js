const axios = require("axios")
const json_diff = require("json-diff")
const opts = {auth: {username: "admin", password: "mariadb"}}

async function reconnect_loop(payload) {
  var res = await axios.post("http://127.0.0.1:8989/sql/", payload, opts)
  const token = "?token=" + res.data.meta.token
  const connection = res.data.links.self

  while (true)
  {
    try{
      var res = await axios.post(connection + "reconnect" + token, null, opts)
      res = await axios.post(connection + "queries" + token, {"sql": "select 1"}, opts)
    } catch (e){
      console.log(e.toString())
    }
  }
  
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

  await reconnect_loop(payload_odbc)
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
