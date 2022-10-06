const mariadb = require("mariadb")
const pool = mariadb.createPool({
    host: '127.0.0.1',
    port: 3000,
    user:'maxuser',
    password: 'maxpwd',
    connectionLimit: 100,
    database: 'test'
});
async function asyncFunction() {
    let conn;
    try {
        conn = await pool.getConnection();
        const rows = await conn.query("SELECT 1 as val");
        console.log(rows); //[ {val: 1}, meta: ... ]

        const res1 = await conn.query("CREATE OR REPLACE TABLE myTable (id INT, name varchar(255))");
        console.log(res1); // { affectedRows: 1, insertId: 1, warningStatus: 0 }                                                                                                                                                                                                           
        const res2 = await conn.query("INSERT INTO myTable VALUES (?, ?)", [1, "mariadb"]);
        console.log(res2); // { affectedRows: 1, insertId: 1, warningStatus: 0 }                                                                                                                                                                                                           
    } catch (err) {
        throw err;
    } finally {
        if (conn) conn.end();
        pool.end()
    }
}
asyncFunction().then(console.log, console.log)
