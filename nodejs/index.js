const mariadb = require('mariadb');
const pool = mariadb.createPool({
    host: '127.0.0.1',
    port:4006,
    user:'maxuser', 
    password: 'maxpwd',
    database: 'test',
    connectionLimit: 5
});
async function asyncFunction() {
    let conn;
    try {
	    conn = await pool.getConnection();

        for (var i = 0; i < 1000; i++) {
	        const rows = await conn.query("SELECT * FROM t1");
        }
        
        await conn.query("CREATE OR REPLACE TABLE myTable (id INT, data VARCHAR(255))");
	    await conn.query("INSERT INTO myTable value (?, ?)", [1, "mariadb"]);
        await conn.query("DROP TABLE myTable");
    } catch (err) {
	    throw err;
    } finally {
	    if (conn) return conn.end();
    }
}

asyncFunction().then(() => pool.end())
