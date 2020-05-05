const pg = require('pg');
const Q = require("q");

var flag2 = 1;
var skip=0;

function ins(pgClient, table, data){
    return new Promise((resolve, reject) => {
        var qry = "INSERT INTO "+table+" VALUES "+data;
        pgClient.query(qry, function(err, results) {
            if (err) {
            console.error(err);
            return reject(err);
            }
            resolve(results);
        })
    })
}

// function while menggunakan promise
function promiseWhile(condition, body) {
    var done = Q.defer();

    function loop() {
        if (!condition()) return done.resolve();
        Q.when(body(), loop, done.reject);
    }
    Q.nextTick(loop);

    // The promise
    return done.promise;
}
// untuk localhost biasa
// const pgConString = "postgres://postgres:1234@localhost:5432/staging_ingestion";

// untuk localhost dari docker
// const pgConString = "postgres://postgres:mysecretpassword@172.17.0.2:5432/staging_ingestion";
const pgConString = "postgres://"+process.env.USER+":"+process.env.PASSWORD+"@"+process.env.HOST+":"+process.env.PORT+"/"+process.env.DATABASE;

var clientpg = new pg.Client(pgConString);

// untuk localhost biasa
// const pgConString2 = "postgres://postgres:1234@localhost:5432/staging_transformation";

// untuk localhost dari docker
// const pgConString2 = "postgres://postgres:mysecretpassword@172.17.0.2:5432/staging_transformation";
const pgConString2 = "postgres://"+process.env.USER2+":"+process.env.PASSWORD2+"@"+process.env.HOST2+":"+process.env.PORT2+"/"+process.env.DATABASE2;

var clientpg2 = new pg.Client(pgConString2);

clientpg.connect(function(err){
    if(err){
        throw err;
    }
    else{
        clientpg2.connect(function(err){
            if(err){
                throw err;
            }
            else{
                var offset = 0;
                var count = 0;

                var dropTable="DROP TABLE IF EXISTS stg_superstore";
                clientpg2.query(dropTable);
                console.log("Drop Table stg_superstore");

                var pgTable3 = "CREATE TABLE IF NOT EXISTS stg_superstore ("+
                        "row_id SERIAL PRIMARY KEY," +
                        "order_id VARCHAR(14)," +
                        "order_date DATE," +
                        "ship_date DATE," +
                        "ship_mode VARCHAR(14)," +
                        "customer_id VARCHAR(8)," + 
                        "customer_name VARCHAR(22)," +
                        "segment VARCHAR(11)," +
                        "country VARCHAR(13)," +
                        "city VARCHAR(16)," +
                        "state VARCHAR(20)," +
                        "postal_code VARCHAR(6)," +
                        "region VARCHAR(7)," +
                        "product_id VARCHAR(15)," +
                        "category VARCHAR(15)," +
                        "sub_category VARCHAR(11)," +
                        "product_name VARCHAR(127)," +
                        "sales NUMERIC(9, 4)," +
                        "quantity NUMERIC(3, 1)," +
                        "discount NUMERIC(3, 2)," +
                        "profit NUMERIC(10, 4)," +
                        "base_price NUMERIC(10, 4)," +
                        "total_sales NUMERIC(10, 4)" +
                    ");";
        
                var pgTable4 = "CREATE TABLE IF NOT EXISTS staging_ingestion_log ("+
                        "job_id SERIAL PRIMARY KEY," +
                        "proses_tsamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP," +
                        "number_of_row INTEGER," +
                        "operation VARCHAR(10)" +
                    ");";
                
                var stgQuery = "SELECT row_id, order_id, DATE(order_date) order_date, DATE(ship_date) ship_date, ship_mode, customer_id, customer_name, segment," +
                    "CASE WHEN Country = 'United States' THEN 'USA' " +
                    "ELSE Country " +
                    "END AS country, " +
                    "UPPER(City) city, " +
                    "UPPER(State) state, " +
                    "postal_code, " +
                    "UPPER(Region) region, " +
                    "product_id, category, sub_category, product_name, sales, quantity, discount, profit, " +
                    "Sales-Profit base_price, " +
                    "Sales*Quantity total_sales " +
                    "FROM superstore ORDER BY row_id LIMIT 1000 OFFSET "+offset;
                
                clientpg2.query(pgTable3, function(err, result){
                    if(err) throw err;
                    else{
                        console.log("Create Table stg_superstore");
                        clientpg2.query(pgTable4);
                        console.log("Create Table staging_ingestion_log");
                        clientpg2.query("SELECT * FROM stg_superstore LIMIT 1", function(err, result){
                            if(err) throw err;
                            else{
                                if(result.rows.length > 0){
                                    skip=1;
                                    flag2=0;
                                }
                                promiseWhile(function() { return flag2 == 1 }, function (){
                                    clientpg.query(stgQuery, function(err, res){
                                        if(err) throw err;
                                        else{
                                            if(res.rows.length > 0){
                                                var full_data = [];
                                                for(var x=0;x<res.rows.length;x++){
                                                    var data = [];
                                                    var value = Object.values(res.rows[x]);
                                                    var keys = Object.keys(res.rows[x]);
                                                    var tempstr = "";
                                                    for(var i=0;i<value.length;i++){
                                                        if(keys[i]=="order_date" || keys[i]=="ship_date"){
                                                            // merubah format timestamp menjadi date
                                                            tempstr = value[i].toDateString();
                                                            value[i] = tempstr;
                                                        }
                                                        if(keys[i]=="customer_name"||keys[i]=="product_name"){
                                                            value[i] = value[i].replace(/'/g,"''");
                                                        }
                                                        data.push("'" + value[i] + "'");
                                                    }
                                                    full_data.push('(' + data.join(', ') + ')');
                                                }
                                                // console.log(full_data);
                                                ins(clientpg2, "stg_superstore", full_data);
                                                count += res.rows.length;
                                                console.log("Running data " + count);
                                                var stg_log = "(DEFAULT,DEFAULT,"+res.rows.length+",'INSERT')";
                                                ins(clientpg2, "staging_ingestion_log",stg_log);
                                            }
                                            else{
                                                flag2 = 0;
                                            }
                                        }
                                    });
                                    offset += 1000;
                                    stgQuery = "SELECT row_id, order_id, DATE(order_date) order_date, DATE(ship_date) ship_date, ship_mode, customer_id, customer_name, segment," +
                                        "CASE WHEN Country = 'United States' THEN 'USA' " +
                                        "ELSE Country " +
                                        "END AS country, " +
                                        "UPPER(City) city, " +
                                        "UPPER(State) state, " +
                                        "postal_code, " +
                                        "UPPER(Region) region, " +
                                        "product_id, category, sub_category, product_name, sales, quantity, discount, profit, " +
                                        "Sales-Profit base_price, " +
                                        "Sales*Quantity total_sales " +
                                        "FROM superstore ORDER BY row_id LIMIT 1000 OFFSET "+offset;
                                    return Q.delay(0); // arbitrary async
                                }).then(function(){
                                    if(skip==0)console.log("Done Input");
                                    else console.log("ALready have data in Table stg_superstore");
                                    setTimeout((function() {
                                        return process.exit(0);
                                    }), 1000);
                                });  
                            }   
                        })
                        
                    }
                });
            }
        })
    }
});
