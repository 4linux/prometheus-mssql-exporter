const debug = require("debug")("app");
const Connection = require('tedious').Connection;
const Request = require('tedious').Request;
const app = require('express')();

const client = require('./metrics').client;
const up = require('./metrics').up;
const metrics = require('./metrics').metrics;

const queryStoreMetrics = require('./query-store').metrics;
const databaseMetrics = require('./database-metrics').metrics;

const clientSlow = require('./database-metrics-slow').client;
const databaseMetricsSlow = require('./database-metrics-slow').metrics;

const registerSlow = require('./database-metrics-slow').registerSlow;

let config = {      
    connect: {
        server: process.env["SERVER"],
        userName: process.env["USERNAME"],
        password: process.env["PASSWORD"],
        options: {
            port: process.env["PORT"] || 1433,
            encrypt: true,
            rowCollectionOnRequestCompletion: true,
	    requestTimeout: 50000
        }
    },
    port: process.env["EXPOSE"] || 4000
};

if (!config.connect.server) {
    throw new Error("Missing SERVER information")
}
if (!config.connect.userName) {
    throw new Error("Missing USERNAME information")
}
if (!config.connect.password) {
    throw new Error("Missing PASSWORD information")
}

/**
 * Connects to a database server and if successful starts the metrics collection interval.
 *
 * @returns Promise<Connection>
 */
async function connect() {
    return new Promise((resolve, reject) => {
        debug("Connecting to database", config.connect.server);
        let connection = new Connection(config.connect);
        connection.on('connect', (error) => {
            if (error) {
                console.error("Failed to connect to database:", error.message || error);
                reject(error);
            } else {
                debug("Connected to database");
                resolve(connection);
            }
        });
        //connection.on('end', () => {
        //    debug("Connection to database ended");
        //});
    });

}

/**
 * Recursive function that executes all collectors sequentially
 *
 * @param connection database connection
 * @param collector single metric: {query: string, collect: function(rows, metric)}
 *
 * @returns Promise of collect operation (no value returned)
 */
async function measure(connection, collector) {
    return new Promise((resolve) => {
        let request = new Request(collector.query, (error, rowCount, rows) => {
            if (!error) {
                collector.collect(rows, collector.metrics, config);
                resolve();
            } else {
                console.error("Error executing SQL query", collector.query, error);
                resolve();
            }
        });
        connection.execSql(request);
    });
}

/**
 * Function that collects from an active server. Should be called via setInterval setup.
 *
 * @param connection database connection
 *
 * @returns Promise of execution (no value returned)
 */
async function collect(connection) {
    up.set(1);
    for (let i = 0; i < metrics.length; i++) {
        await measure(connection, metrics[i]);
    }
}

async function queryStoreCollect(connection) {
    for (let i = 0; i < queryStoreMetrics.length; i++) {
        await measure(connection, queryStoreMetrics[i]);
    }
}


async function databaseCollect(connection){
    for (let i = 0; i < databaseMetrics.length; i++) {
        await measure(connection, databaseMetrics[i]);
    }
}

/*
Function that collect metrics based on complex and slow queries and should be scraped on larger intervals
 */
async function databaseSlowCollect(connection){
    for (let i = 0; i < databaseMetricsSlow.length; i++) {
        await measure(connection, databaseMetricsSlow[i]);
    }
}

async function syncExecSQL(dbconnect,slow=false) {
    return new Promise((resolve) => {
	     let dbrequest = new Request("SELECT  desired_state_desc FROM sys.database_query_store_options", async function (DBerror, DBrowCount, DBrows) {
            if (!DBerror && DBrowCount > 0 && DBrows[0][0].value != "OFF" && slow!=true) {
                //If Query Store is enabled collect some metrics
                await queryStoreCollect(dbconnect);
            }

            if(slow!=true){
                //Collect database specific metrics
                await databaseCollect(dbconnect);
            }else{
                await databaseSlowCollect(dbconnect);
            }


		    await dbDisconnect(dbconnect);
		    delete config.connect.options.database;
		    resolve();
	     });
            dbconnect.execSql(dbrequest);
    });
}

async function dbDisconnect(dbconnect) {
    return new Promise((resolve) => {
       	dbconnect.on('end', function () {
		resolve()
	})
        dbconnect.close();
    });
}

async function collectDBMetrics(connection,slow=false) {
    return new Promise((resolve) => {
        let request = new Request("SELECT name FROM sys.databases WHERE name  NOT IN ('master', 'tempdb', 'model', 'msdb')", async function (error, rowCount, rows) {
            if (!error) {
	     await dbDisconnect(connection);
             for (row of rows) {
                    config.connect.options.database=row[0].value;
                    try{
                        let dbconnect = await connect();
                        console.error("Conectado a ",config.connect.options.database );
	                    await syncExecSQL(dbconnect,slow);
                    }catch(error){
                        console.error("Unable to connect to: ",config.connect.options.database );
                        delete config.connect.options.database;
                    }
                    
	     }
	     resolve();
            } else {
                console.error("Error executing SQL query", collector.query, error);
            }   
        });	
        connection.execSql(request);
    });
}




app.get('/metrics', async (req, res) => {
    res.contentType(client.register.contentType);

    try {
        let connection = await connect();
        await collect(connection, metrics);
	    await dbDisconnect(connection);
        
        connection = await connect();
        await collectDBMetrics(connection);
        


        res.send(client.register.metrics());
    } catch (error) {
        // error connecting
        up.set(0);
        res.header("X-Error", error.message || error);
        res.send(client.register.getSingleMetricAsString(up.name));
    }
});


app.get('/metrics-slow', async (req, res) => {
    res.contentType(clientSlow.register.contentType);

    try {
        
        connection = await connect();
        await collectDBMetrics(connection,true);
        
        res.send(registerSlow.metrics());
    } catch (error) {
        // error connecting
        up.set(0);
        res.header("X-Error", error.message || error);
        res.send(cregisterSlow.getSingleMetricAsString(up.name));
    }
});

const server = app.listen(config.port, function () {
    debug(`Prometheus-MSSQL Exporter listening on local port ${config.port} monitoring ${config.connect.userName}@${config.connect.server}:${config.connect.options.port}`);
});

process.on('SIGINT', function () {
    server.close();
    process.exit(0);
});