const debug = require("debug")("app");
const Connection = require('tedious').Connection;
const Request = require('tedious').Request;
const app = require('express')();

const client = require('./metrics').client;
const up = require('./metrics').up;
const metrics = require('./metrics').metrics;

const queryStoreMetrics = require('./query-store').metrics;

let config = {
    connect: {
        server: process.env["SERVER"],
        userName: process.env["USERNAME"],
        password: process.env["PASSWORD"],
        options: {
            port: process.env["PORT"] || 1433,
            encrypt: true,
            rowCollectionOnRequestCompletion: true
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
        connection.on('end', () => {
            debug("Connection to database ended");
        });
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

async function syncExecSQL(dbrequest) {
    return new Promise((resolve) => {
        connection.execSql(dbrequest);
	
    });
}

async function collectQueryStoreDB(connection) {
    return new Promise((resolve) => {
        let request = new Request("SELECT name FROM sys.databases WHERE name  NOT IN ('master', 'tempdb', 'model', 'msdb')", async function (error, rowCount, rows) {
            if (!error) {
             for (row of rows) {
                     config.connect.options.database=row[0].value;
		     let dbconnect = await connect();
		     let dbrequest = new Request("SELECT  desired_state_desc FROM sys.database_query_store_options", async function (DBerror, DBrowCount, DBrows) {
                         if (!DBerror && DBrowCount > 0 && DBrows[0][0].value != "OFF") {
		             await queryStoreCollect(dbconnect);
                         }
			 
                         delete config.connect.options.database;
			 dbconnect.close();
			 resolve();
		     });
		     
	             await syncExecSQL(dbrequest);
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
	await collectQueryStoreDB(connection);
        connection.close();
        res.send(client.register.metrics());
    } catch (error) {
        // error connecting
        up.set(0);
        res.header("X-Error", error.message || error);
        res.send(client.register.getSingleMetricAsString(up.name));
    }
});

const server = app.listen(config.port, function () {
    debug(`Prometheus-MSSQL Exporter listening on local port ${config.port} monitoring ${config.connect.userName}@${config.connect.server}:${config.connect.options.port}`);
});

process.on('SIGINT', function () {
    server.close();
    process.exit(0);
});
