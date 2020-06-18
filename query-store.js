const debug = require("debug")("metrics");
const client = require('prom-client');

// Query based metrics
// -------------------
// Collect data from query store SQL 2017
const mssql_most_exec_query = {
    metrics: {
	    mssql_total_execution_count: new client.Gauge({name: 'mssql_total_execution_count', help: 'Total Execution Count', labelNames: ["database", "query_id", "query_text_id", "query_sql_text"]}),
    },
    query: `SELECT TOP 100 q.query_id, qt.query_text_id, qt.query_sql_text, SUM(rs.count_executions) AS total_execution_count
FROM sys.query_store_query_text AS qt
JOIN sys.query_store_query AS q
    ON qt.query_text_id = q.query_text_id
JOIN sys.query_store_plan AS p
    ON q.query_id = p.query_id
JOIN sys.query_store_runtime_stats AS rs
    ON p.plan_id = rs.plan_id
WHERE rs.avg_duration > 1000000
GROUP BY q.query_id, qt.query_text_id, qt.query_sql_text
ORDER BY total_execution_count DESC`,
    collect: function (rows, metrics, config) {
	let dbname = config.connect.options.database;
	for (let i = 0; i < rows.length; i++) {
	    const row = rows[i];
	    const mssql_query_id = row[0].value;
	    const mssql_query_text_id = row[1].value;
	    const mssql_query_sql_text = row[2].value;
	    const mssql_total_execution_count = row[3].value;
	    debug("Most Executed Queries -", dbname);
	    metrics.mssql_total_execution_count.set({database: dbname, query_id: mssql_query_id, query_text_id: mssql_query_text_id, query_sql_text: mssql_query_sql_text},mssql_total_execution_count);
       }
    }

};
	
const mssql_most_avg_time_query = {
    metrics: {
	    mssql_avg_duration: new client.Gauge({name: 'mssql_avg_duration_us', help: 'Average Query Duration in micro seconds', labelNames: ["database", "query_sql_text", "query_id"]}),
    },
    query: `SELECT TOP 100 avg(rs.avg_duration) AS avg_duration, max(qt.query_sql_text) AS query_sql_text, q.query_id, GETUTCDATE() AS CurrentUTCTime, max(rs.last_execution_time) AS last_execution_time
FROM sys.query_store_query_text AS qt       
JOIN sys.query_store_query AS q          
    ON qt.query_text_id = q.query_text_id              
JOIN sys.query_store_plan AS p           
    ON q.query_id = p.query_id        
JOIN sys.query_store_runtime_stats AS rs          
    ON p.plan_id = rs.plan_id              
WHERE rs.last_execution_time > DATEADD(hour, -1, GETUTCDATE()) and rs.avg_duration > 1000000
GROUP BY q.query_id      
ORDER BY avg(rs.avg_duration) DESC`,
    collect: function (rows, metrics, config) {
	let dbname = config.connect.options.database;
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const mssql_avg_duration = row[0].value;
            const mssql_query_sql_text = row[1].value;
            const mssql_query_id = row[2].value;
            debug("Most Average Time Query -", dbname);
            metrics.mssql_avg_duration.set({database: dbname, query_sql_text: mssql_query_sql_text, query_id: mssql_query_id},mssql_avg_duration);
	}
    }
};

const mssql_most_avg_io_query = {
    metrics: {
	mssql_avg_physical_io_reads: new client.Gauge({name: 'mssql_avg_physical_io_reads', help: 'Average Physical IO Reads', labelNames: ["database", "query_sql_text", "query_id"]}),
        mssql_avg_rowcount : new client.Gauge({name: 'mssql_avg_rowcount', help: 'Average Row Count', labelNames: ["database", "query_sql_text", "query_id"]}),
        mssql_count_executions : new client.Gauge({name: 'mssql_count_executions', help: 'Cont Executions', labelNames: ["database", "query_sql_text", "query_id"]}),
    },
    query: `SELECT TOP 10 avg(rs.avg_physical_io_reads) as avg_physical_io_reads, max(qt.query_sql_text) as query_sql_text, q.query_id, avg(rs.avg_rowcount) as avg_rowcount, sum(rs.count_executions) as count_executions
FROM sys.query_store_query_text AS qt
JOIN sys.query_store_query AS q
    ON qt.query_text_id = q.query_text_id
JOIN sys.query_store_plan AS p
    ON q.query_id = p.query_id
JOIN sys.query_store_runtime_stats AS rs
    ON p.plan_id = rs.plan_id
JOIN sys.query_store_runtime_stats_interval AS rsi 
    ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
WHERE rsi.start_time >= DATEADD(hour, -1, GETUTCDATE()) and rs.avg_duration > 1000000
GROUP BY q.query_id
ORDER BY avg(rs.avg_physical_io_reads) DESC`,
    collect: function (rows, metrics, config) {
	let dbname = config.connect.options.database;
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const mssql_avg_physical_io_reads = row[0].value;
            const mssql_query_sql_text = row[1].value;
            const mssql_query_id = row[2].value;
            const mssql_avg_rowcount = row[3].value;
            const mssql_count_executions = row[4].value;
            debug("Most Average IO Query -", dbname);
	    metrics.mssql_avg_physical_io_reads.set({database: dbname, query_sql_text: mssql_query_sql_text, query_id: mssql_query_id},mssql_avg_physical_io_reads);
            metrics.mssql_avg_rowcount.set({database: dbname, query_sql_text: mssql_query_sql_text, query_id: mssql_query_id},mssql_avg_rowcount);
            metrics.mssql_count_executions.set({database: dbname, query_sql_text: mssql_query_sql_text, query_id: mssql_query_id},mssql_count_executions);
	}
    }
};

const mssql_most_wait_query = {
    metrics: {
	    mssql_sum_total_wait_ms: new client.Gauge({name: 'mssql_sum_total_wait_ms', help: 'Total Wait ms', labelNames: ["database", "query_sql_text", "query_text_id", "query_id"]}),
    },
    query: `SELECT qt.query_sql_text, qt.query_text_id, st.sum_total_wait_ms,  q.query_id
FROM sys.query_store_query q
JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
JOIN (
 SELECT TOP 50  p.query_id, sum(total_query_wait_time_ms) AS sum_total_wait_ms 
 FROM sys.query_store_wait_stats ws 
 JOIN sys.query_store_plan p ON ws.plan_id = p.plan_id 
 JOIN sys.query_store_query q ON p.query_id = q.query_id 
 JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id 
 JOIN sys.query_store_runtime_stats AS rs ON p.plan_id = rs.plan_id 
 WHERE rs.avg_duration > 1000000 
 GROUP BY p.query_id 
 ORDER BY sum_total_wait_ms DESC) as st ON st.query_id = q.query_id`,
    collect: function (rows, metrics, config) {
	let dbname = config.connect.options.database;
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
	    const mssql_query_sql_text = row[0].value;
	    const mssql_query_text_id = row[1].value;
	    const mssql_sum_total_wait_ms = row[2].value;
	    const mssql_query_id = row[3].value;
	    debug("Most Wait Query -", dbname);
	    metrics.mssql_sum_total_wait_ms.set({database: dbname, query_sql_text: mssql_query_sql_text, query_text_id: mssql_query_text_id, query_id: mssql_query_id},mssql_sum_total_wait_ms);
	}
    }
};


const metrics = [
    mssql_most_exec_query,
    mssql_most_avg_time_query,
    mssql_most_avg_io_query,
    mssql_most_wait_query,
];

module.exports = {
    client: client,
    metrics: metrics,
};

// DOCUMENTATION of queries and their associated metrics (targeted to DBAs)
if (require.main === module) {
    metrics.forEach(function (m) {
        for(let key in m.metrics) {
            if(m.metrics.hasOwnProperty(key)) {
                console.log("--", m.metrics[key].name, m.metrics[key].help);
            }
        }
        console.log(m.query + ";");
        console.log("");
    });

    console.log("/*");
    metrics.forEach(function (m) {
        for (let key in m.metrics) {
            if(m.metrics.hasOwnProperty(key)) {
                console.log("* ", m.metrics[key].name + (m.metrics[key].labelNames.length > 0 ? ( "{" + m.metrics[key].labelNames + "}") : ""), m.metrics[key].help);
            }
        }
    });
    console.log("*/");
}

