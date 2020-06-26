const debug = require("debug")("metrics");
const client = require('prom-client');

const registry = new client.Registry();

// UP metric
const up = new client.Gauge({name: 'up', help: "UP Status", registers: [registry]});

// Query based metrics
// -------------------
// Collect metrics based on queries that are too slow for short scrape interval
const mssql_object_fragmentation = {
    metrics: {
	    mssql_object_fragmentation_percent: new client.Gauge({name: 'mssql_object_fragmentation_percent', help: 'Show percent object fragmentation', labelNames: ["DatabaseName", "ObjectID", "TableName", "IndexName"], registers: [registry]}),
    },
    query: `SELECT 
    OBJECT_NAME(ps.object_id) AS TableName 
   ,i.name AS IndexName         ,ips.index_type_desc 
   ,index_level 
   ,ips.avg_fragmentation_in_percent 
   ,ips.avg_page_space_used_in_percent 
   ,ips.page_count 
FROM   sys.dm_db_partition_stats ps 
INNER JOIN sys.indexes i ON ps.object_id = i.object_id AND ps.index_id = i.index_id 
CROSS APPLY sys.dm_db_index_physical_stats(DB_ID(), ps.object_id, ps.index_id, null, 'DETAILED') ips 
ORDER BY ips.avg_fragmentation_in_percent DESC`,
    collect: function (rows, metrics, config) {
	
    
    let dbname = config.connect.options.database;
    debug("For: Percent Fragmentation -", dbname);
	for (let i = 0; i < rows.length; i++) {
	    const row = rows[i];
	    const mssql_table_name = row[0].value;
	    const mssql_index_name = row[1].value;
	    const mssql_fragmentation_in_percent = row[4].value;
	    const mssql_proposed_index = row[3].value;
	    debug(" Percent Fragmentation -", dbname);
	    metrics.mssql_object_fragmentation_percent.set({DatabaseName: dbname, TableName: mssql_table_name, IndexName: mssql_index_name},mssql_fragmentation_in_percent);
       }
    }

};
	



const metrics = [
    mssql_object_fragmentation,
];

module.exports = {
    client: client,
    metrics: metrics,
    registerSlow : registry,
    up:up
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

