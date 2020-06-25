const debug = require("debug")("metrics");
const client = require('prom-client');

// Query based metrics
// -------------------
// Collect data from query store SQL 2017
const mssql_missing_index = {
    metrics: {
	    mssql_missing_index_suggestion: new client.Gauge({name: 'mssql_missing_index_suggestion', help: 'Sugget index creation', labelNames: ["DatabaseName", "ObjectID", "ObjectName", "ProposedIndex"]}),
    },
    query: `SELECT 
	db.[name] AS [DatabaseName]
    ,id.[object_id] AS [ObjectID]
	,OBJECT_NAME(id.[object_id], db.[database_id]) AS [ObjectName]
    ,'CREATE INDEX [IX_' + OBJECT_NAME(id.[object_id], db.[database_id]) + '_' + REPLACE(REPLACE(REPLACE(ISNULL(id.[equality_columns], ''), ', ', '_'), '[', ''), ']', '') + CASE
        WHEN id.[equality_columns] IS NOT NULL
            AND id.[inequality_columns] IS NOT NULL
            THEN '_'
        ELSE ''
        END + REPLACE(REPLACE(REPLACE(ISNULL(id.[inequality_columns], ''), ', ', '_'), '[', ''), ']', '') + '_' + LEFT(CAST(NEWID() AS [nvarchar](64)), 5) + ']' + ' ON ' + id.[statement] + ' (' + ISNULL(id.[equality_columns], '') + CASE
        WHEN id.[equality_columns] IS NOT NULL
            AND id.[inequality_columns] IS NOT NULL
            THEN ','
        ELSE ''
        END + ISNULL(id.[inequality_columns], '') + ')' + ISNULL(' INCLUDE (' + id.[included_columns] + ')', '') AS [ProposedIndex]
FROM [sys].[dm_db_missing_index_group_stats] gs WITH (NOLOCK)
INNER JOIN [sys].[dm_db_missing_index_groups] ig WITH (NOLOCK) ON gs.[group_handle] = ig.[index_group_handle]
INNER JOIN [sys].[dm_db_missing_index_details] id WITH (NOLOCK) ON ig.[index_handle] = id.[index_handle]
INNER JOIN [sys].[databases] db WITH (NOLOCK) ON db.[database_id] = id.[database_id]
WHERE  db.[database_id] = DB_ID()
OPTION (RECOMPILE)`,
    collect: function (rows, metrics, config) {
	
    
    let dbname = config.connect.options.database;
    debug("For: Suggested Index Creation -", dbname);
	for (let i = 0; i < rows.length; i++) {
	    const row = rows[i];
	    const mssql_database_name = row[0].value;
	    const mssql_object_id = row[1].value;
	    const mssql_object_name = row[2].value;
	    const mssql_proposed_index = row[3].value;
	    debug("Suggested Index Creation -", dbname);
	    metrics.mssql_total_execution_count.set({DatabaseName: mssql_database_name, ObjectID: mssql_object_id, ObjectName: mssql_object_name, ProposedIndex: mssql_proposed_index},1);
       }
    }

};
	



const metrics = [
    mssql_missing_index,
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

