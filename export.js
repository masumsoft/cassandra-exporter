var Promise = require('bluebird');
var cassandra = require('cassandra-driver');
var jsonfile = require('jsonfile');

var HOST = process.env.HOST || '127.0.0.1';
var KEYSPACE = process.env.KEYSPACE;

if (!KEYSPACE) {
    console.log('`KEYSPACE` must be specified as environment variable');
    process.exit();
}

var systemClient = new cassandra.Client({contactPoints: [HOST]});
var client = new cassandra.Client({ contactPoints: [HOST], keyspace: KEYSPACE});

function processTableExport(table) {
    var rows = [];
    var query = 'SELECT * FROM "' + table + '"';
    var options = { prepare : true , fetchSize : 1000 };

    console.log('==================================================');
    console.log('Reading table: ' + table);
    return new Promise(function(resolve, reject) {
        client.eachRow(query, [], options, function (n, row) {
            var rowObject = {};
            row.forEach(function(value, key){
                rowObject[key] = value;
            });
            rows.push(rowObject);
        }, function (err, result) {

            if (err) {
                reject(err);
                return;
            }

            console.log('Loaded ' + rows.length + ' rows from table: ' + table);

            if (result.nextPage) {
                result.nextPage();
                return;
            }

            console.log('Writing all rows into ' + table + '.json...');
            jsonfile.writeFile('data/' + table + '.json', rows, function (err) {
                if (err) {
                    reject(err);
                    return;
                }
                console.log('Done with table: ' + table);
                resolve();
            });
        });
    });
}

systemClient.connect()
    .then(function (){
        var systemQuery = "SELECT columnfamily_name as table_name FROM system.schema_columnfamilies WHERE keyspace_name = ?";
        if (systemClient.metadata.keyspaces.system_schema) {
            systemQuery = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?";
        }

        console.log('Getting tables from keyspace: ' + KEYSPACE);
        return systemClient.execute(systemQuery, [KEYSPACE]);
    })
    .then(function (result){
        var tables = [];
        for(var i=0; i<result.rows.length; i++) {
            tables.push(result.rows[i].table_name);
        }

        return Promise.each(tables, function(table){
            return processTableExport(table);
        });
    })
    .then(function (){
        console.log('==================================================');
        console.log('Completed exporting all tables from keyspace: ' + KEYSPACE);
        systemClient.shutdown();
        client.shutdown();
    })
    .catch(function (err){
        console.log(err);
    });
