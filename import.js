var _ = require('lodash');
var Promise = require('bluebird');
var cassandra = require('cassandra-driver');
var fs = require('fs');
var jsonStream = require('JSONStream');

var HOST = process.env.HOST || '127.0.0.1';
var KEYSPACE = process.env.KEYSPACE;

if (!KEYSPACE) {
    console.log('`KEYSPACE` must be specified as environment variable');
    process.exit();
}

var USER = process.env.USER;
var PASSWORD = process.env.PASSWORD;

var authProvider;

if (USER && PASSWORD) {
    authProvider = new cassandra.auth.PlainTextAuthProvider(USER, PASSWORD);
}

var systemClient = new cassandra.Client({contactPoints: [HOST], authProvider: authProvider});
var client = new cassandra.Client({ contactPoints: [HOST], keyspace: KEYSPACE, authProvider: authProvider});

function buildTableQueryForDataRow(tableInfo, row) {
    var queries = [];
    var isCounterTable = _.some(tableInfo.columns, function(column) {return column.type.code === 5;});
    row = _.omitBy(row, function(item) {return item === null});
    var query = 'INSERT INTO "' + tableInfo.name + '" ("' + _.keys(row).join('","') + '") VALUES (?' + _.repeat(',?', _.keys(row).length-1) + ')';
    var params = _.values(row);
    if (isCounterTable) {
        var primaryKeys = [];
        primaryKeys = primaryKeys.concat(_.map(tableInfo.partitionKeys, function(item){return item.name}));
        primaryKeys = primaryKeys.concat(_.map(tableInfo.clusteringKeys, function(item){return item.name}));
        var primaryKeyFields = _.pick(row, primaryKeys);
        var otherKeyFields = _.omit(row, primaryKeys);
        var setQueries = _.map(_.keys(otherKeyFields), function(key){
            return '"'+ key +'"="'+ key +'" + ?';
        });
        var whereQueries = _.map(_.keys(primaryKeyFields), function(key){
            return '"'+ key +'"=?';
        });
        query = 'UPDATE "' + tableInfo.name + '" SET ' + setQueries.join(', ') + ' WHERE ' + whereQueries.join(' AND ');
        params = _.values(otherKeyFields).concat(_.values(primaryKeyFields));
    }
    params = _.map(params, function(param){
        if (_.isPlainObject(param)) {
            return _.omitBy(param, function(item) {return item === null});
        }
        return param;
    });
    return {
        query: query,
        params: params,
    };
}

function processTableImport(table) {
    var rows = [];
    return new Promise(function(resolve, reject) {
        console.log('==================================================');
        console.log('Reading metadata for table: ' + table);
        systemClient.metadata.getTable(KEYSPACE, table)
            .then(function (tableInfo){
                if (!tableInfo) {
                    resolve();
                    return;
                }

                console.log('Creating read stream from: ' + table + '.json');
                var jsonfile = fs.createReadStream('data/' + table + '.json', {encoding: 'utf8'});
                var readStream = jsonfile.pipe(jsonStream.parse('*'));
                var queries = [];
                var chunkBatch = [];
                var processed = 0;
                readStream.on('data', function(row){
                    var query = buildTableQueryForDataRow(tableInfo, row);
                    queries.push(query);
                    processed++;
                    if (processed%1000 === 0) {
                        console.log('Streaming ' + processed + ' rows to table: ' + table);
                    }

                    if (queries.length === 10) {
                        chunkBatch.push(client.batch(queries, { prepare: true, logged: false }));
                        queries = [];
                        jsonfile.pause();
                        Promise.all(chunkBatch)
                            .then(function (){
                                chunkBatch = [];
                                jsonfile.resume();
                            })
                            .catch(function (err){
                                reject(err);
                            });
                    }

                });
                jsonfile.on('error', function (err) {
                    reject(err);
                });

                var startTime = Date.now();
                jsonfile.on('end', function () {
                    console.log('Streaming ' + processed + ' rows to table: ' + table);
                    if (queries.length > 1) {
                        chunkBatch.push(client.batch(queries, { prepare: true, logged: false }));
                    }
                    else if (queries.length === 1) {
                        chunkBatch.push(client.execute(queries[0].query, queries[0].params, { prepare: true }));
                    }

                    Promise.all(chunkBatch)
                        .then(function (){
                            var timeTaken = (Date.now() - startTime) / 1000;
                            var throughput = processed / timeTaken;
                            console.log('Done with table, throughput: ' + throughput.toFixed(1) + ' rows/s');
                            resolve();
                        })
                        .catch(function (err){
                            reject(err);
                        });
                });
            })
            .catch(function (err){
                reject(err);
            });
    });
}

systemClient.connect()
    .then(function (){
        var systemQuery = "SELECT columnfamily_name as table_name FROM system.schema_columnfamilies WHERE keyspace_name = ?";
        if (systemClient.metadata.keyspaces.system_schema) {
            systemQuery = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?";
        }

        console.log('Finding tables in keyspace: ' + KEYSPACE);
        return systemClient.execute(systemQuery, [KEYSPACE]);
    })
    .then(function (result){
        var tables = [];
        for(var i=0; i<result.rows.length; i++) {
            tables.push(result.rows[i].table_name);
        }

        if (process.env.TABLE) {
            return processTableImport(process.env.TABLE);
        }

        return Promise.each(tables, function(table){
            return processTableImport(table);
        });
    })
    .then(function (){
        console.log('==================================================');
        console.log('Completed importing all tables to keyspace: ' + KEYSPACE);
        systemClient.shutdown();
        client.shutdown();
    })
    .catch(function (err){
        console.log(err);
    });
