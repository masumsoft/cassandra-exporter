var _ = require('lodash');
var Promise = require('bluebird');
var cassandra = require('cassandra-driver');
var fs = require('fs');
var jsonStream = require('JSONStream');

var HOST = process.env.HOST || '127.0.0.1';
var PORT = process.env.PORT || 9042;
var KEYSPACE = process.env.KEYSPACE;

if (!KEYSPACE) {
    console.log('`KEYSPACE` must be specified as environment variable');
    process.exit();
}

var USER = process.env.USER;
var PASSWORD = process.env.PASSWORD;
var DIRECTORY = process.env.DIRECTORY || "./data";
var USE_SSL = process.env.USE_SSL;

var authProvider;
if (USER && PASSWORD) {
    authProvider = new cassandra.auth.PlainTextAuthProvider(USER, PASSWORD);
}

var sslOptions;
if (USE_SSL) {
    sslOptions = { rejectUnauthorized: false };
}

var systemClient = new cassandra.Client({contactPoints: [HOST], authProvider: authProvider, protocolOptions: {port: [PORT]}, sslOptions: sslOptions });
var client = new cassandra.Client({ contactPoints: [HOST], keyspace: KEYSPACE, authProvider: authProvider, protocolOptions: {port: [PORT]}, sslOptions: sslOptions });

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
            if (param.type === 'Buffer') {
                return Buffer.from(param);
            } else if (param.type === 'NOT_A_NUMBER') {
                return Number.NaN;
            } else if (param.type === 'POSITIVE_INFINITY') {
                return Number.POSITIVE_INFINITY;
            } else if (param.type === 'NEGATIVE_INFINITY') {
                return Number.NEGATIVE_INFINITY;
            } else {
                var omittedParams = _.omitBy(param, function(item) {return item === null});
                for (key in omittedParams) {
                    if (_.isObject(omittedParams[key]) && omittedParams[key].type === 'Buffer') {
                        omittedParams[key] = Buffer.from(omittedParams[key]);
                    }
                }
                return omittedParams;
            }
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
                var jsonfile = fs.createReadStream(DIRECTORY + '/' + table + '.json', {encoding: 'utf8'});
                var readStream = jsonfile.pipe(jsonStream.parse('*'));
                var queryPromises = [];
                var processed = 0;
                readStream.on('data', function(row){
                    var query = buildTableQueryForDataRow(tableInfo, row);
                    queryPromises.push(client.execute(query.query, query.params, { prepare: true}));
                    processed++;

                    if (processed%1000 === 0) {
                        console.log('Streaming ' + processed + ' rows to table: ' + table);
                        jsonfile.pause();
                        Promise.all(queryPromises)
                            .then(function (){
                                queryPromises = [];
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
                    Promise.all(queryPromises)
                        .then(function (){
                            var timeTaken = (Date.now() - startTime) / 1000;
                            var throughput = timeTaken ? processed / timeTaken : 0.00;
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
        var gracefulShutdown = [];
        gracefulShutdown.push(systemClient.shutdown());
        gracefulShutdown.push(client.shutdown());
        Promise.all(gracefulShutdown)
            .then(function (){
                console.log('Completed importing to keyspace: ' + KEYSPACE);
            })
            .catch(function (err){
                console.log(err);
            });
    })
    .catch(function (err){
        console.log(err);
    });
