var _ = require('lodash');
var Promise = require('bluebird');
var cassandra = require('cassandra-driver');
var jsonfile = Promise.promisifyAll(require('jsonfile'));

var HOST = process.env.HOST || '127.0.0.1';
var KEYSPACE = process.env.KEYSPACE;

if (!KEYSPACE) {
    console.log('`KEYSPACE` must be specified as environment variable');
    process.exit();
}

var systemClient = new cassandra.Client({contactPoints: [HOST]});
var client = new cassandra.Client({ contactPoints: [HOST], keyspace: KEYSPACE});

function buildTableQueriesForDataRows(tableInfo, rows) {
    var queries = [];
    var isCounterTable = _.some(tableInfo.columns, function(column) {return column.type.code === 5;});
    for(var i = 0; i < rows.length; i++) {
        rows[i] = _.omitBy(rows[i], function(item) {return item === null});
        var query = 'INSERT INTO "' + tableInfo.name + '" ("' + _.keys(rows[i]).join('","') + '") VALUES (?' + _.repeat(',?', _.keys(rows[i]).length-1) + ')';
        var params = _.values(rows[i]);
        if (isCounterTable) {
            var primaryKeys = [];
            primaryKeys = primaryKeys.concat(_.map(tableInfo.partitionKeys, function(item){return item.name}));
            primaryKeys = primaryKeys.concat(_.map(tableInfo.clusteringKeys, function(item){return item.name}));
            var primaryKeyFields = _.pick(rows[i], primaryKeys);
            var otherKeyFields = _.omit(rows[i], primaryKeys);
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
        queries.push({
            query: query,
            params: params,
        });
    }
    return queries;
}

function writeBatch(chunkQuery) {
    var chunkBatch = [];
    for (var i = 0; i < chunkQuery.length; i++) {
        chunkBatch.push(client.execute(chunkQuery[i].query, chunkQuery[i].params, { prepare: true }));
    }
    return Promise.all(chunkBatch);
}

function processTableImport(table) {
    var rows = [];
    return new Promise(function(resolve, reject) {
        console.log('==================================================');
        console.log('Reading all rows from ' + table + '.json...');
        jsonfile.readFileAsync('data/' + table + '.json')
            .then(function (tableRows){
                rows = tableRows;
                if (rows.length === 0) {
                    return false;
                }
                console.log('Reading metadata for table: ' + table);
                return systemClient.metadata.getTable(KEYSPACE, table);
            })
            .then(function (tableInfo){
                if (!tableInfo) {
                    resolve();
                    return;
                }

                console.log('Building queries for data import on table: ' + table);
                var queries = buildTableQueriesForDataRows(tableInfo, rows);

                console.log('Executing import batches on table: ' + table);

                var chunkedQueries = _.chunk(queries, 10);
                var progress = 0;
                return Promise.each(chunkedQueries, function(chunkQuery){
                    if (progress > 0 && progress%1000 === 0) {
                        console.log('Written ' + progress + '/' + queries.length + ' rows to table: ' + table);
                    }
                    progress += chunkQuery.length;
                    return writeBatch(chunkQuery);
                });
            })
            .then(function (){
                console.log('Written all rows to table: ' + table);
                resolve();
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

        console.log('Getting tables from keyspace: ' + KEYSPACE);
        return systemClient.execute(systemQuery, [KEYSPACE]);
    })
    .then(function (result){
        var tables = [];
        for(var i=0; i<result.rows.length; i++) {
            tables.push(result.rows[i].table_name);
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
