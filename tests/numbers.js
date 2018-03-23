var Promise = require('bluebird');
var cassandra = require('cassandra-driver');
var fs = require('fs');
var jsonStream = require('JSONStream');
var cp = require('child_process')

var HOST = process.env.HOST || '127.0.0.1';
var PORT = process.env.PORT || 9042;

var USER = process.env.USER;
var PASSWORD = process.env.PASSWORD;

var authProvider;

if (USER && PASSWORD) {
    authProvider = new cassandra.auth.PlainTextAuthProvider(USER, PASSWORD);
}

var systemClient = new cassandra.Client({ contactPoints: [HOST], authProvider: authProvider, protocolOptions: { port: [PORT] } });
var client = new cassandra.Client({ contactPoints: [HOST], authProvider: authProvider, protocolOptions: { port: [PORT] } });

async function testIt() {
    try {
        await client.execute("CREATE KEYSPACE IF NOT EXISTS NumbersTest WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }");
        await client.execute("CREATE TABLE IF NOT EXISTS NumbersTest.TestTable(k TEXT PRIMARY KEY, v DOUBLE)")
        await client.execute("INSERT INTO NumbersTest.TestTable(k, v) VALUES('NOT_A_NUMBER', NaN)");
        await client.execute("INSERT INTO NumbersTest.TestTable(k, v) VALUES('ZERO', 0)");
        await client.execute("INSERT INTO NumbersTest.TestTable(k, v) VALUES('THIRTEEN_DOT_FOUR', 13.4)");
        await client.execute("INSERT INTO NumbersTest.TestTable(k, v) VALUES('NEGATIVE_INFINITY', -Infinity)");
        await client.execute("INSERT INTO NumbersTest.TestTable(k, v) VALUES('POSITIVE_INFINITY', Infinity)");

        process.env.KEYSPACE = 'numberstest';

        cp.execSync('node export.js')

        const resultSnapshot = fs.readFileSync('data/testtable.json', { encoding: 'utf-8' });

        if (resultSnapshot !== '[{"k":"NOT_A_NUMBER","v":{"type":"NOT_A_NUMBER"}},{"k":"NEGATIVE_INFINITY","v":{"type":"NEGATIVE_INFINITY"}},{"k":"THIRTEEN_DOT_FOUR","v":13.4},{"k":"POSITIVE_INFINITY","v":{"type":"POSITIVE_INFINITY"}},{"k":"ZERO","v":0}]') {
            throw Error('Snapshot ' + resultSnapshot + ' does not matched expected return value');
        }

        await client.execute('TRUNCATE NumbersTest.TestTable');

        cp.execSync('node import.js')

        const rs = await client.execute('SELECT * FROM NumbersTest.TestTable');

        const expected = [
            ['NOT_A_NUMBER', Number.NaN],
            ['NEGATIVE_INFINITY', Number.NEGATIVE_INFINITY],
            ['THIRTEEN_DOT_FOUR', 13.4],
            ['POSITIVE_INFINITY', Number.POSITIVE_INFINITY],
            ['ZERO', 0]
        ]

        rs.rows.forEach((row, i) => {
            row.values().forEach((value, j) => {
                if (expected[i][j] != value && !(Number.isNaN(expected[i][j]) && Number.isNaN(value))) {
                    throw Error('Expected value ' + expected[i][j] + ' but was ' + value)
                }
            })
        })

        console.info('PASS');
    } catch (e) {
        console.error('FAIL', e);
    } finally {
        await Promise.all([systemClient.shutdown(), client.shutdown()])
    }
}

testIt()
