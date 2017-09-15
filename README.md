# Cassandra Exporter

Cassandra exporter is a data export / import tool for cassandra that is simple to use and works for unicode and complex data types. It is developed in Javascript and the exported data is stored in JSON formatted files.

# Why another tool?

Cassandra has some great tools for exporting and importing data:

* snapshots
* sstable2json
* CQL's COPY FROM/TO

But the problem is snapshots and sstable2json are not that straight forward to use. They are intended for moving large data sets and to me unnecessarily complicated to use for day to day development.

The COPY command was intended for development or moving small datasets, but is not reliable. Because it uses csv exports which breaks for complex data types and non ascii encodings if you try to import that data. So for development purposes and for moving small datasets (< few million rows per table) I needed something that works robustly and is simple to use.

# Download

You can either download the compiled binary for your operating system from the [releases](https://github.com/masumsoft/cassandra-exporter/releases) section or if you have nodejs installed, you can use the source code directly to execute the export / import scripts.

# Usage (Compiled Binary)

## To export all table data from a keyspace

```
HOST=127.0.0.1 KEYSPACE=from_keyspace_name ./export
```

It will create exported json files in the data directory for each table in the keyspace.

## To import all table data into a keyspace

```
HOST=127.0.0.1 KEYSPACE=to_keyspace_name ./import
```

It will process all json files in the data directory and import them to corresponding tables in the keyspace.

## To export/import a single table in a keyspace

```
HOST=127.0.0.1 KEYSPACE=from_keyspace_name TABLE=my_table_name ./export

HOST=127.0.0.1 KEYSPACE=to_keyspace_name TABLE=my_table_name ./import
```

## To export/import using authentication

```
KEYSPACE=from_keyspace_name USERNAME=user1 PASSWORD=pa$$word ./export

KEYSPACE=to_keyspace_name USERNAME=user1 PASSWORD=pa$$word ./import
```

Please note that the user requires access to the system tables in order to work properly.


# Usage (from NodeJS)

If you already have nodejs installed in your system, then you can execute using the source directly like this:

```
HOST=127.0.0.1 KEYSPACE=from_keyspace_name TABLE=my_table_name node export.js

HOST=127.0.0.1 KEYSPACE=from_keyspace_name TABLE=my_table_name node import.js
```
# Usage (Docker)

The Dockerfiles provide a volume mounted at /data and expect the environment variables `HOST` and `KEYSPACE`. `Dockerfile.import` provides `import.js` functionality. `Dockerfile.export` provides `export.js` functionality. By using the -v option of `docker run` this provides the facility to store the output/input directory in an arbitrary location. It also allows running cassandra-export from any location. This requires [Docker](https://www.docker.com/) to be installed.

# Note

Cassandra exporter only export / import data. It expects the tables to be present beforehand. If you need to also export schema and the indexes, then you could easily use cqlsh and the source command to export / import the schema before moving the data.

```
// To export keyspace schema, use cqlsh like this
cqlsh -e "DESC KEYSPACE mykeyspace" > my_keyspace_schema.cql

// To import keyspace schema open the cqlsh shell
// in the same directory of `my_keyspace_schema.cql`, then
source 'my_keyspace_schema.cql'
```
