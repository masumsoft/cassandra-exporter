Cassandra exporter is a data export / import tool for cassandra that is simple to use and works for unicode and complex data types. It is developed in Javascript and the exported data is stored in JSON formatted files.

# Why another tool?

Cassandra has some great tools for exporting and importing data:

* snapshots
* sstable2json
* CQL's COPY FROM/TO

But the problem is snapshots and sstable2json are not that straight forward to use. They are intended for moving large data sets and to me unnecessarily complicated to use for day to day development.

The COPY command was intended for development or moving small datasets, but is not reliable. Because it uses csv exports which breaks for complex data types and non ascii encodings if you try to import that data. So for development purposes and for moving small datasets (< few million rows per table) I needed something that works robustly and is simple to use.

# Usage

## To export all table data from a keyspace

```
HOST=127.0.0.1 KEYSPACE=from_keyspace_name node export.js
```

It will create exported json files in the data directory for each table in the keyspace.

## To import all table data into a keyspace

```
HOST=127.0.0.1 KEYSPACE=to_keyspace_name node import.js
```

It will process all json files in the data directory and import them to corresponding tables in the keyspace.

## To export/import a single table in a keyspace

```
HOST=127.0.0.1 KEYSPACE=from_keyspace_name TABLE=my_table_name node export.js

HOST=127.0.0.1 KEYSPACE=to_keyspace_name TABLE=my_table_name node import.js
```

## To export/import using authentication

```
KEYSPACE=from_keyspace_name USERNAME=user1 PASSWORD=pa$$word node export.js

KEYSPACE=to_keyspace_name USERNAME=user1 PASSWORD=pa$$word node import.js
```

Please note that the user requires access to the system tables in order to work properly.

