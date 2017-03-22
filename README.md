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
