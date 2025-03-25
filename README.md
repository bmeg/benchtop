# benchtop

## Command line

Build:

```
make
```

### Load data

```
benchtop load test.data embeddings test.ndjson
```

- `test.data` : name of archive
- `embeddings` : name of table
- `test.ndjson` : file to be loaded

### List tables

```
benchtop tables test.data
```

### Get keys

```
benchtop keys test.data embeddings
```

### Get records

```
benchtop get test.data embeddings <key1> <key2> ...
```

## Format

Data is stored in a large binary files and index using [Pebble Key Value storage](https://github.com/cockroachdb/pebble).

### Key/Value format

Written using [Pebble](https://github.com/cockroachdb/)

#### Table Entries

**Key**
|bytes|0|5:... |
|-|-|---------|
|type|t|<[]byte> |
|Desc|prefix|user ID|

The user ID is provided by the user, but should be checked to ensure it is unique.

**Value**
|bytes|0:4|4:...|
|-|-|-------|
|type|[]byte|
|Desc|BSON formatted Column definitions|

First is the Table system ID, which is used as a prefix during key lookup. Then rest
of the bytes describe a list of columns and their data types.

#### Table ID

**Key**
|bytes|0|5:... |
|-|-|---------|
|type|T|uint32|
|Desc|prefix|system table ID|

The generated ID for a table.

**Value**
|bytes|0:4|4:...|
|-|-|-------|
|type|[]byte|
|Desc|User ID of table|

#### ID Entries

These map the user specified ID to a data block specified with offset and size.

**Key**
|bytes|0|1:5|1:... |
|-|-|-|--------|
|type|k|uint32|<[]byte> |
|Desc|prefix|system table ID|user row ID|

**Value**
|bytes|0:8|8:16|
|-|-|---------|
|type|uint64|uint64|
|Desc|offset|size|

### Data file format

Sequentially written [BSON](https://bsonspec.org/) entries.
