# benchtop

Benchtop is a framework for storing large JSON documents as JSON blobs directly to disk with indexing provided by the key value database PebbleDb.

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

Data is stored in a large binary files and indexed using [Pebble Key Value storage](https://github.com/cockroachdb/pebble).

### Key/Value format

Written using [Pebble](https://github.com/cockroachdb/)

#### Table Entries

Benchtop KV Store Key Structure

This document outlines the binary key structure used by the benchtop package for storing and indexing data in a key-value (KV) store like PebbleDB. The structure is designed for efficient lookups, scans, and indexing of tabular or graph-like data by leveraging key prefixes and a consistent binary layout.
Core Concepts

1. Key Prefixes

All keys begin with a single-byte prefix to denote the type of data they represent. This allows different types of data to coexist in the same keyspace and enables efficient prefix scans (e.g., "find all position keys").

    T (TablePrefix): Keys related to table metadata.

    P (PosPrefix): Keys that map a row ID to its physical location.

    F (FieldPrefix): Keys that form a secondary index on specific field values.

    R (RFieldPrefix): Keys that form a reverse index for efficient index deletion.

2. Field Separator

A special byte separator, FieldSep (ASCII 0x1F - Unit Separator), is used as a delimiter within compound keys (like the field indexes). This character is chosen because it is a non-printable control character that is not expected to appear in standard string data, ensuring reliable splitting of key components.
Key Types

1.  Table Keys

    Purpose: To store metadata or identifiers for data tables.

    Structure: T | TableId

        T: The literal character 'T' (TablePrefix).

        TableId: The unique byte slice identifier for the table.

    Functions:

        NewTableKey(id []byte): Creates a new table key.

        ParseTableKey(key []byte): Extracts the TableId from a table key.

2.  Position (Row Location) Keys

    Purpose: These keys are the primary index, mapping a unique row/vertex ID to its physical location (offset and size) in a data file.

    Structure: P | TableId | RowId

        P: The literal character 'P' (PosPrefix).

        TableId: A 2-byte uint16 (little-endian) identifying the table the row belongs to.

        RowId: The unique byte slice identifier for the row/vertex.

    Associated Value: The value stored for this key is an encoded RowLoc struct (see below).

    Functions:

        NewPosKey(table uint16, name []byte): Creates a new position key.

        ParsePosKey(key []byte): Extracts the TableId and RowId from a key.

        NewPosKeyPrefix(table uint16): Creates a key prefix for scanning all rows within a specific table.

3.  Field Index Keys

    Purpose: To create a secondary index on specific field values. This allows for fast lookups of all rows that have a certain value for a given field (e.g., find all users where city == 'New York').

    Structure: F<sep>Field<sep>Label<sep>Value<sep>RowId

        F: The literal character 'F' (FieldPrefix).

        <sep>: The FieldSep byte.

        Field: The name of the indexed field (e.g., "city").

        Label: The label or type of the row (e.g., "user").

        Value: The JSON-encoded value of the field (e.g., "New York").

        RowId: The unique ID of the row that contains this field value.

    Functions:

        FieldKey(field, label string, value any, rowID []byte): Creates a full field index key.

        FieldKeyParse(key []byte): Parses a field key back into its components.

        FieldLabelKey(field, label string): Creates a key prefix for scanning all indexed values for a specific field and label.

4.  Reverse Field Index Keys

    Purpose: To enable the efficient deletion of a row's entries from the field indexes. When a row is deleted, this reverse index is used to quickly find all the Field Index Keys that point to it, without having to scan the entire index.

    Structure: R<sep>Label<sep>Field<sep>RowId

        R: The literal character 'R' (RFieldPrefix).

        <sep>: The FieldSep byte.

        Label: The label of the row.

        Field: The name of the indexed field.

        RowId: The unique ID of the row.

    Functions:

        RFieldKey(label, field, rowID string): Creates a new reverse field key.

Value Structures
RowLoc

    Purpose: Represents the physical location of a data record, acting as a "pointer" to the full data object stored elsewhere. It is the value component for a Position Key.

    Structure: A fixed 10-byte binary layout.

        Section (Bytes 0-1): A uint16 identifying the file or section where the data is stored.

        Offset (Bytes 2-5): A uint32 representing the starting byte offset within the section.

        Size (Bytes 6-9): A uint32 representing the length of the data in bytes.

    Functions:

        EncodeRowLoc(loc *RowLoc): Encodes a RowLoc struct into a 10-byte slice.

        DecodeRowLoc(v []byte): Decodes a 10-byte slice back into a RowLoc struct.
