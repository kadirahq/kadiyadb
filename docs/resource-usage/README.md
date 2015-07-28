# KadiraDB Resource Usage

KadiraDB is a time series database designed for high write throughput and for fast reads for series of data items. KadiraDB is a low level database which can be embedded into any Go program. An example use case for KadiraDB is "KadiraDB for Metrics" which stores time series of numeric metrics and provides client libraries for Go and NodeJS and user interfaces for database administrators.



## Disk Space Requirements

The most important detail about disk space requirement is that **KadiraDB is designed to be used with SSD**. Database writes are done in random locations on the disk which does not perform well when done on spinning disks. As SSDs are becoming the de-facto storage medium for databases and as it supports write operations on random locations better, it has been chosen for KadiraDB storage. All code optimisations are done only targeting SSDs.

Disk space is required for storing block data and index data. Block data is usually more significant to total storage requirement. On disk KadiraDB consists of block files, index files and metadata files. Compared to block files and index files, storage space required for metadata files is negligible. Databases also have a retention option which controls how long the data is kept in the database. This can also be useful for controlling disk space requirements.

Database files are first divided into **epochs**. Being divided into epochs makes it possible to delete older epochs without affecting other database operations. Directory structure of a database will be similar to the one given below:

``` shell
❯ ls -lRh
total 0
drwxr-xr-x  4 user  group   136B Jul 27 17:44 test

./test:
total 8
drwxr-xr-x  11 user  group   374B Jul 27 17:44 epoch_1437998400000000000
-rw-r--r--   1 user  group    45B Jul 27 17:35 metadata

./test/epoch_1437998400000000000:
total 1771528
-rw-r--r--  1 user  group    25M Jul 27 17:44 index
-rw-r--r--  1 user  group    14B Jul 27 17:44 metadata
-rw-r--r--  1 user  group   120M Jul 27 17:44 seg_0
-rw-r--r--  1 user  group   120M Jul 27 17:44 seg_1
-rw-r--r--  1 user  group   120M Jul 27 17:44 seg_2
-rw-r--r--  1 user  group   120M Jul 27 17:44 seg_3
-rw-r--r--  1 user  group   120M Jul 27 17:44 seg_4
-rw-r--r--  1 user  group   120M Jul 27 17:44 seg_5
-rw-r--r--  1 user  group   120M Jul 27 17:44 seg_6
```

The above example is a test database created using "KadiraDB for Metrics". This example shows a database with a single epoch. The epoch has one index file (25MB) and 7 segment files (120MB each). How disk space is allocated for these files are explained in later sections.



### Disk Space Required for Block Data

Total storage space for block storage can be calculated using payload size, number of records, time range and the resolution. The duration of an epoch can also affect the total storage cost. Total disk space required for a block can be calculated by:

``` 
record_size = payloads_per_record * payload_size
segment_size = records_per_segment * record_size
segment_count = ceil((record_count + PreallocThreshold)/segment_size)
block_size = segment_count * segment_size
```

Disk space allocation is done in segments therefore having a high number for **records per segment** parameter can lead to more unused space. Having a smaller number for **records per segment** is not suitable for databases with large number of segments because it increases the number of open file handlers. Segments are pre allocated when the available space for new records goes below **PreallocThresh** which is set to 1000 records by default.

With a good estimate on the number of different records is enough to predict the total disk space the database server will need. It can be calculated by:

``` 
total_size = block_size * (blocks_kept_by_retention + 1)
```



### Disk Space Required for Index Data

Index data requirements can't be calculated exactly because index fields are made with variable length strings. Index elements are encoded using protocol buffers and appended to a file. An uint32 header is also added which has the encoded data size. Index elements are stored in this protocol buffer format.

``` 
message Item {
  repeated string fields = 1;
  uint32 value = 2;
}
```

Following examples can be helpful to get a general idea about index space requirements.

| Number of fields | Average field size | Average Item size | Items per 25MB | 
| ---------------- | ------------------ | ----------------- | -------------- | 
| 0                | 10                 | 4 + 2 bytes       | 4,369,066      | 
| 1                | 10                 | 4 + 14 bytes      | 1,456,355      | 
| 1                | 50                 | 4 + 54 bytes      | 451,972        | 
| 10               | 10                 | 4 + 122 bytes     | 208,050        | 
| 10               | 50                 | 4 + 522 bytes     | 49,837         | 

Index files grow by **PreallocSize**, 25 MB by default when the remaining space goes below **PreallocThresh** which is set to 5MB by default.



## Main Memory Usage

KadiraDB uses memory mapping in order to read and write data faster. When a database is loaded, a number of latest epochs will be loaded in read-write mode. When a request is received, older epochs will be loaded in read-only mode. The number of read-write and read-only epochs can be configured per database. Among these epoch types, read-write epochs use memory mapping to speed up operations and therefore they require more memory.



### Memory Required for Read-Write Epochs

The latest **MaxRWEpochs** number of epochs are considered read-write epochs where **MaxRWEpochs** is configurable. Only read-write epochs accept write operations. In most cases, most of the read operations are also sent to latest epochs. In order to respond faster, read-write epochs memory map block data. Index is also memory mapped in order to speed up writing index entries. Total memory requirement can be estimated by by:

``` 
total_memory = block_mmap_size + index_mmap_size + index_tree_size
```

These are the most significant facts affecting memory requirement. Both **block_mmap_size** and **index_mmap_size** are equal to the total size of files used for them. The **index_tree_size** is the in memory tree used to query the index. The worst case scenario is when all fields are unique with each index entry. If this happens the memory requirements would be similar to the following example. In real-world situations the index tree size will be significantly smaller than these numbers.

| Number of fields | Average field size | Number of index items | Index tree size | 
| ---------------- | ------------------ | --------------------- | --------------- | 
| 5                | 10                 | 0                     | 488             | 
| 5                | 10                 | 1                     | 2,216           | 
| 5                | 50                 | 1                     | 2,456           | 
| 5                | 50                 | 10                    | 16,104          | 
| 5                | 50                 | 10                    | 18,504          | 



### Memory Require for Read-Only Epochs

Older epochs are loaded in read-only mode. Read only epochs don't use memory mapping therefore the memory is only required for the in-memory index tree. The above table shows the maximum memory required for given parameters.