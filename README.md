# text_idx
The `text_idx` project offers an efficient Java-based implementation of two key functionalities:

- `Sparse Indexing`: It provides a mechanism for creating sparse indexes using N structured files, 
facilitating effective search operations based on these indexes. This approach allows for efficient retrieval of data 
based on specific criteria, enhancing the performance of search operations.
- `Log-Structured Trees (LST)`: The project also includes a log-structured tree implemented within a structured string table, 
along with a write-ahead log. This architecture enables efficient storage and retrieval of data, with changes logged 
in a structured manner to ensure data integrity and consistency.

### Implementation
The `text_idx` project API and data flows are presented below:

#### `put` / `remove` API flow:
<img src="./plots/LSM_put_kv.png" alt="">

#### `get` API flow:
<img src="./plots/LSM_get_kv.png" alt="">

#### `put` / `get` data flow:
<img src="./plots/LSM_flow.png" alt="">

Implementation includes:

- **Block**: Located at src/main/java/store/lsm/block, the Block module serves as the fundamental unit for data and operations. 
Block operations are defined by the semantics of the Block class, utilizing operations such as `St` (Store) and `Rm` (Remove).
- **Index**: The index API, found at `src/main/java/store/lsm/index`, encompasses the definition of both index 
and sparse index structures. Notably, it includes an implementation for sparse index queries. For detailed insights into 
sparse index functionality, refer to the `src/main/java/store/lsm/index/SparseIndexQuery.java` class, and for a 
comprehensive understanding, explore `src/test/java/lsm/index/SparseIndexTest.java`.
- **Structured string table**: The table module provides an API for structured string tables. Each table consists of table metadata, 
defined in `src/main/java/store/lsm/table/TableMetaData.java`, and `N` segments containing serialized blocks indexed appropriately. 
The entire `sstable` implementation is encapsulated within `src/main/java/store/lsm/table/StructuredStringTable.java`.
- **Write Ahead Log (WAL)**: The wal module is responsible for implementing the Write Ahead Log concept, as described in 
write ahead logging. This functionality is realized in `src/main/java/store/lsm/wal/WriteAheadLog.java`.
- **LSM (Log-Structured Tree)**: The lsm module serves as the developer API, residing in `src/main/java/store/lsm/Lsm.java`. 
It facilitates the CRUD operations for key/value entities and manages the orchestration logic between the Write Ahead Log 
and the structured string files to ensure efficient CRUD operations.

### Results
Developer API and results shown in the unit test 
[LsmTest.java](https://github.com/alexgaas/text_idx/blob/4b60ce1ee2f15a0b2d309917737d63314628a58e/src/test/java/lsm/LsmTest.java)

**Developer API** example:
```text
try(Store lsm = new Lsm(baseTestPath, 4, 3)) {
    // put test data
    for (int i1 = 0; i1 < 10; i1++) {
        lsm.put(String.valueOf(i1), String.valueOf(i1));
    }
    // assert data in the LSM store
    for (int i1 = 0; i1 < 10; i1++) {
        assertEquals(String.valueOf(i1), lsm.get(String.valueOf(i1)));
    }
    // remove data from LSM store
    for (int i1 = 0; i1 < 10; i1++) {
        String s = String.valueOf(i1);
        lsm.remove(s);
    }
    // assert data have been removed
    for (int i = 0; i < 10; i++) {
        String s = lsm.get(String.valueOf(i));
        Assertions.assertNull(s);
    }
}
```

### TODO
- `merge` phase is not implemented yet
- `serde` (serialization / deserialization) level is not separated from LSM. It is though mixed up -
I used manual byte serialization / deserialization for write ahead log and `ObjectMapper` for string structured tables
- `text_idx` uses `RandomAccessFile` for most of IO operations. This IO API in fact is
deprecated and have to be replaced with `FileChannel`. Details in this article -
  https://github.com/alexgaas/java_file_io
- there is no `BloomFilter`
- no effective benchmark tests
- to run unit tests successfully following folders in `src/test/resources` must be created before run:
`lsm`, `metadata`, `sparse_index`, `sstable`, `wal`

### License
MIT - https://github.com/git/git-scm.com/blob/main/MIT-LICENSE.txt