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
- `block` module (`src/main/java/store/lsm/block`). `Block` is minimal unit of data/operation. Block operation is defined by semantic of block class (`St` / `Rm`).
- `index` API (`src/main/java/store/lsm/index`) includes definition of index and sparse index. More important is it have a sparse index query implementation.
For details please see `src/main/java/store/lsm/index/SparseIndexQuery.java` class. Full explanation of how sparse index works please see 
`src/test/java/lsm/index/SparseIndexTest.java`.
- `table` (structured string table) API. Every table must have table metadata (`src/main/java/store/lsm/table/TableMetaData.java`) and N
segments (serialized blocks by index). Whole implementation of `sstable` in the `src/main/java/store/lsm/table/StructuredStringTable.java`.
- `wal` implements [write ahead log](https://en.wikipedia.org/wiki/Write-ahead_logging):
`src/main/java/store/lsm/wal/WriteAheadLog.java`.
- `lsm` implements developer API (`src/main/java/store/lsm/Lsm.java`) to CRUD key/value entities and orchestration logic between
write ahead log and set of structured string files for effective CRUD operations.

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