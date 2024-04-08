package lsm.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import store.lsm.block.impl.StBlock;
import store.lsm.index.Index;
import store.lsm.index.SparseIndex;
import store.lsm.index.SparseIndexQuery;
import store.lsm.table.StructuredStringTable;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SparseIndexIntegrationTest {
    private static final String baseTestPath = "./src/test/resources/sparse_index/";
    private StructuredStringTable table;

    @BeforeEach
    public void prepareTestTable() throws IOException {
        Index index = new Index();
        for (int i = 0; i < 9; i++) {
            StBlock stBlock = new StBlock(String.format("key%d", i), String.format("value%d", i));
            index.put(stBlock.getKey(), stBlock);
        }
        /*
        let's make segment size as 3
            1 - key0 | key1 | key2 |
            2 - key3 | key4 | key5 |
            3 - key6 | key7 | key8 |
         */
        table = StructuredStringTable.createFromIndex(baseTestPath, 3, index);
    }

    @AfterEach
    public void removeTestTable(){
        Arrays.stream(Objects.requireNonNull(new File(baseTestPath).listFiles())).forEach(File::delete);
    }

    @Test
    public void testQueryByKey() throws IOException {
        SparseIndex sparseIndex = table.GetSparseIndex();
        RandomAccessFile tableFile = table.GetTableFile();
        SparseIndexQuery idxQuery = new SparseIndexQuery(sparseIndex, tableFile);

        /* let's find [key4]

            for k in [key0, key3, key6]
             if k compareTo key <= 0
                fill lastSmallPosition
             else
                fill firstBigPosition

            1: latestMinimalPosition = key0, earliestMaximalPosition = null
            2: latestMinimalPosition = key3, earliestMaximalPosition = null
            3: latestMinimalPosition = key3, earliestMaximalPosition = key6
         */

        String key = "key4";
        var result = idxQuery.queryByKey(key);

        assertEquals(key, result.getKey());

    }
}
