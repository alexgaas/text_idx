package lsm.table;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import store.lsm.block.impl.StBlock;
import store.lsm.index.Index;
import store.lsm.table.StructuredStringTable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StructuredStringTableTest {
    private static final String baseTestPath = "./src/test/resources/sstable/";
    private static final String fileName = String.valueOf(System.currentTimeMillis());
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
    public void removeTestTable() {
        Arrays.stream(Objects.requireNonNull(new File(baseTestPath).listFiles())).forEach(File::delete);
    }

    @Test
    public void testCreateFromIndex() throws IOException {
        String key = "key4";
        var result = table.query(key);
        assertEquals(key, result.getKey());
    }

    @Test
    public void testCreateFromFile() throws IOException {
        StructuredStringTable tableFromFile = StructuredStringTable.createFromFile(table.GetFilePath());
        String key = "key4";
        var result = tableFromFile.query(key);
        assertEquals(key, result.getKey());
    }
}
