package lsm.wal;

import store.lsm.block.Block;
import store.lsm.block.impl.RmBlock;
import store.lsm.block.impl.StBlock;
import store.lsm.table.StructuredStringTable;
import org.junit.Test;

import java.io.IOException;
import java.util.TreeMap;

public class StructuredStringTableTest {
    @Test
    public void testTable() throws IOException {
        TreeMap<String, Block> index = new TreeMap<>();
        for (int i = 0; i < 10; i++) {
            StBlock stBlock = new StBlock("key" + i, "value" + i);
            index.put(stBlock.getKey(), stBlock);
        }
        index.put("key100", new StBlock("key100", "value100"));
        index.put("key100", new RmBlock("key100"));

        StructuredStringTable.createFromIndex("src/test/resources/lsm/test.txt", 3, index);
    }

    @Test
    public void testQuery() throws IOException {
        StructuredStringTable ssTable = StructuredStringTable.createFromFile("src/test/resources/lsm/test.txt");
        System.out.println(ssTable.query("key0"));
        System.out.println(ssTable.query("key5"));
        System.out.println(ssTable.query("key9"));
        System.out.println(ssTable.query("key100"));
    }
}
