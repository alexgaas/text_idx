package lsm.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import store.lsm.block.impl.BlockOperation;
import store.lsm.block.impl.StBlock;
import store.lsm.index.Index;
import store.lsm.index.IndexPosition;
import store.lsm.index.SparseIndex;
import store.lsm.index.SparseIndexQuery;
import store.lsm.table.StructuredStringTable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

/*
Example of .table file:

{
	"0":"{\"type\":\"SET\",\"key\":\"0\",\"value\":\"value0\"}",
	"1":"{\"type\":\"SET\",\"key\":\"1\",\"value\":\"value1\"}",
	"10":"{\"type\":\"SET\",\"key\":\"10\",\"value\":\"value10\"}"
}
{
	"11":"{\"type\":\"SET\",\"key\":\"11\",\"value\":\"value11\"}",
	"12":"{\"type\":\"SET\",\"key\":\"12\",\"value\":\"value12\"}",
	"13":"{\"type\":\"SET\",\"key\":\"13\",\"value\":\"value13\"}"
}
{
	"14":"{\"type\":\"SET\",\"key\":\"14\",\"value\":\"value14\"}",
	"2":"{\"type\":\"SET\",\"key\":\"2\",\"value\":\"value2\"}",
	"3":"{\"type\":\"SET\",\"key\":\"3\",\"value\":\"value3\"}"
}
{
	"4":"{\"type\":\"SET\",\"key\":\"4\",\"value\":\"value4\"}",
	"5":"{\"type\":\"SET\",\"key\":\"5\",\"value\":\"value5\"}",
	"6":"{\"type\":\"SET\",\"key\":\"6\",\"value\":\"value6\"}"
}
{
	"7":"{\"type\":\"SET\",\"key\":\"7\",\"value\":\"value7\"}",
	"8":"{\"type\":\"SET\",\"key\":\"8\",\"value\":\"value8\"}",
	"9":"{\"type\":\"SET\",\"key\":\"9\",\"value\":\"value9\"}"
}
{
	"0":{"start":0,"len":184},
	"11":{"start":184,"len":190},
	"14":{"start":374,"len":184},
	"4":{"start":558,"len":181},
	"7":{"start":739,"len":181}
}

As you may see:
"0":{"start":0,"len":184} referencing to ->
{
	"0":"{\"type\":\"SET\",\"key\":\"0\",\"value\":\"value0\"}",
	"1":"{\"type\":\"SET\",\"key\":\"1\",\"value\":\"value1\"}",
	"10":"{\"type\":\"SET\",\"key\":\"10\",\"value\":\"value10\"}"
}
 */
public class SparseIndexTest {
    private static final String baseTestPath = "./src/test/resources/sparse_index/";
    @Test
    public void testSetPositionList(){
        SparseIndex index = new SparseIndex();
        // 0, 3, 6
        index.put("0", new IndexPosition(0, 4));
        index.put("3", new IndexPosition(4, 4));
        index.put("6", new IndexPosition(8, 4));
        // 9, 12, 15
        index.put("9", new IndexPosition(12, 4));
        index.put("12", new IndexPosition(16, 4));
        index.put("15", new IndexPosition(20, 4));

        /*
            sparse index:
            0 - [], ..., 3  - [], ..., 6  - [], ...,
                                   4^            8^
            9 - [], ..., 12 - [], ..., 15 - []
         */
        SparseIndexQuery query = new SparseIndexQuery(index, null);
        query.setPositionListByKey("4"); // 0 - [], ..., 3  - []
        query.setPositionListByKey("8"); // 3  - [], ..., 6  - []

        Assertions.assertNotNull(query.GetSparseKeyPositionList());
        assertEquals(query.GetSparseKeyPositionList().getFirst(), index.get("3"));
        assertEquals(query.GetSparseKeyPositionList().getLast(), index.get("9"));
    }

    @AfterEach
    public void removeTestTable(){
        Arrays.stream(Objects.requireNonNull(new File(baseTestPath).listFiles())).forEach(File::delete);
    }

    @Test
    public void getSegmentBySparseIndex() throws IOException {
        // setup any key for validation
        String key = "8";
        StBlock expectedBlock = new StBlock("8", "value8");
        // prepare test table
        StructuredStringTable ssTable = prepareTestTable();
        SparseIndexQuery query = new SparseIndexQuery(ssTable.GetSparseIndex(), ssTable.GetTableFile());
        query.queryByKey(key);

        byte[] result = query.getSegmentBySparseIndex();
        Assertions.assertNotNull(result);

        int start = 0;
        ObjectMapper mapper = new ObjectMapper();

        // query.GetSparseKeyPositionList() - "7":{"start":739,"len":181}
        // run through sparse index and read segment json
        for (IndexPosition position : query.GetSparseKeyPositionList()) {
            // get segmented json as [ObjectNode]
            ObjectNode segmentJson = (ObjectNode) mapper.readTree(result, start, (int)position.len);
            // if key matches segment -> return
            /*
            segmentJson:
            {
                "7":"{\"type\":\"SET\",\"key\":\"7\",\"value\":\"value7\"}",
                "8":"{\"type\":\"SET\",\"key\":\"8\",\"value\":\"value8\"}",
                "9":"{\"type\":\"SET\",\"key\":\"9\",\"value\":\"value9\"}"
            }
             */
            if (segmentJson.findValue(key) != null){
                ObjectNode value = mapper.readValue(segmentJson.findValue(key).asText(), ObjectNode.class);
                assertEquals(expectedBlock, BlockOperation.toBlock(value));
                break;
            }
            start += (int)position.len;
        }
    }

    private StructuredStringTable prepareTestTable() throws IOException {
        Index index = new Index();
        for (int i = 0; i < 15; i++) {
            StBlock stBlock = new StBlock(Integer.toString(i), String.format("value%d", i));
            index.put(stBlock.getKey(), stBlock);
        }
        return StructuredStringTable.createFromIndex(baseTestPath, 3, index);
    }
}
