package lsm.block;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import store.lsm.block.Block;
import store.lsm.block.impl.BlockOperation;
import store.lsm.block.impl.RmBlock;
import store.lsm.block.impl.StBlock;

import static org.junit.jupiter.api.Assertions.*;

public class BlockTest {
    @Test
    public void testStBlockSerialization(){
        Block block = new StBlock("testKey", "testValue");
        assertFalse(block.toString().isEmpty());
    }

    @Test
    public void testRmBlockSerialization(){
        Block block = new RmBlock("testKey");
        assertFalse(block.toString().isEmpty());
    }

    @Test
    public void testToBlock() throws JsonProcessingException {
        // test StBlock
        Block block = new StBlock("testKey", "testValue");
        ObjectMapper mapper = new ObjectMapper();
        JsonNode value = mapper.readTree(block.toString());
        var result = BlockOperation.toBlock(value);
        assertEquals(block, result);

        // test RmBlock
        block = new RmBlock("testKey");
        value = mapper.readTree(block.toString());
        result = BlockOperation.toBlock(value);
        assertEquals(block, result);
    }
}
