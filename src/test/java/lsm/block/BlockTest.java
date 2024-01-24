package lsm.block;

import org.junit.Test;
import store.lsm.block.Block;
import store.lsm.block.impl.RmBlock;
import store.lsm.block.impl.StBlock;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class BlockTest {
    @Test
    public void TestStBlockSerialization(){
        Block block = new StBlock("testKey", "testValue");
        assertFalse(block.toString().isEmpty());
    }

    @Test
    public void TestRmBlockSerialization(){
        Block block = new RmBlock("testKey");
        assertFalse(block.toString().isEmpty());
    }
}
