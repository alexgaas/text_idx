package store.lsm.block.impl;

import com.alibaba.fastjson.JSONObject;
import store.lsm.block.Block;

public class BlockOperation {
    public static final String TYPE = "type";
    public static Block toBlock(JSONObject value) {
        if (value.getString(TYPE).equals(BlockOperationType.SET.name())) {
            return value.toJavaObject(StBlock.class);
        } else if (value.getString(TYPE).equals(BlockOperationType.REMOVE.name())) {
            return value.toJavaObject(RmBlock.class);
        }
        return null;
    }

    public enum BlockOperationType {
        SET,
        REMOVE
    }
}
