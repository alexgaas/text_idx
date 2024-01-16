package store.lsm.block;

import com.alibaba.fastjson.JSON;
import store.lsm.block.impl.BlockOperation;

public abstract class AbstractBlock implements Block {
    public BlockOperation.BlockOperationType type;

    public AbstractBlock(BlockOperation.BlockOperationType type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
