package store.lsm.block.impl;

import store.lsm.block.AbstractBlock;

public class RmBlock extends AbstractBlock {
    public String key;

    public RmBlock(String key) {
        super(BlockOperation.BlockOperationType.REMOVE);
        this.key = key;
    }

    @Override
    public String getKey() {
        return key;
    }
}
