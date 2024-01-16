package store.lsm.block.impl;

import store.lsm.block.AbstractBlock;

public class StBlock extends AbstractBlock {
    public String key;
    public String value;

    public StBlock(String key, String value) {
        super(BlockOperation.BlockOperationType.SET);
        this.key = key;
        this.value = value;
    }

    @Override
    public String getKey() {
        return key;
    }
}
