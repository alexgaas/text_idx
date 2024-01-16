package store.lsm.index;

import store.lsm.block.Block;

import java.util.TreeMap;

public class Index extends TreeMap<String, Block> {
    public Index(){
        super();
    }
}
