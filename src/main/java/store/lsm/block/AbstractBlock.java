package store.lsm.block;

import com.fasterxml.jackson.core.JsonProcessingException;
import store.lsm.block.impl.BlockOperation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public abstract class AbstractBlock implements Block {
    public BlockOperation.BlockOperationType type;

    public AbstractBlock(BlockOperation.BlockOperationType type) {
        this.type = type;
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter objectWrapper = mapper.writer();
        try {
            return objectWrapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
