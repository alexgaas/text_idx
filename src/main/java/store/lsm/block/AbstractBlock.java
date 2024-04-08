package store.lsm.block;

import com.fasterxml.jackson.core.JsonProcessingException;
import store.lsm.block.impl.BlockOperation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.util.Objects;

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

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != this.getClass()) {
            return false;
        }

        final AbstractBlock other = (AbstractBlock) obj;
        if (!Objects.equals(this.type, other.type)) {
            return false;
        }

        return Objects.equals(this.toString(), other.toString());
    }
}
