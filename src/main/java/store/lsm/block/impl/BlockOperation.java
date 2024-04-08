package store.lsm.block.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import store.lsm.block.Block;

import java.nio.charset.StandardCharsets;

public class BlockOperation {
    public static final String TYPE = "type";

    public static Block toBlock(JsonNode value) {
        ObjectMapper mapper = new ObjectMapper();
        // serialize to [StBlock] or [RmBlock] based on type [SET / REMOVE]
        if (value.findValue(TYPE).asText().equals(BlockOperationType.SET.name())) {
            return  mapper.convertValue(value, StBlock.class);
        } else if (value.findValue(TYPE).asText().equals(BlockOperationType.REMOVE.name())) {
            return  mapper.convertValue(value, RmBlock.class);
        }
        return null;
    }

    public static Block toBlock(byte[] bytes) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode value = mapper.readTree(new String(bytes, StandardCharsets.UTF_8));
        return BlockOperation.toBlock(value);
    }

    public static byte[] toByteArray(Block block) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsBytes(block);
    }

    public enum BlockOperationType {
        SET,
        REMOVE
    }
}
