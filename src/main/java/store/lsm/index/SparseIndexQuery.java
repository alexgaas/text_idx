package store.lsm.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import store.lsm.block.Block;
import store.lsm.block.impl.BlockOperation;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.LinkedList;

import static java.nio.file.StandardOpenOption.*;

public class SparseIndexQuery {
    private final SparseIndex index;

    @Deprecated
    private final RandomAccessFile tableFile;

    private final Path tableFilePath;

    private final Boolean switchToFileChannel;

    private final LinkedList<IndexPosition> sparseKeyPositionList = new LinkedList<>();
    private IndexPosition latestMinimalPosition = null;
    private IndexPosition earliestMaximalPosition = null;

    @Deprecated
    public SparseIndexQuery(SparseIndex index, RandomAccessFile tableFile){
        this.index = index;
        this.switchToFileChannel = false;
        this.tableFile = tableFile;
        this.tableFilePath = null;
    }

    public SparseIndexQuery(SparseIndex index, Path tableFilePath){
        this.index = index;
        this.switchToFileChannel = true;
        this.tableFilePath = tableFilePath;
        this.tableFile = null;
    }

    public void setPositionListByKey(String key){
        for (String k : index.keySet()) {
            if (k.compareTo(key) <= 0) {
                latestMinimalPosition = index.get(k);
            } else {
                earliestMaximalPosition = index.get(k);
                break;
            }
        }
        if (latestMinimalPosition != null) {
            sparseKeyPositionList.add(latestMinimalPosition);
        }
        if (earliestMaximalPosition != null) {
            sparseKeyPositionList.add(earliestMaximalPosition);
        }
    }

    byte[] readSegment(long start, long len) throws IOException {
        byte[] segment = new byte[(int) len];

        if (!switchToFileChannel){
            assert tableFile != null;
            tableFile.seek(start);
            tableFile.read(segment);
        }
        else
        {
            assert this.tableFilePath != null;
            try(FileChannel ch = FileChannel.open(this.tableFilePath, READ)){
                ByteBuffer buf = ByteBuffer.allocate((int) len);
                ch.position(start);
                ch.read(buf);
                if (buf.hasArray()) {
                    segment = buf.array();
                }
            }
        }

        return segment;
    }

    public byte[] getSegmentBySparseIndex() throws IOException {
        IndexPosition firstKeyPosition = sparseKeyPositionList.getFirst();
        IndexPosition lastKeyPosition = sparseKeyPositionList.getLast();

        long start, len;
        start = firstKeyPosition.start;
        if (firstKeyPosition.equals(lastKeyPosition)) {
            len = firstKeyPosition.len;
        } else {
            len = lastKeyPosition.start + lastKeyPosition.len - start;
        }

        return readSegment(start, len);
    }

    public Block queryByKey(String key) throws IOException {
        setPositionListByKey(key);
        if (sparseKeyPositionList.isEmpty()) {
            return null;
        }

        byte[] segment = getSegmentBySparseIndex();

        int pStart = 0;
        ObjectMapper mapper = new ObjectMapper();
        for (IndexPosition position : sparseKeyPositionList) {
            ObjectNode segmentJson = (ObjectNode) mapper.readTree(segment, pStart, (int) position.len);

            if (segmentJson.findValue(key) != null){
                ObjectNode value = mapper.readValue(segmentJson.findValue(key).asText(), ObjectNode.class);
                return BlockOperation.toBlock(value);
            }

            pStart += (int) position.len;
        }
        return null;
    }

    public LinkedList<IndexPosition> GetSparseKeyPositionList(){
        return sparseKeyPositionList;
    }
}
