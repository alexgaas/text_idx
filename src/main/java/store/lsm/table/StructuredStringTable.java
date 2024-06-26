package store.lsm.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import store.lsm.block.Block;

import store.lsm.block.impl.RmBlock;
import store.lsm.block.impl.StBlock;
import store.lsm.index.Index;
import store.lsm.index.IndexPosition;
import store.lsm.index.SparseIndex;
import store.lsm.index.SparseIndexQuery;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class StructuredStringTable implements Closeable {
    private static final String TABLE = ".table";
    private TableMetaData tableMetaData;
    private SparseIndex sparseIndex;
    private RandomAccessFile tableFile;

    private final String filePath;

    private StructuredStringTable(String filePath, int segmentSize) throws IOException {
        this.tableMetaData = new TableMetaData();
        this.tableMetaData.segmentSize = segmentSize;
        this.filePath = filePath;

        initTableFileToZeroPosition(filePath);

        sparseIndex = new SparseIndex();
    }

    private void initTableFileToZeroPosition(String filePath) throws IOException {
        this.tableFile = new RandomAccessFile(filePath, "rw");
        tableFile.seek(0);
    }

    private static StructuredStringTable fromIndex(String filePath, int segmentSize, Index index) throws IOException {
        StructuredStringTable ssTable = new StructuredStringTable(filePath, segmentSize);
        ssTable.initFromIndex(index);
        return ssTable;
    }

    public static StructuredStringTable createFromIndex(String dataDir, int segmentSize, Index index) throws IOException {
        return fromIndex(dataDir + System.currentTimeMillis() + TABLE, segmentSize, index);
    }

    public static StructuredStringTable createFromFile(String filePath) throws IOException {
        StructuredStringTable ssTable = new StructuredStringTable(filePath, 0);
        ssTable.restoreFromFile();
        return ssTable;
    }

    public static Boolean exists(File file){
        String fileName = file.getName();
        return file.isFile() && fileName.endsWith(TABLE);
    }

    public Block query(String key) throws IOException {
        SparseIndexQuery idxQuery = new SparseIndexQuery(sparseIndex, tableFile);
        return idxQuery.queryByKey(key);
    }

    private void restoreFromFile() throws IOException {
        var tableMetaData = TableMetaData.read(tableFile);
        byte[] indexBytes = new byte[(int) tableMetaData.indexLen];
        tableFile.seek(tableMetaData.indexStart);
        tableFile.read(indexBytes);
        String indexStr = new String(indexBytes, StandardCharsets.UTF_8);

        ObjectMapper mapper = new ObjectMapper();
        this.sparseIndex = mapper.readValue(indexStr, SparseIndex.class);
        this.tableMetaData = tableMetaData;
    }

    private void initFromIndex(Index index) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode segment = mapper.createObjectNode();

        tableMetaData.dataStart = tableFile.getFilePointer();
        for (Block command : index.values()) {
            if (command instanceof StBlock) {
                StBlock set = (StBlock) command;
                segment.put(set.getKey(), set.toString());
            }

            if (command instanceof RmBlock) {
                RmBlock rm = (RmBlock) command;
                segment.put(rm.getKey(), rm.toString());
            }

            if (segment.size() >= tableMetaData.segmentSize) {
                writeSegment(segment);
            }
        }

        if (!segment.isEmpty()) {
            writeSegment(segment);
        }
        tableMetaData.dataLen = tableFile.getFilePointer() - tableMetaData.dataStart;
        byte[] indexBytes = mapper.writeValueAsBytes(sparseIndex);

        tableMetaData.indexStart = tableFile.getFilePointer();
        tableFile.write(indexBytes);
        tableMetaData.indexLen = indexBytes.length;

        tableMetaData.write(tableFile);
    }

    private void writeSegment(ObjectNode segment) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        byte[] bytes = mapper.writeValueAsBytes(segment);

        long start = tableFile.getFilePointer();
        tableFile.write(bytes);

        Optional<String> firstKey = Optional.ofNullable(segment.fields().next().getKey());
        firstKey.ifPresent(s -> sparseIndex.put(s, new IndexPosition(start, bytes.length)));

        segment.removeAll();
    }

    @Override
    public void close() throws IOException {
        tableFile.close();
    }

    public RandomAccessFile GetTableFile(){
        return this.tableFile;
    }

    public SparseIndex GetSparseIndex(){
        return this.sparseIndex;
    }

    public String GetFilePath() { return this.filePath; }
}

