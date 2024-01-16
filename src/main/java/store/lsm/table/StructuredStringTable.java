package store.lsm.table;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import store.lsm.block.Block;
import store.lsm.block.impl.RmBlock;
import store.lsm.block.impl.StBlock;
import store.lsm.index.IndexPosition;
import store.lsm.index.SparseIndex;
import store.lsm.index.SparseIndexQuery;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.TreeMap;

public class StructuredStringTable implements Closeable {
    private static final String TABLE = ".table";
    private static final String FILE_MODE = "rw";

    private TableMetaData tableMetaData;

    private SparseIndex sparseIndex;

    private RandomAccessFile tableFile;

    private StructuredStringTable(String filePath, int segmentSize) throws IOException {
        this.tableMetaData = new TableMetaData();
        this.tableMetaData.segmentSize = segmentSize;
        initTableFileToZeroPosition(filePath);
        sparseIndex = new SparseIndex();
    }

    private void initTableFileToZeroPosition(String filePath) throws IOException {
        this.tableFile = new RandomAccessFile(filePath, FILE_MODE);
        tableFile.seek(0);
    }

    private static StructuredStringTable fromIndex(String filePath, int partSize, TreeMap<String, Block> index) throws IOException {
        StructuredStringTable ssTable = new StructuredStringTable(filePath, partSize);
        ssTable.initFromIndex(index);
        return ssTable;
    }

    public static StructuredStringTable createFromIndex(String dataDir, int segmentSize, TreeMap<String, Block> index) throws IOException {
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

        this.sparseIndex = SparseIndex
                .getFromTypedIndex(JSONObject.parseObject(indexStr, new TypeReference<>(){}));

        this.tableMetaData = tableMetaData;
    }

    private void initFromIndex(TreeMap<String, Block> index) throws IOException {
        JSONObject segment = new JSONObject(true);
        tableMetaData.dataStart = tableFile.getFilePointer();
        for (Block command : index.values()) {
            if (command instanceof StBlock) {
                StBlock set = (StBlock) command;
                segment.put(set.getKey(), set);
            }

            if (command instanceof RmBlock) {
                RmBlock rm = (RmBlock) command;
                segment.put(rm.getKey(), rm);
            }

            if (segment.size() >= tableMetaData.segmentSize) {
                writeSegment(segment);
            }
        }

        if (!segment.isEmpty()) {
            writeSegment(segment);
        }
        tableMetaData.dataLen = tableFile.getFilePointer() - tableMetaData.dataStart;

        byte[] indexBytes = JSONObject.toJSONString(sparseIndex).getBytes(StandardCharsets.UTF_8);
        tableMetaData.indexStart = tableFile.getFilePointer();
        tableFile.write(indexBytes);
        tableMetaData.indexLen = indexBytes.length;

        tableMetaData.write(tableFile);
    }

    private void writeSegment(JSONObject partData) throws IOException {
        byte[] partDataBytes = partData.toJSONString().getBytes(StandardCharsets.UTF_8);
        long start = tableFile.getFilePointer();
        tableFile.write(partDataBytes);

        Optional<String> firstKey = partData.keySet().stream().findFirst();
        firstKey.ifPresent(s -> sparseIndex.put(s, new IndexPosition(start, partDataBytes.length)));
        partData.clear();
    }

    @Override
    public void close() throws IOException {
        tableFile.close();
    }
}

