package store.lsm;

import com.alibaba.fastjson.JSONObject;
import store.Store;
import store.lsm.block.Block;
import store.lsm.block.impl.RmBlock;
import store.lsm.block.impl.StBlock;
import store.lsm.table.StructuredStringTable;
import store.lsm.wal.WriteAheadLog;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.io.File;

public class Lsm implements Store {
    //private static final String TABLE = ".table";
    //private static final String WRITE_AHEAD_LOG = "log";
    //private static final String FILE_MODE = "rw";
    //private static final String WRITE_AHEAD_LOG_TMP = "logTmp";

    private TreeMap<String, Block> index;

    private TreeMap<String, Block> immutableIndex;

    private final LinkedList<StructuredStringTable> tables;

    private final String dataDir;

    private final ReadWriteLock indexLock;

    private final int storeThreshold;

    private final int segmentSize;

    //private RandomAccessFile writeAheadLog;
    private WriteAheadLog writeAheadLog;

    //private File writeAheadLogFile;

    public Lsm(String dataDir, int storeThreshold, int segmentSize) {
        try {
            this.dataDir = dataDir;
            this.storeThreshold = storeThreshold;
            this.segmentSize = segmentSize;
            this.indexLock = new ReentrantReadWriteLock();

            tables = new LinkedList<>();
            index = new TreeMap<>();

            File dir = new File(dataDir);
            File[] files = dir.listFiles();
            if (files == null || files.length == 0) {
                //writeAheadLogFile = new File(dataDir + WRITE_AHEAD_LOG);
                //writeAheadLog = new RandomAccessFile(writeAheadLogFile, FILE_MODE);
                writeAheadLog = new WriteAheadLog(dataDir);
                return;
            }

            TreeMap<Long, StructuredStringTable> ssTableTreeMap = new TreeMap<>(Comparator.reverseOrder());
            for (File file : files) {
                String fileName = file.getName();
                if (WriteAheadLog.tempLogExists(file)){ //file.isFile() && fileName.equals(WriteAheadLog.WRITE_AHEAD_LOG_TMP)) {
                    //restoreFromWal(new RandomAccessFile(file, FILE_MODE));
                    writeAheadLog.restoreFromLog(index);
                }
                if (StructuredStringTable.exists(file)){//file.isFile() && fileName.endsWith(TABLE)) {
                    int dotIndex = fileName.indexOf(".");
                    Long time = Long.parseLong(fileName.substring(0, dotIndex));

                    ssTableTreeMap.put(time, StructuredStringTable.createFromFile(file.getAbsolutePath()));
                } else if (WriteAheadLog.logExists(file)){ //file.isFile() && fileName.equals(WriteAheadLog.WRITE_AHEAD_LOG)) {
                    //writeAheadLogFile = file;
                    //writeAheadLog = new RandomAccessFile(file, FILE_MODE);
                    //restoreFromWal(writeAheadLog);
                    writeAheadLog = new WriteAheadLog(file);
                    writeAheadLog.restoreFromLog(index);
                }
            }
            tables.addAll(ssTableTreeMap.values());
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

//    private void restoreFromWal(RandomAccessFile wal) {
//        try {
//            long len = wal.length();
//            long start = 0;
//            wal.seek(start);
//            while (start < len) {
//                int valueLen = wal.readInt();
//                byte[] bytes = new byte[valueLen];
//                wal.read(bytes);
//                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
//                Block block = BlockOperation.toBlock(value);
//                if (block != null) {
//                    index.put(block.getKey(), block);
//                }
//                start += 4;
//                start += valueLen;
//            }
//            wal.seek(wal.length());
//        } catch (Throwable t) {
//            throw new RuntimeException(t);
//        }
//    }

    @Override
    public void put(String key, String value) {
        try {
            StBlock block = new StBlock(key, value);
            byte[] bytes = JSONObject.toJSONBytes(block);
            indexLock.writeLock().lock();
            writeAheadLog.writeInt(bytes.length);
            writeAheadLog.write(bytes);
            index.put(key, block);

            if (index.size() > storeThreshold) {
                switchIndex();
                dropToTable();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }

    }

    private void switchIndex() {
        try {
            indexLock.writeLock().lock();
            immutableIndex = index;
            index = new TreeMap<>();
            writeAheadLog.close();

//            File tmpWal = new File(dataDir + WRITE_AHEAD_LOG_TMP);
//            if (tmpWal.exists()) {
//                if (!tmpWal.delete()) {
//                    throw new RuntimeException(WRITE_AHEAD_LOG_TMP);
//                }
//            }
//            if (!writeAheadLogFile.renameTo(tmpWal)) {
//                throw new RuntimeException(WRITE_AHEAD_LOG_TMP);
//            }
            writeAheadLog.renameLogFileToCopy();

            //writeAheadLogFile = new File(dataDir + WRITE_AHEAD_LOG);
            //writeAheadLog = new RandomAccessFile(writeAheadLogFile, FILE_MODE);

            writeAheadLog = new WriteAheadLog(dataDir);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    private void dropToTable() {
        try {
            StructuredStringTable ssTable = StructuredStringTable.createFromIndex(
                    dataDir,
                    segmentSize,
                    immutableIndex);
            tables.addFirst(ssTable);

            immutableIndex = null;

//            File tmpWal = new File(dataDir + WRITE_AHEAD_LOG_TMP);
//            if (tmpWal.exists()) {
//                if (!tmpWal.delete()) {
//                    throw new RuntimeException(WRITE_AHEAD_LOG_TMP);
//                }
//            }
            writeAheadLog.removeLogCopy();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }


    @Override
    public String get(String key) {
        try {
            indexLock.readLock().lock();
            Block block = index.get(key);
            if (block == null && immutableIndex != null) {
                block = immutableIndex.get(key);
            }
            if (block == null) {
                for (StructuredStringTable ssTable : tables) {
                    block = ssTable.query(key);
                    if (block != null) {
                        break;
                    }
                }
            }
            if (block instanceof StBlock) {
                return ((StBlock) block).value;
            }
            if (block instanceof RmBlock) {
                return null;
            }
            return null;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.readLock().unlock();
        }
    }

    @Override
    public void remove(String key) {
        try {
            indexLock.writeLock().lock();
            RmBlock rmBlock = new RmBlock(key);
            byte[] bytes = JSONObject.toJSONBytes(rmBlock);
            writeAheadLog.writeInt(bytes.length);
            writeAheadLog.write(bytes);
            index.put(key, rmBlock);
            if (index.size() > storeThreshold) {
                switchIndex();
                dropToTable();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        writeAheadLog.close();
        for (StructuredStringTable ssTable : tables) {
            ssTable.close();
        }
    }
}
