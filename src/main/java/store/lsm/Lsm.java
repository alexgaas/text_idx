package store.lsm;

import store.lsm.block.Block;
import store.lsm.block.impl.BlockOperation;
import store.lsm.block.impl.RmBlock;
import store.lsm.block.impl.StBlock;
import store.lsm.index.Index;
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
    private Index index;

    private Index immutableIndex;

    private final LinkedList<StructuredStringTable> tables;

    private final String dataDir;

    private final ReadWriteLock indexLock;

    private final int storeThreshold;

    private final int segmentSize;
    private WriteAheadLog writeAheadLog;

    public Lsm(String dataDir, int storeThreshold, int segmentSize) {
        this.dataDir = dataDir;
        this.storeThreshold = storeThreshold;
        this.segmentSize = segmentSize;
        this.indexLock = new ReentrantReadWriteLock();

        try {
            tables = new LinkedList<>();
            index = new Index();

            File dir = new File(dataDir);
            File[] files = dir.listFiles((file, name) -> !name.equals(".DS_Store"));
            if (files == null || files.length == 0) {
                writeAheadLog = new WriteAheadLog(dataDir);
                return;
            }

            TreeMap<Long, StructuredStringTable> ssTableTreeMap = new TreeMap<>(Comparator.reverseOrder());
            for (File file : files) {
                initIndex(file);
                initTables(file, file.getName(), ssTableTreeMap);
            }
            tables.addAll(ssTableTreeMap.values());
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public Lsm(String dataDir, int storeThreshold) {
        this(dataDir, storeThreshold, storeThreshold);
    }

    private void initIndex(File file) throws IOException {
        if (WriteAheadLog.tempLogExists(file)){
            index.putAll(writeAheadLog.restoreFromLog());
        }
        else if (WriteAheadLog.logExists(file)){
            writeAheadLog = new WriteAheadLog(file);
            index.putAll(writeAheadLog.restoreFromLog());
        }
    }

    private static void initTables(File file, String fileName, TreeMap<Long, StructuredStringTable> ssTableTreeMap) throws IOException {
        if (StructuredStringTable.exists(file)){
            int dotIndex = fileName.indexOf(".");
            Long time = Long.parseLong(fileName.substring(0, dotIndex));
            ssTableTreeMap.put(time, StructuredStringTable.createFromFile(file.getAbsolutePath()));
        }
    }

    private void upsert(Block block){
        try {
            byte[] bytes = BlockOperation.toByteArray(block);

            indexLock.writeLock().lock();
            writeAheadLog.writeInt(bytes.length);
            writeAheadLog.write(bytes);
            index.put(block.getKey(), block);

            if (index.size() > storeThreshold) {
                shiftIndex();
                dropToTable();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public void put(String key, String value) {
        StBlock block = new StBlock(key, value);
        upsert(block);
    }

    @Override
    public void remove(String key) {
        RmBlock rmBlock = new RmBlock(key);
        upsert(rmBlock);
    }

    private void shiftIndex() {
        try {
            immutableIndex = index;
            index = new Index();
            writeAheadLog.close();

            writeAheadLog.renameLogFileToCopy();
            writeAheadLog = new WriteAheadLog(dataDir);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void dropToTable() {
        try {
            StructuredStringTable ssTable = StructuredStringTable.createFromIndex(dataDir, segmentSize, immutableIndex);
            tables.addFirst(ssTable);

            immutableIndex = null;

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
    public void close() throws IOException {
        writeAheadLog.close();
        for (StructuredStringTable ssTable : tables) {
            ssTable.close();
        }
    }
}
