package store.lsm.wal;

import store.lsm.block.Block;
import store.lsm.block.impl.BlockOperation;
import store.lsm.index.Index;

import java.io.*;

public class WriteAheadLog extends RandomAccessFile implements Closeable {
    private static final String WRITE_AHEAD_LOG = "log";
    private static final String WRITE_AHEAD_LOG_TMP = "logTmp";
    private final RandomAccessFile writeAheadLog;
    private final File walFile;

    public WriteAheadLog(String dataDir) throws FileNotFoundException {
        super(new File(dataDir + WRITE_AHEAD_LOG), "rw");
        this.walFile = new File(dataDir + WRITE_AHEAD_LOG);
        this.writeAheadLog = new RandomAccessFile(walFile, "rw");
    }

    public WriteAheadLog(File walFile) throws FileNotFoundException {
        super(walFile, "rw");
        this.walFile = walFile;
        this.writeAheadLog = new RandomAccessFile(walFile, "rw");
    }

    public void restoreFromLog(Index index) throws IOException {
        long start = 0;
        writeAheadLog.seek(start);
        while (start < writeAheadLog.length()) {
            int valueLen = writeAheadLog.readInt();
            byte[] bytes = new byte[valueLen];
            writeAheadLog.read(bytes);

            Block block = BlockOperation.toBlock(bytes);
            if (block != null) {
                index.put(block.getKey(), block);
            }
            start += 4;
            start += valueLen;
        }
        writeAheadLog.seek(writeAheadLog.length());
    }

    public void renameLogFileToCopy(){
        String dataDir = GetDataDir();
        File tmpWal = new File(dataDir + WRITE_AHEAD_LOG_TMP);
        if (tmpWal.exists()) {
            if (!tmpWal.delete()) {
                throw new RuntimeException(WRITE_AHEAD_LOG_TMP);
            }
        }
        if (!walFile.renameTo(tmpWal)) {
            throw new RuntimeException(WRITE_AHEAD_LOG_TMP);
        }
    }

    public void removeLogCopy(){
        String dataDir = GetDataDir();
        File tmpWal = new File(dataDir + WRITE_AHEAD_LOG_TMP);
        if (tmpWal.exists()) {
            if (!tmpWal.delete()) {
                throw new RuntimeException(WRITE_AHEAD_LOG_TMP);
            }
        }
    }

    private static Boolean exists(File file, String fileType){
        return file.isFile() && file.getName().equals(fileType);
    }

    public static Boolean logExists(File file){
        return exists(file, WriteAheadLog.WRITE_AHEAD_LOG);
    }

    public static Boolean tempLogExists(File file){
        return exists(file, WriteAheadLog.WRITE_AHEAD_LOG_TMP);
    }

    private String GetDataDir() {
        String dataDir = walFile.getAbsolutePath().split(WRITE_AHEAD_LOG)[0];
        if (dataDir.isEmpty()){
            throw new RuntimeException();
        }
        return dataDir;
    }

    @Override
    public void close() throws IOException {
        writeAheadLog.close();
    }
}
