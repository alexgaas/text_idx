package store.lsm.table;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.*;

public class TableMetaData {
    private static final int LONG_SIZE_64 = 8;
    /*
    defined in order as is to write in table metadata:
    | segmentSize | dataStart | dataLen | indexStart | indexLen | version |

    example of file representation (binary):

    0000 001b 7b22 6b65 7922 3a22 3922 2c22
    7479 7065 223a 2252 454d 4f56 4522 7d00
    0000 1c7b 226b 6579 223a 2231 3022 2c22
    7479 7065 223a 2252 454d 4f56 4522 7d

     */
    public long segmentSize;
    public long dataStart;
    public long dataLen;
    public long indexStart;
    public long indexLen;
    public long version;
    private Path filePath;

    @Deprecated
    public TableMetaData(){
    }

    public TableMetaData(Path filePath){
        this.filePath = filePath;
    }

    private ByteBuffer writeLong(long... values) throws IOException {
        ByteBuffer b = ByteBuffer.allocate(LONG_SIZE_64 * values.length);
        for(long value: values){
            b.putLong(value);
        }
        return  b.flip();
    }

    public void write() {
        try(FileChannel ch = FileChannel.open(this.filePath, CREATE_NEW, WRITE)){
            int result = ch.write(writeLong(
                    // since all  metadata have same type as [long] we can just write them sequentially as:
                    // |segmentSize| dataStart| dataLen| indexStart| indexLen| version|
                        segmentSize, dataStart, dataLen, indexStart, indexLen, version)
            );
            if (result <= 0){
                throw new RuntimeException("File " + filePath + " not been written");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static long readLong(FileChannel ch, long position) throws IOException {
        ByteBuffer b = ByteBuffer.allocate(LONG_SIZE_64);
        ch.read(b, position);
        return b.flip().getLong();
    }

    public static TableMetaData read(Path path) throws IOException {
        TableMetaData tableMetaData = new TableMetaData();
        try(FileChannel ch = FileChannel.open(path, READ)){
            long fileLen = ch.size();
            /*
            Get file length and read from the end. Example of metadata record:

            | segmentSize | dataStart | dataLen | indexStart | indexLen | version |
            |      3      |     0     |    566  |     566    |    123   |    0    |
             */
            tableMetaData.segmentSize   = readLong(ch, fileLen - LONG_SIZE_64 * 6);
            tableMetaData.dataStart     = readLong(ch, fileLen - LONG_SIZE_64 * 5);
            tableMetaData.dataLen       = readLong(ch, fileLen - LONG_SIZE_64 * 4);
            tableMetaData.indexStart    = readLong(ch, fileLen - LONG_SIZE_64 * 3);
            tableMetaData.indexLen      = readLong(ch, fileLen - LONG_SIZE_64 * 2);
            tableMetaData.version       = readLong(ch, fileLen - LONG_SIZE_64);

            return tableMetaData;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Deprecated
    public void write(RandomAccessFile file) throws IOException {
        // since all  metadata have same type as [long] we can just write them sequentially as:
        // | segmentSize | dataStart | dataLen | indexStart | indexLen | version |
        file.writeLong(segmentSize);

        file.writeLong(dataStart);

        file.writeLong(dataLen);

        file.writeLong(indexStart);

        file.writeLong(indexLen);

        file.writeLong(version);
        // example:
        // |      3      |     0     |    566  |     566    |    123   |    0    |
    }

    @Deprecated
    public static TableMetaData read(RandomAccessFile file) throws IOException {
        TableMetaData TableMetaData = new TableMetaData();
        long fileLen = file.length();

        /*
        Get file length and read from the end. Example of metadata record:

        | segmentSize | dataStart | dataLen | indexStart | indexLen | version |
        |      3      |     0     |    566  |     566    |    123   |    0    |
         */
        file.seek(fileLen - LONG_SIZE_64 * 6);
        TableMetaData.segmentSize = file.readLong();

        file.seek(fileLen - LONG_SIZE_64 * 5);
        TableMetaData.dataStart = file.readLong();

        file.seek(fileLen - LONG_SIZE_64 * 4);
        TableMetaData.dataLen = file.readLong();

        file.seek(fileLen - LONG_SIZE_64 * 3);
        TableMetaData.indexStart = file.readLong();

        file.seek(fileLen - LONG_SIZE_64 * 2);
        TableMetaData.indexLen = file.readLong();

        file.seek(fileLen - LONG_SIZE_64);
        TableMetaData.version = file.readLong();

        return TableMetaData;
    }
}