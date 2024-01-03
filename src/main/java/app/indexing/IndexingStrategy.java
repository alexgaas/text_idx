package app.indexing;

import io.DataUnit;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class IndexingStrategy {
    public IndexingStrategy(){

    }

    public static void startIndexing(String path) throws Exception {
        long persistentChinkSize = 8 * DataUnit.MEGABYTE.value();

        FileChannel inChannel = new RandomAccessFile(path, "r").getChannel();
        long fileSize = inChannel.size();
        long chunkSize = persistentChinkSize;
        if (fileSize > persistentChinkSize){
            chunkSize = fileSize;
        }
        MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, chunkSize);
        inChannel.close();
    }

}
