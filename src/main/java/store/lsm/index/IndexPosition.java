package store.lsm.index;

public class IndexPosition {
    public IndexPosition(){ }

    public IndexPosition(long start, long len){
        this.start = start;
        this.len = len;
    }

    public long start;
    public long len;
}
