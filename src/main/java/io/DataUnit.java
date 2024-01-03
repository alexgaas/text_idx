package io;

/*
Constant	Data Size	Power of 2	Size in Bytes
BYTES	1B	2^0	1
KILOBYTES	1KB	2^10	1,024
MEGABYTES	1MB	2^20	1,048,576
GIGABYTES	1GB	2^30	1,073,741,824
TERABYTES	1TB	2^40	1,099,511,627,776
 */
public enum DataUnit {
    BYTE,
    KILOBYTE,
    MEGABYTE,
    GIGABYTES,
    TERABYTE;

    public long value(){
        long val = 0;
        switch (this){
            case BYTE:
                val = 1L;
                break;
            case KILOBYTE:
                val = 1024L;
                break;
            case MEGABYTE:
                val = 1048576L;
                break;
            case GIGABYTES:
                val = 1073741824L;
                break;
            case TERABYTE:
                val = 1099511627776L;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + this);
        }
        return val;
    }
}
