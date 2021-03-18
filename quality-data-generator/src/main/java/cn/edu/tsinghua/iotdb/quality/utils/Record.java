package cn.edu.tsinghua.iotdb.quality.utils;

import java.util.List;

public class Record {

    // timestamp
    private long timestamp;
    // list of data values
    private List<Object> recordDataValue;

    public Record(long timestamp, List<Object> recordDataValue) {
        this.timestamp = timestamp;
        this.recordDataValue = recordDataValue;
    }

    public int size(){
        return recordDataValue.size();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public List<Object> getRecordDataValue() {
        return recordDataValue;
    }

}
