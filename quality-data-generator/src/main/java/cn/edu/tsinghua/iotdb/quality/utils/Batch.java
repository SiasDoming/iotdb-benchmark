package cn.edu.tsinghua.iotdb.quality.utils;
/*
 * Copy From package cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.utils.Batch.java
 */

import java.util.LinkedList;
import java.util.List;

public class Batch {

    // device schema
    private DeviceSchema deviceSchema;
    // raw data
    private List<Record> records;

    public Batch() {
        records = new LinkedList<>();
    }

    public Batch(DeviceSchema deviceSchema, List<Record> records) {
        this.deviceSchema = deviceSchema;
        this.records = records;
    }

    public DeviceSchema getDeviceSchema() {
        return deviceSchema;
    }

    public void setDeviceSchema(DeviceSchema deviceSchema) {
        this.deviceSchema = deviceSchema;
    }

    public List<Record> getRecords() {
        return records;
    }

    public void addRecord(Record record) {
        records.add(record);
    }
    public void addRecord(long timestamp, List<Object> values) {
        records.add(new Record(timestamp, values));
    }

    public int size() {
        return records.size();
    }

    public int pointNum() {
        int pointNum = 0;
        for (Record record : records) {
            pointNum += record.size();
        }
        return pointNum;
    }

}
