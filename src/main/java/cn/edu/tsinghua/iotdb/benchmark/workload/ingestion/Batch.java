package cn.edu.tsinghua.iotdb.benchmark.workload.ingestion;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import java.util.List;
import java.util.LinkedList;

public class Batch {

  private DeviceSchema deviceSchema;
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

  public void add(long timestamp, List<Object> values) {
    records.add(new Record(timestamp, values));
  }

  /**
   * use the row protocol which means data are organized in List[timestamp, List[value]]
   *
   * @return data point number in this batch
   */
  public int pointNum() {
    int pointNum = 0;
    for (Record record : records) {
      pointNum += record.size();
    }
    return pointNum;
  }

}
