package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.UDFRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.BasicReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.GeolifeReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.ReddReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.TDriveReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import java.util.List;
import java.util.ArrayList;

public class RealDatasetWorkLoad implements IWorkload {

  private BasicReader reader;

  private Config config;
  private List<DeviceSchema> deviceSchemaList;
  private long startTime;
  private long endTime;

  /**
   * write test.
   *
   * @param files real dataset files
   * @param config config
   */
  public RealDatasetWorkLoad(List<String> files, Config config) {
    switch (config.DATA_SET) {
      case TDRIVE:
        reader = new TDriveReader(config, files);
        break;
      case REDD:
        reader = new ReddReader(config, files);
        break;
      case GEOLIFE:
        reader = new GeolifeReader(config, files);
        break;
      default:
        throw new RuntimeException(config.DATA_SET + " not supported");
    }
  }

  /**
   * read test.
   *
   * @param config config
   */
  public RealDatasetWorkLoad(Config config) {
    this.config = config;

    //init sensor list
    List<String> sensorList = new ArrayList<>();
    for (int i = 0; i < config.QUERY_SENSOR_NUM; i++) {
      sensorList.add(config.FIELDS.get(i));
    }

    //init device schema list
    deviceSchemaList = new ArrayList<>();
    for (int i = 1; i <= config.QUERY_DEVICE_NUM; i++) {
      String deviceIdStr = "" + i;
      DeviceSchema deviceSchema = new DeviceSchema(calGroupIdStr(deviceIdStr, config.GROUP_NUMBER),
          deviceIdStr, sensorList);
      deviceSchemaList.add(deviceSchema);
    }

    //init startTime, endTime
    startTime = config.REAL_QUERY_START_TIME;
    endTime = config.REAL_QUERY_STOP_TIME;

  }

  public Batch getOneBatch() {
    if (reader.hasNextBatch()) {
      return reader.nextBatch();
    } else {
      return null;
    }
  }

  @Override
  public Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex) throws WorkloadException {
    throw new WorkloadException("not support in real data workload.");
  }

  @Override
  public RangeQuery getRangeQuery() {
    return new RangeQuery(deviceSchemaList, startTime, endTime);
  }

  @Override
  public AggRangeQuery getAggRangeQuery() {
    return new AggRangeQuery(deviceSchemaList, startTime, endTime, config.QUERY_AGGREGATE_FUN);
  }

  @Override
  public UDFRangeQuery getUDFRangeQuery() {
    int currentUDFLoop = config.getAndIncrementQueryUDFLoop();
    return new UDFRangeQuery(deviceSchemaList, startTime, endTime, config.QUERY_UDF_NAME_LIST.get(currentUDFLoop), config.QUERY_UDF_FULL_CLASS_NAME.get(currentUDFLoop));
  }

  static String calGroupIdStr(String deviceId, int groupNum) {
    return String.valueOf(deviceId.hashCode() % groupNum);
  }

}
