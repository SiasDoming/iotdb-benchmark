package cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.SyntheticWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangedUDFQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

public class IoTDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
  private static final String ALREADY_KEYWORD = "already exist";
  private static final String ALREADY_REGISTRATED_KEYWORTD = "already been registered";
  private static boolean IS_UDF_REGISTRATED = false;
  protected static Config config = ConfigDescriptor.getInstance().getConfig();

  protected IoTDBConnection ioTDBConnection;
  protected ExecutorService service;
  protected Future<?> future;

  public IoTDB() {
  }

  @Override
  public void init() throws TsdbException {
    try {
      ioTDBConnection = new IoTDBConnection();
      ioTDBConnection.init();
      this.service = Executors.newSingleThreadExecutor();
    } catch (Exception e) {
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException{
    try {
      Session cleanupSession = new Session(config.HOST, config.PORT, Constants.USER,
              Constants.PASSWD);
      cleanupSession.open(config.ENABLE_THRIFT_COMPRESSION);
      String cleanupSql = String.format("DELETE TIMESERIES root.%s*", config.GROUP_NAME_PREFIX);
      cleanupSession.executeNonQueryStatement(cleanupSql);
      cleanupSession.close();
    } catch (Exception e) {
      LOGGER.error("Clean up IoTDB data failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void close() throws TsdbException {
    ioTDBConnection.close();
    if (service != null) {
      service.shutdownNow();
    }
    if (future != null) {
      future.cancel(true);
    }
  }

  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    if (!config.OPERATION_PROPORTION.split(":")[0].equals("0")) {
      try {
        Session metaSession = new Session(config.HOST, config.PORT, Constants.USER,
            Constants.PASSWD);
        metaSession.open(config.ENABLE_THRIFT_COMPRESSION);
        // get all storage groups
        Set<String> groups = new HashSet<>();
        for (DeviceSchema schema : schemaList) {
          groups.add(schema.getGroup());
        }
        // register storage groups
        for (String group : groups) {
          metaSession.setStorageGroup(Constants.ROOT_SERIES_NAME + "." + group);
        }

        // create time series
        List<String> paths = new ArrayList<>();
        List<TSDataType> tsDataTypes = new ArrayList<>();
        List<TSEncoding> tsEncodings = new ArrayList<>();
        List<CompressionType> compressionTypes = new ArrayList<>();
        int count = 0;
        int createSchemaBatchNum = 10000;
        for (DeviceSchema deviceSchema : schemaList) {
          int sensorIndex = 0;
          for (String sensor : deviceSchema.getSensors()) {
            paths.add(getSensorPath(deviceSchema, sensor));
            String datatype = SyntheticWorkload.getNextDataType(sensorIndex++);
            tsDataTypes.add(Enum.valueOf(TSDataType.class, datatype));
            tsEncodings.add(Enum.valueOf(TSEncoding.class, getEncodingType(datatype)));
            compressionTypes.add(Enum.valueOf(CompressionType.class, config.COMPRESSOR));
            if (++count % createSchemaBatchNum == 0) {
              metaSession
                  .createMultiTimeseries(paths, tsDataTypes, tsEncodings, compressionTypes, null,
                      null, null, null);
              paths.clear();
              tsDataTypes.clear();
              tsEncodings.clear();
              compressionTypes.clear();
            }
          }
        }
        if (!paths.isEmpty()) {
          metaSession
              .createMultiTimeseries(paths, tsDataTypes, tsEncodings, compressionTypes, null,
                  null, null, null);
          paths.clear();
          tsDataTypes.clear();
          tsEncodings.clear();
          compressionTypes.clear();
        }
        metaSession.close();
      } catch (Exception e) {
        // ignore if already has the time series
        if (!e.getMessage().contains(ALREADY_KEYWORD) && !e.getMessage().contains("300")) {
          LOGGER.error("Register IoTDB schema failed because ", e);
          throw new TsdbException(e);
        }
      }
    }
  }

  String getEncodingType(String dataType) {
    switch (dataType) {
      case "BOOLEAN":
        return config.ENCODING_BOOLEAN;
      case "INT32":
        return config.ENCODING_INT32;
      case "INT64":
        return config.ENCODING_INT64;
      case "FLOAT":
        return config.ENCODING_FLOAT;
      case "DOUBLE":
        return config.ENCODING_DOUBLE;
      case "TEXT":
        return config.ENCODING_TEXT;
      default:
        LOGGER.error("Unsupported data type {}.", dataType);
        return null;
    }
  }

  // convert deviceSchema to the format: root.group_1.d_1
  private String getDevicePath(DeviceSchema deviceSchema) {
    return Constants.ROOT_SERIES_NAME + "." + deviceSchema.getGroup() + "." + deviceSchema
        .getDevice();
  }

  // convert deviceSchema and sensor to the format: root.group_1.d_1.s_1
  private String getSensorPath(DeviceSchema deviceSchema, String sensor) {
    return Constants.ROOT_SERIES_NAME + "." + deviceSchema.getGroup() + "." + deviceSchema
        .getDevice() + "." + sensor;
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
      for (Record record : batch.getRecords()) {
        String sql = getInsertOneBatchSql(batch.getDeviceSchema(), record.getTimestamp(),
            record.getRecordDataValue());
        statement.addBatch(sql);
      }
      statement.executeBatch();
      return new Status(true);
    } catch (Exception e) {
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    String sql = getRangeQuerySql(rangeQuery.getDeviceSchema(), rangeQuery.getStartTimestamp(),
        rangeQuery.getEndTimestamp());
    return executeQueryAndGetStatus(sql);
  }

  /**
   * SELECT max_value(s_76) FROM root.group_3.d_31 WHERE time >= 2010-01-01 12:00:00 AND time <=
   * 2010-01-01 12:30:00
   */
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    String aggQuerySqlHead = getAggQuerySqlHead(aggRangeQuery.getDeviceSchema(),
        aggRangeQuery.getAggFun());
    String sql = addWhereTimeClause(aggQuerySqlHead, aggRangeQuery.getStartTimestamp(),
        aggRangeQuery.getEndTimestamp());
    return executeQueryAndGetStatus(sql);
  }

  /**
   * SELECT UDF_name(s_3) FROM root.group_2.d_29 WHERE time >= 2010-01-01 12:00:00 AND time <=
   * 2010-01-01 12:30:00
   */
  @Override
  public Status rangedUDFQuery(RangedUDFQuery rangedUDFQuery) {
    if (!IS_UDF_REGISTRATED) {
      try {
        registerUDF(rangedUDFQuery.getUdfName(), rangedUDFQuery.getUdfFullClassName());
      } catch (Exception e) {
        return new Status(false, e, "UDF registration failed");
      }
    }
    String rangedUDFQuerySqlHead = getRangedUDFQuerySqlHead(rangedUDFQuery.getDeviceSchema(),
            rangedUDFQuery.getUdfName());
    String sql = addWhereTimeClause(rangedUDFQuerySqlHead, rangedUDFQuery.getStartTimestamp(),
            rangedUDFQuery.getEndTimestamp());
    return executeQueryAndGetStatus(sql);
  }

  /**
   * generate simple query header.
   *
   * @param devices schema list of query devices
   * @return Simple Query header. e.g. SELECT s_0, s_3 FROM root.group_0.d_1, root.group_1.d_2
   */
  private String getSimpleQuerySqlHead(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<String> querySensors = devices.get(0).getSensors();
    builder.append(querySensors.get(0));
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(querySensors.get(i));
    }
    return addFromClause(devices, builder);
  }

  private String getAggQuerySqlHead(List<DeviceSchema> devices, String aggFun) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<String> querySensors = devices.get(0).getSensors();
    builder.append(aggFun).append("(").append(querySensors.get(0)).append(")");
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(aggFun).append("(").append(querySensors.get(i)).append(")");
    }
    return addFromClause(devices, builder);
  }

  private String addFromClause(List<DeviceSchema> devices, StringBuilder builder) {
    builder.append(" FROM ").append(getDevicePath(devices.get(0)));
    for (int i = 1; i < devices.size(); i++) {
      builder.append(", ").append(getDevicePath(devices.get(i)));
    }
    return builder.toString();
  }

  private Status executeQueryAndGetStatus(String sql) {
    if (!config.IS_QUIET_MODE) {
      LOGGER.info("{} query SQL: {}", Thread.currentThread().getName(), sql);
    }
    AtomicInteger line = new AtomicInteger();
    AtomicInteger queryResultPointNum = new AtomicInteger();
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
      future = service.submit(() -> {
        try {
          try (ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
              line.getAndIncrement();
            }
          }
        } catch (SQLException e) {
          LOGGER.error("exception occurred when execute query={}", sql, e);
        }
        queryResultPointNum.set(line.get() * config.QUERY_SENSOR_NUM * config.QUERY_DEVICE_NUM);
      });

      try {
        future.get(config.READ_OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        future.cancel(true);
        return new Status(false, queryResultPointNum.get(), e, sql);
      }
      return new Status(true, queryResultPointNum.get());
    } catch (Exception e) {
      return new Status(false, queryResultPointNum.get(), e, sql);
    } catch (Throwable t) {
      return new Status(false, queryResultPointNum.get(), new Exception(t), sql);
    }
  }

  private String getRangeQuerySql(List<DeviceSchema> deviceSchemas, long start, long end) {
    return addWhereTimeClause(getSimpleQuerySqlHead(deviceSchemas), start, end);
  }

  private String addWhereTimeClause(String prefix, long start, long end) {
    String startTime = start + "";
    String endTime = end + "";
    return prefix + " WHERE time >= " + startTime + " AND time <= " + endTime;
  }

  private String getInsertOneBatchSql(DeviceSchema deviceSchema, long timestamp,
      List<Object> values) {
    StringBuilder builder = new StringBuilder();
    builder.append("insert into ")
        .append(Constants.ROOT_SERIES_NAME)
        .append(".").append(deviceSchema.getGroup())
        .append(".").append(deviceSchema.getDevice())
        .append("(timestamp");
    for (String sensor : deviceSchema.getSensors()) {
      builder.append(",").append(sensor);
    }
    builder.append(") values(");
    builder.append(timestamp);
    int sensorIndex = 0;
    for (Object value : values) {
      switch (SyntheticWorkload.getNextDataType(sensorIndex)) {
        case "BOOLEAN":
        case "INT32":
        case "INT64":
        case "FLOAT":
        case "DOUBLE":
          builder.append(",").append(value);
          break;
        case "TEXT":
          builder.append(",").append("'").append(value).append("'");
          break;
      }
      sensorIndex++;
    }
    builder.append(")");
    LOGGER.debug("getInsertOneBatchSql: {}", builder);
    return builder.toString();
  }

  private String getRangedUDFQuerySqlHead(List<DeviceSchema> devices, String udfName) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<String> querySensors = devices.get(0).getSensors();
    builder.append(udfName).append("(").append(querySensors.get(0)).append(")");
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(udfName).append("(").append(querySensors.get(i)).append(")");
    }
    return addFromClause(devices, builder);
  }

  public void registerUDF(String udfName, String udfFullClassName) throws TsdbException {
    try {
      Session udfSession = new Session(config.HOST, config.PORT, Constants.USER, Constants.PASSWD);
      udfSession.open(config.ENABLE_THRIFT_COMPRESSION);
      String registrationSql = String.format("CREATE FUNCTION %s AS \"%s\"", udfName, udfFullClassName);
      udfSession.executeNonQueryStatement(registrationSql);
      udfSession.close();
      IS_UDF_REGISTRATED = true;
    } catch (Exception e) {
      // ignore if given udf is already registrated
      if (!e.getMessage().contains(ALREADY_REGISTRATED_KEYWORTD) && !e.getMessage().contains("300")) {
        LOGGER.error("Register IoTDB UDF failed because ", e);
        throw new TsdbException(e);
      }
    }

  }

}
