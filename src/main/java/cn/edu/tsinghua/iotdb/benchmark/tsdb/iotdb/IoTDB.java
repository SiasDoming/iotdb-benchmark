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
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.UDFRangeQuery;
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

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class IoTDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
  private static final String ALREADY_KEYWORD = "already exist";
  private static final String ALREADY_REGISTERED_KEYWORD = "already been registered";
  private static final String QUERY_COMMON_DEVICE = "DEVICE";
  private static final String QUERY_COMMON_GROUP = "GROUP";
  private static final String QUERY_COMMON_ROOT = "ROOT";

  protected static Config config = ConfigDescriptor.getInstance().getConfig();
  private static Map<String, Boolean> IS_UDF_REGISTERED = new HashMap<>();

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
      for (String udfName : IS_UDF_REGISTERED.keySet()) {
        cleanupSql = String.format("DROP FUNCTION %s", udfName);
        cleanupSession.executeNonQueryStatement(cleanupSql);
      }
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
    return Constants.ROOT_SERIES_NAME + "." + deviceSchema.getGroup() + "." + deviceSchema.getDevice();
  }

  // convert deviceSchema and sensor to the format: root.group_1.d_1.s_1
  private String getSensorPath(DeviceSchema deviceSchema, String sensor) {
    return Constants.ROOT_SERIES_NAME + "." + deviceSchema.getGroup() + "."
            + deviceSchema.getDevice() + "." + sensor;
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

  private String getInsertOneBatchSql(DeviceSchema deviceSchema, long timestamp, List<Object> values) {
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

  /**
   * SELECT s_0 FROM root.group_0.d_1 WHERE time >= 2010-01-01 12:00:00 AND time <= 2010-01-01 12:30:00
   */
  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    StringBuilder builder = getSimpleQuerySqlHead(rangeQuery.getDeviceSchema());
    addFromClause(builder, rangeQuery.getDeviceSchema());
    addWhereTimeClause(builder, rangeQuery.getStartTimestamp(), rangeQuery.getEndTimestamp());
    return executeQueryAndGetStatus(builder.toString());
  }

  /**
   * generate simple query header.
   *
   * @param devices schema list of query devices
   * @return Simple Query header. e.g. SELECT s_0, s_3
   */
  private StringBuilder getSimpleQuerySqlHead(List<DeviceSchema> devices) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("SELECT ");
    String commonLevel = getCommonPrefixLevel(devices);
    for (DeviceSchema device : devices) {
      String groupName = device.getGroup();
      String deviceName = device.getDevice();
      for (String sensor : device.getSensors()) {
        switch (commonLevel) {
          case QUERY_COMMON_DEVICE:
            sqlBuilder.append(sensor);
            break;
          case QUERY_COMMON_GROUP:
            sqlBuilder.append(deviceName).append(".").append(sensor);
            break;
          case QUERY_COMMON_ROOT:
            sqlBuilder.append(groupName).append(".").append(deviceName).append(".").append(sensor);
            break;
        }
        sqlBuilder.append(", ");
      }
      // if all device schemas share common device and sensor, the SQL statement only need to specify sensors once
      // e.g. select udfName(s_1), udfName(s_2) from root.group_0.d_0, root.group_1.d_1
      // outputs udfName(root.group_0.d_0.s_1), udfName(root.group_0.d_0.s_2), udfName(root.group_1.d_1.s_1), udfName(root.group_1.d_1.s_2)
      if (commonLevel.equals(QUERY_COMMON_DEVICE)) {
        break;
      }
    }
    // remove the last redundant ", "
    sqlBuilder.delete(sqlBuilder.length() - 2, sqlBuilder.length());
    return sqlBuilder;
  }

  /**
   * SELECT max_value(s_3) FROM root.group_2.d_1 WHERE time >= 2010-01-01 12:00:00 AND time <= 2010-01-01 12:30:00
   */
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    StringBuilder builder = getAggQuerySqlHead(aggRangeQuery.getDeviceSchema(), aggRangeQuery.getAggFun());
    addFromClause(builder, aggRangeQuery.getDeviceSchema());
    addWhereTimeClause(builder, aggRangeQuery.getStartTimestamp(), aggRangeQuery.getEndTimestamp());
    return executeQueryAndGetStatus(builder.toString());
  }

  /**
   * generate aggregation query header.
   *
   * @param devices schema list of query devices
   * @param aggFun aggregation function name
   * @return Aggregation Query header. e.g. SELECT aggFun(s_0), aggFun(s_3)
   */
  private StringBuilder getAggQuerySqlHead(List<DeviceSchema> devices, String aggFun) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("SELECT ");
    String commonLevel = getCommonPrefixLevel(devices);
    for (DeviceSchema device : devices) {
      String groupName = device.getGroup();
      String deviceName = device.getDevice();
      for (String sensor : device.getSensors()) {
        sqlBuilder.append(aggFun).append("(");
        switch (commonLevel) {
          case QUERY_COMMON_DEVICE:
            sqlBuilder.append(sensor);
            break;
          case QUERY_COMMON_GROUP:
            sqlBuilder.append(deviceName).append(".").append(sensor);
            break;
          case QUERY_COMMON_ROOT:
            sqlBuilder.append(groupName).append(".").append(deviceName).append(".").append(sensor);
            break;
        }
        sqlBuilder.append("), ");
      }
      if (commonLevel.equals(QUERY_COMMON_DEVICE)) {
        break;
      }
    }
    // remove the last redundant ", "
    sqlBuilder.delete(sqlBuilder.length() - 2, sqlBuilder.length());
    return sqlBuilder;
  }

  /**
   * SELECT udfName(s_3) FROM root.group_2.d_2 WHERE time >= 2010-01-01 12:00:00 AND time <= 2010-01-01 12:30:00
   */
  @Override
  public Status udfRangeQuery(UDFRangeQuery udfRangeQuery) {
    if (!IS_UDF_REGISTERED.getOrDefault(udfRangeQuery.getUdfName(), false)) {
      try {
        registerUDF(udfRangeQuery.getUdfName(), udfRangeQuery.getUdfFullClassName());
      } catch (Exception e) {
        return new Status(false, e, "UDF registration failed");
      }
    }
    StringBuilder sqlBuilder = getRangedUDFQuerySqlHead(udfRangeQuery.getDeviceSchema(), udfRangeQuery);
    addFromClause(sqlBuilder, udfRangeQuery.getDeviceSchema());
    addWhereTimeClause(sqlBuilder, udfRangeQuery.getStartTimestamp(), udfRangeQuery.getEndTimestamp());
    return executeQueryAndGetStatus(sqlBuilder.toString());
  }

  /**
   * generate UDF query header.
   *
   * @param devices schema list of query devices
   * @param udfRangeQuery UDF Query information
   * @return UDF Query header. e.g. SELECT udfName(s_0, s_1), udfName(s_3, s_5)
   */
  private StringBuilder getRangedUDFQuerySqlHead(List<DeviceSchema> devices, UDFRangeQuery udfRangeQuery) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("SELECT ");
    String commonLevel = getCommonPrefixLevel(devices);
    int timeSeriesCount = 0;
    for (DeviceSchema device : devices) {
      String groupName = device.getGroup();
      String deviceName = device.getDevice();
      for (String sensor : device.getSensors()) {
        if (timeSeriesCount == 0) {
          sqlBuilder.append(udfRangeQuery.getUdfName()).append("(");
        }
        switch (commonLevel) {
          case QUERY_COMMON_DEVICE:
            sqlBuilder.append(sensor);
            break;
          case QUERY_COMMON_GROUP:
            sqlBuilder.append(deviceName).append(".").append(sensor);
            break;
          case QUERY_COMMON_ROOT:
            sqlBuilder.append(groupName).append(".").append(deviceName).append(".").append(sensor);
            break;
        }
        timeSeriesCount ++;
        if (timeSeriesCount == udfRangeQuery.getTimeSeriesNumber()) {
          for (Map.Entry<String, String> argument : udfRangeQuery.getArguments().entrySet()) {
            sqlBuilder.append(", \"").append(argument.getKey()).append("\"=\"").append(argument.getValue()).append("\"");
          }
          sqlBuilder.append("), ");
          timeSeriesCount = 0;
        } else {
          sqlBuilder.append(", ");
        }
      }
      if (commonLevel.equals(QUERY_COMMON_DEVICE)) {
        break;
      }
    }
    // remove the last redundant ", "
    sqlBuilder.delete(sqlBuilder.length() - 2, sqlBuilder.length());
    return sqlBuilder;
  }

  public void registerUDF(String udfName, String udfFullClassName) throws TsdbException {
    if (IS_UDF_REGISTERED.getOrDefault(udfName, false)) {
      return;
    }
    try {
      Session udfSession = new Session(config.HOST, config.PORT, Constants.USER, Constants.PASSWD);
      udfSession.open(config.ENABLE_THRIFT_COMPRESSION);
      String registrationSql = String.format("CREATE FUNCTION %s AS \"%s\"", udfName, udfFullClassName);
      udfSession.executeNonQueryStatement(registrationSql);
      udfSession.close();
      IS_UDF_REGISTERED.put(udfName, true);
    } catch (Exception e) {
      // ignore if given udf is already registered
      if (e.getMessage().contains(ALREADY_REGISTERED_KEYWORD)) {
        IS_UDF_REGISTERED.put(udfName, true);
      } else {
        LOGGER.error("Register IoTDB UDF failed because ", e);
        throw new TsdbException(e);
      }
    }
  }

  private StringBuilder addFromClause(StringBuilder sqlBuilder, List<DeviceSchema> devices) {
    sqlBuilder.append(" FROM ");
    switch (getCommonPrefixLevel(devices)) {
      case QUERY_COMMON_DEVICE:
        sqlBuilder.append(getDevicePath(devices.get(0)));
        for (int i = 1; i < devices.size(); i++) {
          sqlBuilder.append(", ").append(getDevicePath(devices.get(i)));
        }
        break;
      case QUERY_COMMON_GROUP:
        sqlBuilder.append(devices.get(0).getGroup());
        break;
      case QUERY_COMMON_ROOT:
        sqlBuilder.append(Constants.ROOT_SERIES_NAME);
        break;
    }
    return sqlBuilder;
  }

  private String getCommonPrefixLevel(List<DeviceSchema> devices) {
    boolean commonDevice = true;
    List<String> querySensors = devices.get(0).getSensors();
    for (int i = 1; i < devices.size(); i++) {
      for (String sensor : devices.get(i).getSensors()) {
        if (!querySensors.contains(sensor)) {
          commonDevice = false;
          break;
        }
      }
    }
    if (commonDevice) {
      return QUERY_COMMON_DEVICE;
    }

    boolean commonGroup = true;
    String queryGroup = devices.get(0).getGroup();
    for (int i = 1; i < devices.size(); i++) {
      if (!devices.get(i).getGroup().equals(queryGroup)) {
        commonGroup = false;
        break;
      }
    }
    if (commonGroup) {
      return QUERY_COMMON_GROUP;
    } else {
      return QUERY_COMMON_ROOT;
    }
  }

  private StringBuilder addWhereTimeClause(StringBuilder sqlBuilder, long start, long end) {
    sqlBuilder.append(" WHERE time >= ").append(start).append(" AND time <= ").append(end);
    return sqlBuilder;
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
}
