package cn.edu.tsinghua.iotdb.benchmark.measurement;

import cn.edu.tsinghua.iotdb.benchmark.client.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.Metric;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.TotalOperationResult;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.TotalResult;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.PersistenceFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.quantile.TDigest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.text.SimpleDateFormat;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Date;

public class Measurement {

  private static final Logger LOGGER = LoggerFactory.getLogger(Measurement.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static Map<Operation, Map<String, TDigest>> operationLatencyDigest = new EnumMap<>(Operation.class);
  private static Map<Operation, Map<String, Double>> operationLatencySumAllClient = new EnumMap<>(Operation.class);
  private static final String OPERATION_ITEM = "%-30s";
  private static final String RESULT_ITEM = "%-20s";
  private static final String LATENCY_ITEM = "%-12s";
  private static final int COMPRESSION = 100;

  private double createSchemaTime;
  private double elapseTime;
  private Map<Operation, Map<String, Long>> okOperationNumMap;
  private Map<Operation, Map<String, Long>> failOperationNumMap;
  private Map<Operation, Map<String, Long>> okPointNumMap;
  private Map<Operation, Map<String, Long>> failPointNumMap;
  private Map<Operation, Map<String, Double>> operationLatencySumThisClient;

  static {
    for (Operation operation : Operation.values()) {
      operationLatencyDigest.put(operation, new HashMap<>());
      operationLatencySumAllClient.put(operation, new HashMap<>());
      switch (operation) {
        case INGESTION:
        case RANGE_QUERY:
          operationLatencyDigest.get(operation).put("NONE", new TDigest(COMPRESSION));
          operationLatencySumAllClient.get(operation).put("NONE", 0D);
          break;
        case AGG_RANGE_QUERY:
          operationLatencyDigest.get(operation).put(config.QUERY_AGGREGATE_FUN, new TDigest(COMPRESSION));
          operationLatencySumAllClient.get(operation).put(config.QUERY_AGGREGATE_FUN, 0D);
          break;
        case RANGED_UDF_QUERY:
          for (String udfName : config.QUERY_UDF_NAME_LIST) {
            operationLatencyDigest.get(operation).put(udfName, new TDigest(COMPRESSION));
            operationLatencySumAllClient.get(operation).put(udfName, 0D);
          }
          break;
      }
    }
  }

  public Measurement() {
    okOperationNumMap = new EnumMap<>(Operation.class);
    failOperationNumMap = new EnumMap<>(Operation.class);
    okPointNumMap = new EnumMap<>(Operation.class);
    failPointNumMap = new EnumMap<>(Operation.class);
    operationLatencySumThisClient = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      okOperationNumMap.put(operation, new HashMap<>());
      failOperationNumMap.put(operation, new HashMap<>());
      okPointNumMap.put(operation, new HashMap<>());
      failPointNumMap.put(operation, new HashMap<>());
      operationLatencySumThisClient.put(operation, new HashMap<>());
      switch (operation) {
        case INGESTION:
        case RANGE_QUERY:
          okOperationNumMap.get(operation).put("NONE", 0L);
          failOperationNumMap.get(operation).put("NONE", 0L);
          okPointNumMap.get(operation).put("NONE", 0L);
          failPointNumMap.get(operation).put("NONE", 0L);
          operationLatencySumThisClient.get(operation).put("NONE", 0D);
          break;
        case AGG_RANGE_QUERY:
          okOperationNumMap.get(operation).put(config.QUERY_AGGREGATE_FUN, 0L);
          failOperationNumMap.get(operation).put(config.QUERY_AGGREGATE_FUN, 0L);
          okPointNumMap.get(operation).put(config.QUERY_AGGREGATE_FUN, 0L);
          failPointNumMap.get(operation).put(config.QUERY_AGGREGATE_FUN, 0L);
          operationLatencySumThisClient.get(operation).put(config.QUERY_AGGREGATE_FUN, 0D);
          break;
        case RANGED_UDF_QUERY:
          for (String udfName : config.QUERY_UDF_NAME_LIST) {
            okOperationNumMap.get(operation).put(udfName, 0L);
            failOperationNumMap.get(operation).put(udfName, 0L);
            okPointNumMap.get(operation).put(udfName, 0L);
            failPointNumMap.get(operation).put(udfName, 0L);
            operationLatencySumThisClient.get(operation).put(udfName, 0D);
          }
          break;
      }
    }
  }

  private Map<Operation, Map<String, Double>> getOperationLatencySumThisClient() {
    return operationLatencySumThisClient;
  }

  private long getOkOperationNum(Operation operation) {
    switch (operation) {
      case INGESTION:
      case RANGE_QUERY:
        return getOkOperationNum(operation, "NONE");
      case AGG_RANGE_QUERY:
      case RANGED_UDF_QUERY:
        long sum = 0L;
        for (Long value : okOperationNumMap.get(operation).values()) {
          sum += value;
        }
        return sum;
      default:
        return 0L;
    }
  }

  private long getOkOperationNum(Operation operation, String funcName) {
    return okOperationNumMap.get(operation).get(funcName);
  }

  private long getFailOperationNum(Operation operation) {
    switch (operation) {
      case INGESTION:
      case RANGE_QUERY:
        return getFailOperationNum(operation, "NONE");
      case AGG_RANGE_QUERY:
      case RANGED_UDF_QUERY:
        long sum = 0L;
        for (Long value : failOperationNumMap.get(operation).values()) {
          sum += value;
        }
        return sum;
      default:
        return 0L;
    }
  }

  private long getFailOperationNum(Operation operation, String funcName) {
    return failOperationNumMap.get(operation).get(funcName);
  }

  private long getOkPointNum(Operation operation) {
    switch (operation) {
      case INGESTION:
      case RANGE_QUERY:
        return getOkPointNum(operation, "NONE");
      case AGG_RANGE_QUERY:
      case RANGED_UDF_QUERY:
        long sum = 0L;
        for (Long value : okPointNumMap.get(operation).values()) {
          sum += value;
        }
        return sum;
      default:
        return 0L;
    }
  }

  private long getOkPointNum(Operation operation, String funcName) {
    return okPointNumMap.get(operation).get(funcName);
  }

  private long getFailPointNum(Operation operation) {
    switch (operation) {
      case INGESTION:
      case RANGE_QUERY:
        return getFailPointNum(operation, "NONE");
      case AGG_RANGE_QUERY:
      case RANGED_UDF_QUERY:
        long sum = 0L;
        for (Long value : failPointNumMap.get(operation).values()) {
          sum += value;
        }
        return sum;
      default:
        return 0L;
    }
  }

  private long getFailPointNum(Operation operation, String funcName) {
    return failPointNumMap.get(operation).get(funcName);
  }

  public void addOperationLatency(Operation operation, double latency) {
    switch (operation) {
      case INGESTION:
      case RANGE_QUERY:
        addOperationLatency(operation, "NONE", latency);
        break;
      case AGG_RANGE_QUERY:
      case RANGED_UDF_QUERY:
      default:
        LOGGER.debug("Unspecified operation while recording {} latency", operation.getName());
        break;
    }
  }

  public void addOperationLatency(Operation operation, String funcName, double latency) {
    synchronized (operationLatencyDigest.get(operation).get(funcName)) {
      operationLatencyDigest.get(operation).get(funcName).add(latency);
    }
    operationLatencySumThisClient.get(operation).put(funcName, operationLatencySumThisClient.get(operation).get(funcName) + latency);
  }

  public void addOkPointNum(Operation operation, int pointNum) {
    switch (operation) {
      case INGESTION:
      case RANGE_QUERY:
        addOkPointNum(operation, "NONE", pointNum);
        break;
      case AGG_RANGE_QUERY:
      case RANGED_UDF_QUERY:
      default:
        LOGGER.debug("Unspecified operation while recording {} ok point number", operation.getName());
        break;
    }
  }

  public void addOkPointNum(Operation operation, String funcName, int pointNum) {
    okPointNumMap.get(operation).put(funcName, okPointNumMap.get(operation).get(funcName) + pointNum);
  }

  public void addFailPointNum(Operation operation, int pointNum) {
    switch (operation) {
      case INGESTION:
      case RANGE_QUERY:
        addFailPointNum(operation, "NONE", pointNum);
        break;
      case AGG_RANGE_QUERY:
      case RANGED_UDF_QUERY:
      default:
        LOGGER.debug("Unspecified operation while recording {} fail point number", operation.getName());
        break;
    }
  }

  public void addFailPointNum(Operation operation, String funcName, int pointNum) {
    failPointNumMap.get(operation).put(funcName, failPointNumMap.get(operation).get(funcName) + pointNum);
  }

  public void addOkOperationNum(Operation operation) {
    switch (operation) {
      case INGESTION:
      case RANGE_QUERY:
        addOkOperationNum(operation, "NONE");
        break;
      case AGG_RANGE_QUERY:
      case RANGED_UDF_QUERY:
      default:
        LOGGER.debug("Unspecified operation while recording {} ok operation number", operation.getName());
        break;
    }
  }

  public void addOkOperationNum(Operation operation, String funcName) {
    okOperationNumMap.get(operation).put(funcName, okOperationNumMap.get(operation).get(funcName) + 1);
  }

  public void addFailOperationNum(Operation operation) {
    switch (operation) {
      case INGESTION:
      case RANGE_QUERY:
        addFailOperationNum(operation, "NONE");
        break;
      case AGG_RANGE_QUERY:
      case RANGED_UDF_QUERY:
      default:
        LOGGER.debug("Unspecified operation while recording {} fail operation number", operation.getName());
        break;
    }
  }

  public void addFailOperationNum(Operation operation, String funcName) {
    failOperationNumMap.get(operation).put(funcName, failOperationNumMap.get(operation).get(funcName) + 1);
  }

  public double getCreateSchemaTime() {
    return createSchemaTime;
  }

  public void setCreateSchemaTime(double createSchemaTime) {
    this.createSchemaTime = createSchemaTime;
  }

  public double getElapseTime() {
    return elapseTime;
  }

  public void setElapseTime(double elapseTime) {
    this.elapseTime = elapseTime;
  }

  /**
   * users need to call calculateMetrics() after calling mergeMeasurement() to update metrics.
   *
   * @param m measurement to be merged
   */
  public void mergeMeasurement(Measurement m) {
    for (Operation operation : Operation.values()) {
      for (String funcName : okOperationNumMap.get(operation).keySet()) {
        okOperationNumMap.get(operation).put(funcName, okOperationNumMap.get(operation).get(funcName) + m.getOkOperationNum(operation, funcName));
        failOperationNumMap.get(operation).put(funcName, failOperationNumMap.get(operation).get(funcName) + m.getFailOperationNum(operation, funcName));
        okPointNumMap.get(operation).put(funcName, okPointNumMap.get(operation).get(funcName) + m.getOkPointNum(operation, funcName));
        failPointNumMap.get(operation).put(funcName, failPointNumMap.get(operation).get(funcName) + m.getFailPointNum(operation, funcName));
        // set operationLatencySumThisClient of this measurement the largest latency sum among all threads
        if(operationLatencySumThisClient.get(operation).get(funcName) < m.getOperationLatencySumThisClient().get(operation).get(funcName)) {
          operationLatencySumThisClient.get(operation).put(funcName, m.getOperationLatencySumThisClient().get(operation).get(funcName));
        }
        operationLatencySumAllClient.get(operation).put(funcName,
                operationLatencySumAllClient.get(operation).get(funcName) + m.getOperationLatencySumThisClient().get(operation).get(funcName));
      }
    }
  }

  public void calculateMetrics() {
    for (Operation operation : Operation.values()) {
      for (String funcName : okOperationNumMap.get(operation).keySet()) {
        double avgLatency = 0;
        if (okOperationNumMap.get(operation).get(funcName) != 0) {
          avgLatency = operationLatencySumAllClient.get(operation).get(funcName) / okOperationNumMap.get(operation).get(funcName);
          Metric.AVG_LATENCY.getTypeValueMap().get(operation).put(funcName, avgLatency);
          Metric.MAX_THREAD_LATENCY_SUM.getTypeValueMap().get(operation).put(funcName, operationLatencySumThisClient.get(operation).get(funcName));
          Metric.MIN_LATENCY.getTypeValueMap().get(operation).put(funcName, operationLatencyDigest.get(operation).get(funcName).quantile(0.0));
          Metric.MAX_LATENCY.getTypeValueMap().get(operation).put(funcName, operationLatencyDigest.get(operation).get(funcName).quantile(1.0));
          Metric.P10_LATENCY.getTypeValueMap().get(operation).put(funcName, operationLatencyDigest.get(operation).get(funcName).quantile(0.1));
          Metric.P25_LATENCY.getTypeValueMap().get(operation).put(funcName, operationLatencyDigest.get(operation).get(funcName).quantile(0.25));
          Metric.MEDIAN_LATENCY.getTypeValueMap().get(operation).put(funcName, operationLatencyDigest.get(operation).get(funcName).quantile(0.5));
          Metric.P75_LATENCY.getTypeValueMap().get(operation).put(funcName, operationLatencyDigest.get(operation).get(funcName).quantile(0.75));
          Metric.P90_LATENCY.getTypeValueMap().get(operation).put(funcName, operationLatencyDigest.get(operation).get(funcName).quantile(0.90));
          Metric.P95_LATENCY.getTypeValueMap().get(operation).put(funcName, operationLatencyDigest.get(operation).get(funcName).quantile(0.95));
          Metric.P99_LATENCY.getTypeValueMap().get(operation).put(funcName, operationLatencyDigest.get(operation).get(funcName).quantile(0.99));
          Metric.P999_LATENCY.getTypeValueMap().get(operation).put(funcName, operationLatencyDigest.get(operation).get(funcName).quantile(0.999));
        }
      }
    }
  }

  public void showMeasurements() {
    PersistenceFactory persistenceFactory = new PersistenceFactory();
    ITestDataPersistence recorder = persistenceFactory.getPersistence();
    System.out.println(Thread.currentThread().getName() + " measurements:");
    System.out.println("Create schema cost " + String.format("%.3f", createSchemaTime) + " second");
    System.out.println("Test elapsed time (not include schema creation): " + String.format("%.3f", elapseTime) + " second");
    recorder.saveResult("total", TotalResult.CREATE_SCHEMA_TIME.getName(), "" + createSchemaTime);
    recorder.saveResult("total", TotalResult.ELAPSED_TIME.getName(), "" + elapseTime);

    System.out.println(
            "------------------------------------------------------------Result Matrix------------------------------------------------------------");
    StringBuilder format = new StringBuilder();
    format.append(OPERATION_ITEM);
    for (int i = 0; i < 5; i++) {
      format.append(RESULT_ITEM);
    }
    format.append("\n");
    System.out.printf(format.toString(), "Operation", "okOperation", "okPoint", "failOperation", "failPoint", "throughput(point/s)");
    for (Operation operation : Operation.values()) {
      String throughput, operationTitle;
      switch (operation) {
        case INGESTION:
        case RANGE_QUERY:
          throughput = String.format("%.3f", okPointNumMap.get(operation).get("NONE") / elapseTime);
          System.out.printf(format.toString(), operation.getName(), okOperationNumMap.get(operation).get("NONE"), okPointNumMap.get(operation).get("NONE"),
                  failOperationNumMap.get(operation).get("NONE"), failPointNumMap.get(operation).get("NONE"), throughput);
          recorder.saveResult(operation.toString(), TotalOperationResult.OK_OPERATION_NUM.getName(), "" + okOperationNumMap.get(operation).get("NONE"));
          recorder.saveResult(operation.toString(), TotalOperationResult.OK_POINT_NUM.getName(), "" + okPointNumMap.get(operation).get("NONE"));
          recorder.saveResult(operation.toString(), TotalOperationResult.FAIL_OPERATION_NUM.getName(), "" + failOperationNumMap.get(operation).get("NONE"));
          recorder.saveResult(operation.toString(), TotalOperationResult.FAIL_POINT_NUM.getName(), "" + failPointNumMap.get(operation).get("NONE"));
          recorder.saveResult(operation.toString(), TotalOperationResult.THROUGHPUT.getName(), throughput);
          break;
        case AGG_RANGE_QUERY:
          String aggFuncName = config.QUERY_AGGREGATE_FUN;
          operationTitle = operation.getName() + ": " + aggFuncName;
          throughput = String.format("%.3f", okPointNumMap.get(operation).get(aggFuncName) / elapseTime);
          System.out.printf(format.toString(), operationTitle, okOperationNumMap.get(operation).get(aggFuncName), okPointNumMap.get(operation).get(aggFuncName),
                  failOperationNumMap.get(operation).get(aggFuncName), failPointNumMap.get(operation).get(aggFuncName), throughput);
          recorder.saveResult(operationTitle, TotalOperationResult.OK_OPERATION_NUM.getName(), "" + okOperationNumMap.get(operation).get(aggFuncName));
          recorder.saveResult(operationTitle, TotalOperationResult.OK_POINT_NUM.getName(), "" + okPointNumMap.get(operation).get(aggFuncName));
          recorder.saveResult(operationTitle, TotalOperationResult.FAIL_OPERATION_NUM.getName(), "" + failOperationNumMap.get(operation).get(aggFuncName));
          recorder.saveResult(operationTitle, TotalOperationResult.FAIL_POINT_NUM.getName(), "" + failPointNumMap.get(operation).get(aggFuncName));
          recorder.saveResult(operationTitle, TotalOperationResult.THROUGHPUT.getName(), throughput);
          break;
        case RANGED_UDF_QUERY:
          for (String udfName : config.QUERY_UDF_NAME_LIST) {
            operationTitle = operation.getName() + ": " + udfName;
            throughput = String.format("%.3f", okPointNumMap.get(operation).get(udfName) / elapseTime);
            System.out.printf(format.toString(), operationTitle, okOperationNumMap.get(operation).get(udfName), okPointNumMap.get(operation).get(udfName),
                    failOperationNumMap.get(operation).get(udfName), failPointNumMap.get(operation).get(udfName), throughput);
            recorder.saveResult(operationTitle, TotalOperationResult.OK_OPERATION_NUM.getName(), "" + okOperationNumMap.get(operation).get(udfName));
            recorder.saveResult(operationTitle, TotalOperationResult.OK_POINT_NUM.getName(), "" + okPointNumMap.get(operation).get(udfName));
            recorder.saveResult(operationTitle, TotalOperationResult.FAIL_OPERATION_NUM.getName(), "" + failOperationNumMap.get(operation).get(udfName));
            recorder.saveResult(operationTitle, TotalOperationResult.FAIL_POINT_NUM.getName(), "" + failPointNumMap.get(operation).get(udfName));
            recorder.saveResult(operationTitle, TotalOperationResult.THROUGHPUT.getName(), throughput);
          }
          break;
      }
    }
    System.out.println(
            "-------------------------------------------------------------------------------------------------------------------------------------");
    recorder.close();
  }

  public void showConfigs() {
    System.out.println("----------------------Main Configurations----------------------");
    System.out.println("OPERATION_PROPORTION: " + config.OPERATION_PROPORTION);
    System.out.println("ENABLE_THRIFT_COMPRESSION: " + config.ENABLE_THRIFT_COMPRESSION);
    System.out.println("INSERT_DATATYPE_PROPORTION: " + config.INSERT_DATATYPE_PROPORTION);
    System.out.println("ENCODING(BOOLEAN/INT32/INT64/FLOAT/DOUBLE/TEXT): " + config.ENCODING_BOOLEAN
                                                                     + "/" + config.ENCODING_INT32
                                                                     + "/" + config.ENCODING_INT64
                                                                     + "/" + config.ENCODING_FLOAT
                                                                     + "/" + config.ENCODING_DOUBLE
                                                                     + "/" + config.ENCODING_TEXT);
    System.out.println("IS_CLIENT_BIND: " + config.IS_CLIENT_BIND);
    System.out.println("CLIENT_NUMBER: " + config.CLIENT_NUMBER);
    System.out.println("GROUP_NUMBER: " + config.GROUP_NUMBER);
    System.out.println("DEVICE_NUMBER: " + config.DEVICE_NUMBER);
    System.out.println("SENSOR_NUMBER: " + config.SENSOR_NUMBER);
    System.out.println("BATCH_SIZE: " + config.BATCH_SIZE);
    System.out.println("LOOP: " + config.LOOP);
    System.out.println("POINT_STEP: "+ config.POINT_STEP);
    System.out.println("QUERY_INTERVAL: " + config.QUERY_INTERVAL);
    System.out.println("IS_OVERFLOW: " + config.IS_OVERFLOW);
    System.out.println("OVERFLOW_MODE: " + config.OVERFLOW_MODE);
    System.out.println("OVERFLOW_RATIO: " + config.OVERFLOW_RATIO);
    System.out.println("---------------------------------------------------------------");
  }

  public void showMetrics() {
    PersistenceFactory persistenceFactory = new PersistenceFactory();
    ITestDataPersistence recorder = persistenceFactory.getPersistence();
    System.out.println(
            "---------------------------------------------------------------------------Latency (ms) Matrix---------------------------------------------------------------------------");
    System.out.printf(OPERATION_ITEM, "Operation");
    for (Metric metric : Metric.values()) {
      System.out.printf(LATENCY_ITEM, metric.name);
    }
    System.out.println();
    for (Operation operation : Operation.values()) {
      String operationTitle;
      switch (operation) {
        case INGESTION:
        case RANGE_QUERY:
          System.out.printf(OPERATION_ITEM, operation.getName());
          for (Metric metric : Metric.values()) {
            String metricResult = String.format("%.3f", metric.typeValueMap.get(operation).get("NONE"));
            System.out.printf(LATENCY_ITEM, metricResult);
            recorder.saveResult(operation.toString(), metric.name, metricResult);
          }
          System.out.println();
          break;
        case AGG_RANGE_QUERY:
          String aggFuncName = config.QUERY_AGGREGATE_FUN;
          operationTitle = operation.getName() + ": " + aggFuncName;
          System.out.printf(OPERATION_ITEM, operationTitle);
          for (Metric metric : Metric.values()) {
            String metricResult = String.format("%.3f", metric.typeValueMap.get(operation).get(aggFuncName));
            System.out.printf(LATENCY_ITEM, metricResult);
            recorder.saveResult(operation.toString(), metric.name, metricResult);
          }
          System.out.println();
          break;
        case RANGED_UDF_QUERY:
          for (String udfName : config.QUERY_UDF_NAME_LIST) {
            operationTitle = operation.getName() + ": " + udfName;
            System.out.printf(OPERATION_ITEM, operationTitle);
            for (Metric metric : Metric.values()) {
              String metricResult = String.format("%.3f", metric.typeValueMap.get(operation).get(udfName));
              System.out.printf(LATENCY_ITEM, metricResult);
              recorder.saveResult(operation.toString(), metric.name, metricResult);
            }
            System.out.println();
          }
          break;
      }
    }
    System.out.println(
            "-------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
    recorder.close();
  }

  public void outputCSV() {

    try {
      String fileName = createFileName();
      File csv = new File(fileName);
      createDirectory();
      csv.createNewFile();
      outputConfigToCSV(csv);
      outputResultMatrixToCSV(csv);
      outputMetricsToCSV(csv);

    } catch (IOException e) {
      LOGGER.error("Exception occurred during writing csv file because: ", e);
    }
  }

  private String createFileName() {
    // Formatting time
    SimpleDateFormat sdf = new SimpleDateFormat();
    sdf.applyPattern("yyyy-MM-dd-HH-mm-ss");
    Date date = new Date();
    String currentTime = sdf.format(date);

    // Formatting current Operations
    StringBuilder fileNameSB = new StringBuilder();
    String[] operations = config.OPERATION_PROPORTION.split(":");

    for (int i = 0; i < operations.length; i++) {
      // Specify inserting or querying mode
      if (i == 0) {
        fileNameSB.append("I");
      } else if (i == 1) {
        fileNameSB.append("Q");
      }
      // Specify whether a specific operation is processed in this time of test.
      if (!operations[i].equals("0")) {
        if (i == 0) {
          fileNameSB.append("1");
        } else {
          fileNameSB.append(i);
        }
      } else {
        fileNameSB.append("0");
      }
    }
    return "data/csvOutput/" + fileNameSB.toString() + "-" + currentTime + "-test-result.csv";
  }

  private void createDirectory() {
    File folder = new File("data/csvOutput");
    if (!folder.exists() && !folder.isDirectory()) {
      folder.mkdirs();
    }
  }

  private void outputConfigToCSV(File csv) {
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
      bw.write("Main Configurations");
      bw.newLine();
      bw.write("OPERATION_PROPORTION," + config.OPERATION_PROPORTION);
      bw.newLine();
      bw.write("ENABLE_THRIFT_COMPRESSION," + config.ENABLE_THRIFT_COMPRESSION);
      bw.newLine();
      bw.write("INSERT_DATATYPE_PROPORTION," + config.INSERT_DATATYPE_PROPORTION);
      bw.newLine();
      bw.write("ENCODING(BOOLEAN/INT32/INT64/FLOAT/DOUBLE/TEXT)," + config.ENCODING_BOOLEAN
                                                                + "/" + config.ENCODING_INT32
                                                                + "/" + config.ENCODING_INT64
                                                                + "/" + config.ENCODING_FLOAT
                                                                + "/" + config.ENCODING_DOUBLE
                                                                + "/" + config.ENCODING_TEXT);
      bw.newLine();
      bw.write("IS_CLIENT_BIND," + config.IS_CLIENT_BIND);
      bw.newLine();
      bw.write("CLIENT_NUMBER," + config.CLIENT_NUMBER);
      bw.newLine();
      bw.write("GROUP_NUMBER," + config.GROUP_NUMBER);
      bw.newLine();
      bw.write("DEVICE_NUMBER," + config.DEVICE_NUMBER);
      bw.newLine();
      bw.write("SENSOR_NUMBER," + config.SENSOR_NUMBER);
      bw.newLine();
      bw.write("BATCH_SIZE," + config.BATCH_SIZE);
      bw.newLine();
      bw.write("LOOP," + config.LOOP);
      bw.newLine();
      bw.write("POINT_STEP,"+ config.POINT_STEP);
      bw.newLine();
      bw.write("QUERY_INTERVAL," + config.QUERY_INTERVAL);
      bw.newLine();
      bw.write("IS_OVERFLOW," + config.IS_OVERFLOW);
      bw.newLine();
      bw.write("OVERFLOW_MODE," + config.OVERFLOW_MODE);
      bw.newLine();
      bw.write("OVERFLOW_RATIO," + config.OVERFLOW_RATIO);
      bw.close();
    } catch (IOException e) {
      LOGGER.error("Exception occurred during operating buffer writer because: ", e);
    }
  }

  private void outputResultMatrixToCSV(File csv) {
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
      bw.newLine();
      bw.write("Result Matrix");
      bw.newLine();
      bw.write("Operation" + "," + "okOperation" + "," + "okPoint" + "," + "failOperation"
              + "," + "failPoint" + "," + "throughput(point/s)");
      for (Operation operation : Operation.values()) {
        String throughput, operationTitle;
        switch (operation) {
          case INGESTION:
          case RANGE_QUERY:
            throughput = String.format("%.3f", okPointNumMap.get(operation).get("NONE") / elapseTime);
            bw.newLine();
            bw.write(operation.getName() + "," + okOperationNumMap.get(operation).get("NONE") + "," + okPointNumMap.get(operation).get("NONE")
                    + "," + failOperationNumMap.get(operation).get("NONE") + "," + failPointNumMap.get(operation).get("NONE") + "," + throughput);
            break;
          case AGG_RANGE_QUERY:
            String aggFuncName = config.QUERY_AGGREGATE_FUN;
            operationTitle = operation.getName() + ": " + aggFuncName;
            throughput = String.format("%.3f", okPointNumMap.get(operation).get(aggFuncName) / elapseTime);
            bw.newLine();
            bw.write(operationTitle + "," + okOperationNumMap.get(operation).get(aggFuncName) + "," + okPointNumMap.get(operation).get(aggFuncName)
                    + "," + failOperationNumMap.get(operation).get(aggFuncName) + "," + failPointNumMap.get(operation).get(aggFuncName) + "," + throughput);
            break;
          case RANGED_UDF_QUERY:
            for (String udfName : config.QUERY_UDF_NAME_LIST) {
              operationTitle = operation.getName() + ": " + udfName;
              throughput = String.format("%.3f", okPointNumMap.get(operation).get(udfName) / elapseTime);
              bw.newLine();
              bw.write(operationTitle + "," + okOperationNumMap.get(operation).get(udfName) + "," + okPointNumMap.get(operation).get(udfName)
                      + "," + failOperationNumMap.get(operation).get(udfName) + "," + failPointNumMap.get(operation).get(udfName) + "," + throughput);
            }
            break;
        }
      }
      bw.close();
    } catch (IOException e) {
      LOGGER.error("Exception occurred during operating buffer writer because: ", e);
    }
  }

  private void outputMetricsToCSV(File csv) {
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));
      bw.newLine();
      bw.write("Latency (ms) Matrix");
      bw.newLine();
      bw.write("Operation");
      for (Metric metric : Metric.values()) {
        bw.write("," + metric.name);
      }
      bw.newLine();
      for (Operation operation : Operation.values()) {
        String throughput, operationTitle;
        switch (operation) {
          case INGESTION:
          case RANGE_QUERY:
            bw.write(operation.getName());
            for (Metric metric : Metric.values()) {
              String metricResult = String.format("%.3f", metric.typeValueMap.get(operation).get("NONE"));
              bw.write("," + metricResult);
            }
            bw.newLine();
            break;
          case AGG_RANGE_QUERY:
            String aggFuncName = config.QUERY_AGGREGATE_FUN;
            operationTitle = operation.getName() + ": " + aggFuncName;
            bw.write(operationTitle);
            for (Metric metric : Metric.values()) {
              String metricResult = String.format("%.3f", metric.typeValueMap.get(operation).get(aggFuncName));
              bw.write("," + metricResult);
            }
            bw.newLine();
            break;
          case RANGED_UDF_QUERY:
            for (String udfName : config.QUERY_UDF_NAME_LIST) {
              operationTitle = operation.getName() + ": " + udfName;
              bw.write(operationTitle);
              for (Metric metric : Metric.values()) {
                String metricResult = String.format("%.3f", metric.typeValueMap.get(operation).get(udfName));
                bw.write("," + metricResult);
              }
              bw.newLine();
            }
            break;
        }
      }
      bw.close();
    } catch (IOException e) {
      LOGGER.error("Exception occurred during operating buffer writer because: ", e);
    }
  }

}
