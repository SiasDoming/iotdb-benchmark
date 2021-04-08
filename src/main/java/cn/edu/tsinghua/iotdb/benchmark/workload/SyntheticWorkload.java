package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.client.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.*;
import cn.edu.tsinghua.iotdb.benchmark.distribution.PoissonDistribution;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.UDFRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DataSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SyntheticWorkload implements IWorkload {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyntheticWorkload.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static Random timestampRandom = new Random(config.DATA_SEED);
  private ProbTool probTool;
  private Map<DeviceSchema, Long> maxTimestampIndexMap;
  private Random poissonRandom;
  private Random queryDeviceRandom;
  private Map<Operation, Long> operationLoops;
  private static Random random = new Random(config.DATA_SEED);
  private static final String DECIMAL_FORMAT = "%." + config.NUMBER_OF_DECIMAL_DIGIT + "f";
  private static Random dataRandom = new Random(config.DATA_SEED);
  private static int scaleFactor = 1;
  private static Object[][] workloadValues = initWorkloadValues();
  private static final String CHAR_TABLE =
      "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static final long timeStampConst = getTimestampConst(config.TIMESTAMP_PRECISION);

  public SyntheticWorkload(int clientId) {
    probTool = new ProbTool();
    maxTimestampIndexMap = new HashMap<>();
    poissonRandom = new Random(config.DATA_SEED);
    for (DeviceSchema schema : DataSchema.getInstance().getClientBindSchema().get(clientId)) {
      maxTimestampIndexMap.put(schema, 0L);
    }
    queryDeviceRandom = new Random(config.QUERY_SEED + clientId);
    operationLoops = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      operationLoops.put(operation, 0L);
    }
  }

  private static void initScaleFactor() {
    for (int i = 0; i < ConfigDescriptor.getInstance().getConfig().NUMBER_OF_DECIMAL_DIGIT; i++) {
      scaleFactor *= 10;
    }
  }

  private static Object[][] initWorkloadValues() {
    initScaleFactor();
    Object[][] workloadValues = null;
    if (!config.OPERATION_PROPORTION.split(":")[0].equals("0")) {
      workloadValues = new Object[config.SENSOR_NUMBER][config.WORKLOAD_BUFFER_SIZE];
      int sensorIndex = 0;
      for (int j = 0; j < config.SENSOR_NUMBER; j++) {
        String sensor = config.SENSOR_CODES.get(j);
        for (int i = 0; i < config.WORKLOAD_BUFFER_SIZE; i++) {
          long currentTimestamp = getCurrentTimestamp(i);
          Object value;
          if (getNextDataType(sensorIndex).equals("TEXT")) {
            //TEXT case: pick NUMBER_OF_DECIMAL_DIGIT chars to be a String for insertion.
            StringBuilder builder = new StringBuilder();
            for (int k = 0; k < config.NUMBER_OF_DECIMAL_DIGIT; k++) {
              assert dataRandom != null;
              builder.append(CHAR_TABLE.charAt(dataRandom.nextInt(CHAR_TABLE.length())));
            }
            value = builder.toString();
          } else {
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            Number number = Function.getValueByFuntionidAndParam(param, currentTimestamp);
            switch (getNextDataType(sensorIndex)) {
              case "BOOLEAN":
                value = number.floatValue() > 500;
                break;
              case "INT32":
                value = number.intValue();
                break;
              case "INT64":
                value = number.longValue();
                break;
              case "FLOAT":
                value = ((float) (Math.round(number.floatValue() * scaleFactor))) / scaleFactor;
                break;
              case "DOUBLE":
                value = ((double) Math.round(number.doubleValue() * scaleFactor)) / scaleFactor;
                break;
              default:
                value = null;
                break;
            }
          }
          workloadValues[j][i] = value;
        }
        sensorIndex++;
      }
    }
    return workloadValues;
  }

  public static String getNextDataType(int sensorIndex) {
    List<Double> proportion = config.proportion;
    double sensorPosition = sensorIndex * 1.0 / config.SENSOR_NUMBER;
    int i;
    for (i = 1; i <= TSDataType.values().length; i++) {
      if (sensorPosition >= proportion.get(i - 1) && sensorPosition < proportion.get(i)) {
        break;
      }
    }
    switch (i) {
      case 1:
        return "BOOLEAN";
      case 2:
        return "INT32";
      case 3:
        return "INT64";
      case 4:
        return "FLOAT";
      case 5:
        return "DOUBLE";
      case 6:
        return "TEXT";
      default:
        LOGGER.error("Unsupported data type {}, use default data type: TEXT.", i);
        return "TEXT";
    }
  }

  private static long getCurrentTimestamp(long stepOffset) {
    long timeStampOffset = config.POINT_STEP * stepOffset;
    if (config.IS_OVERFLOW) {
      timeStampOffset += (long) (random.nextDouble() * config.POINT_STEP);
    } else {
      if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
        timeStampOffset += (long) (config.POINT_STEP * timestampRandom.nextDouble());
      }
    }
    return Constants.START_TIMESTAMP * timeStampConst + timeStampOffset;
  }

  private Batch getOrderedBatch(DeviceSchema deviceSchema, long loopIndex) {
    Batch batch = new Batch();
    for (long batchOffset = 0; batchOffset < config.BATCH_SIZE; batchOffset++) {
      long stepOffset = loopIndex * config.BATCH_SIZE + batchOffset;
      addOneRowIntoBatch(batch, stepOffset);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  private Batch getLocalOutOfOrderBatch(DeviceSchema deviceSchema, long loopIndex) {
    Batch batch = new Batch();
    long barrier = (long) (config.BATCH_SIZE * config.OVERFLOW_RATIO);
    long stepOffset = loopIndex * config.BATCH_SIZE + barrier;
    addOneRowIntoBatch(batch, stepOffset);
    for (long batchOffset = 0; batchOffset < barrier; batchOffset++) {
      stepOffset = loopIndex * config.BATCH_SIZE + batchOffset;
      addOneRowIntoBatch(batch, stepOffset);
    }
    for (long batchOffset = barrier + 1; batchOffset < config.BATCH_SIZE; batchOffset++) {
      stepOffset = loopIndex * config.BATCH_SIZE + batchOffset;
      addOneRowIntoBatch(batch, stepOffset);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  private Batch getDistOutOfOrderBatch(DeviceSchema deviceSchema) {
    Batch batch = new Batch();
    PoissonDistribution poissonDistribution = new PoissonDistribution(poissonRandom);
    int nextDelta;
    long stepOffset;
    for (long batchOffset = 0; batchOffset < config.BATCH_SIZE; batchOffset++) {
      if (probTool.returnTrueByProb(config.OVERFLOW_RATIO, poissonRandom)) {
        // generate overflow timestamp
        nextDelta = poissonDistribution.getNextPossionDelta();
        stepOffset = maxTimestampIndexMap.get(deviceSchema) - nextDelta;
      } else {
        // generate normal increasing timestamp
        maxTimestampIndexMap.put(deviceSchema, maxTimestampIndexMap.get(deviceSchema) + 1);
        stepOffset = maxTimestampIndexMap.get(deviceSchema);
      }
      addOneRowIntoBatch(batch, stepOffset);
    }
    batch.setDeviceSchema(deviceSchema);
    return batch;
  }

  static void addOneRowIntoBatch(Batch batch, long stepOffset) {
    List<Object> values = new ArrayList<>();
    long currentTimestamp = getCurrentTimestamp(stepOffset);
    for (int i = 0; i < config.SENSOR_NUMBER; i++) {
      values.add(workloadValues[i][(int) (Math.abs(stepOffset) % config.WORKLOAD_BUFFER_SIZE)]);
    }
    batch.add(currentTimestamp, values);
  }

  public Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex) throws WorkloadException {
    if (!config.IS_OVERFLOW) {
      return getOrderedBatch(deviceSchema, loopIndex);
    } else {
      switch (config.OVERFLOW_MODE) {
        case 0:
          return getDistOutOfOrderBatch(deviceSchema);
        case 1:
          return getLocalOutOfOrderBatch(deviceSchema, loopIndex);
        default:
          throw new WorkloadException("Unsupported overflow mode: " + config.OVERFLOW_MODE);
      }
    }
  }

  private List<DeviceSchema> getQueryDeviceSchemaList() throws WorkloadException {
    checkQuerySchemaParams();
    List<DeviceSchema> queryDevices = new ArrayList<>();
    List<Integer> clientDevicesIndex = new ArrayList<>();
    for (int m = 0; m < config.DEVICE_NUMBER * config.REAL_INSERT_RATE; m++) {
      clientDevicesIndex.add(m);
    }
    Collections.shuffle(clientDevicesIndex, queryDeviceRandom);
    for (int m = 0; m < config.QUERY_DEVICE_NUM; m++) {
      DeviceSchema deviceSchema = new DeviceSchema(clientDevicesIndex.get(m));
      List<String> sensors = deviceSchema.getSensors();
      Collections.shuffle(sensors, queryDeviceRandom);
      List<String> querySensors = new ArrayList<>();
      for (int i = 0; i < config.QUERY_SENSOR_NUM; i++) {
        querySensors.add(sensors.get(i));
      }
      deviceSchema.setSensors(querySensors);
      queryDevices.add(deviceSchema);
    }
    return queryDevices;
  }

  private void checkQuerySchemaParams() throws WorkloadException {
    if (!(config.QUERY_DEVICE_NUM > 0 && config.QUERY_DEVICE_NUM <= config.DEVICE_NUMBER)) {
      throw new WorkloadException("QUERY_DEVICE_NUM is not correct, please check.");
    }
    if (!(config.QUERY_SENSOR_NUM > 0 && config.QUERY_SENSOR_NUM <= config.SENSOR_NUMBER)) {
      throw new WorkloadException("QUERY_SENSOR_NUM is not correct, please check.");
    }
  }

  private long getQueryStartTimestamp(Operation operation) {
    long currentQueryLoop = operationLoops.get(operation);
    long timestampOffset = currentQueryLoop * config.STEP_SIZE * config.POINT_STEP;
    operationLoops.put(operation, currentQueryLoop + 1);
    return Constants.START_TIMESTAMP * timeStampConst + timestampOffset;
  }

  public RangeQuery getRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    long startTimestamp = getQueryStartTimestamp(Operation.RANGE_QUERY);
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new RangeQuery(queryDevices, startTimestamp, endTimestamp);
  }

  public AggRangeQuery getAggRangeQuery() throws WorkloadException {
    List<DeviceSchema> queryDevices = getQueryDeviceSchemaList();
    long startTimestamp = getQueryStartTimestamp(Operation.AGG_RANGE_QUERY);
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new AggRangeQuery(queryDevices, startTimestamp, endTimestamp,
        config.QUERY_AGGREGATE_FUN);
  }

  private List<DeviceSchema> getUDFQueryDeviceSchemaList(UDFInformation udfInformation) throws WorkloadException {
    checkUDFQuerySchemaParams(udfInformation);
    List<DeviceSchema> queryDevices = new ArrayList<>();
    // shuffle device to randomly get config.QUERY_DEVICE_NUM devices
    List<Integer> clientDevicesIndex = new ArrayList<>();
    for (int m = 0; m < config.DEVICE_NUMBER * config.REAL_INSERT_RATE; m++) {
      clientDevicesIndex.add(m);
    }
    Collections.shuffle(clientDevicesIndex, queryDeviceRandom);
    for (int m = 0; m < config.QUERY_DEVICE_NUM; m++) {
      DeviceSchema deviceSchema = new DeviceSchema(clientDevicesIndex.get(m));
      List<String> sensors = deviceSchema.getSensors();
      // build inverse index of data type to sensors
      Map<TSDataType, List<String>> dataTypeSensorMap = new EnumMap<>(TSDataType.class);
      for (TSDataType dataType: TSDataType.values()) {
        dataTypeSensorMap.put(dataType, new ArrayList<>());
      }
      for (int i = 0; i < sensors.size(); i++) {
        switch (getNextDataType(i)) {
          case "BOOLEAN":
            dataTypeSensorMap.get(TSDataType.BOOLEAN).add(sensors.get(i));
            break;
          case "INT32":
            dataTypeSensorMap.get(TSDataType.INT32).add(sensors.get(i));
            break;
          case "INT64":
            dataTypeSensorMap.get(TSDataType.INT64).add(sensors.get(i));
            break;
          case "FLOAT":
            dataTypeSensorMap.get(TSDataType.FLOAT).add(sensors.get(i));
            break;
          case "DOUBLE":
            dataTypeSensorMap.get(TSDataType.DOUBLE).add(sensors.get(i));
            break;
        }
      }
      // build input time series pairs
      List<String> querySensors = new ArrayList<>();
      for (int i = 0; i < config.QUERY_SENSOR_NUM; i++) {
        // randomly choose acceptable data type and sensor
        for (List<TSDataType> dataTypeList : udfInformation.getTimeSeries()) {
          int dataTypeIndex = queryDeviceRandom.nextInt(dataTypeList.size());
          while (dataTypeSensorMap.get(dataTypeList.get(dataTypeIndex)).size() == 0) {
            dataTypeIndex ++;
            if (dataTypeIndex == dataTypeList.size()) {
              dataTypeIndex = 0;
            }
          }
          List<String> sensorList = dataTypeSensorMap.get(dataTypeList.get(dataTypeIndex));
          querySensors.add(sensorList.get(queryDeviceRandom.nextInt(sensorList.size())));
        }
        deviceSchema.setSensors(querySensors);
      }
      queryDevices.add(deviceSchema);
    }
    return queryDevices;
  }

  private void checkUDFQuerySchemaParams(UDFInformation udfInformation) throws WorkloadException {
    checkQuerySchemaParams();
    // check if time series of required data type are generated
    Map<TSDataType, Boolean> dataTypeSensors = new EnumMap<>(TSDataType.class);
    double lastTypeNumber = 0.0, thisTypeNumber;
    for (int i = 1; i < config.proportion.size(); i++) {
      thisTypeNumber = config.SENSOR_NUMBER * config.proportion.get(i);
      dataTypeSensors.put(TSDataType.deserialize((byte) (i - 1)), (thisTypeNumber-lastTypeNumber >= 1.0));
      lastTypeNumber = thisTypeNumber;
    }
    for (List<TSDataType> timeSeries : udfInformation.getTimeSeries()) {
      boolean isAvailable = false;
      for (TSDataType dataType : timeSeries) {
        if (dataTypeSensors.get(dataType)) {
          isAvailable = true;
          break;
        }
      }
      if (!isAvailable) {
        LOGGER.error("No synthetic time series available for UDF {}, please check INSERT_DATATYPE_PROPORTION.", udfInformation.getUdfName());
        throw new WorkloadException("No synthetic time series available for UDF " + udfInformation.getUdfName()
                + ". Please check input data type and INSERT_DATATYPE_PROPORTION.");
      }
    }
  }

  @Override
  public UDFRangeQuery getUDFRangeQuery() throws WorkloadException{
    UDFInformation udfInformation = config.QUERY_UDF_INFO_LIST.get(config.getAndIncrementQueryUDFLoop());
    List<DeviceSchema> queryDevices = getUDFQueryDeviceSchemaList(udfInformation);
    long startTimestamp = getQueryStartTimestamp(Operation.UDF_RANGE_QUERY);
    long endTimestamp = startTimestamp + config.QUERY_INTERVAL;
    return new UDFRangeQuery(queryDevices, startTimestamp, endTimestamp, udfInformation.getUdfName(), udfInformation.getFullClassName(),
            udfInformation.getTimeSeries().size(), udfInformation.getArguments());
  }

  private static long getTimestampConst(String timePrecision) {
    if (timePrecision.equals("ms")) {
      return 1L;
    } else if (timePrecision.equals("us")) {
      return 1000L;
    } else {
      return 1000000L;
    }
  }
}
