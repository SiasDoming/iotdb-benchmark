package cn.edu.tsinghua.iotdb.benchmark.conf;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ConfigDescriptor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigDescriptor.class);

  private Config config;

  private static class ConfigDescriptorHolder {

    private static final ConfigDescriptor INSTANCE = new ConfigDescriptor();
  }

  private ConfigDescriptor() {
    config = new Config();
    loadProps();
    loadListJson();
    config.initInnerFunction();
    config.initSensorCodes();
    config.initSensorFunction();
  }

  public static final ConfigDescriptor getInstance() {
    return ConfigDescriptorHolder.INSTANCE;
  }

  public Config getConfig() {
    return config;
  }

  private void loadProps() {
    String url = System.getProperty(Constants.BENCHMARK_CONF, "conf/config.properties");
    if (url != null) {
      InputStream inputStream;
      try {
        inputStream = new FileInputStream(new File(url));
      } catch (FileNotFoundException e) {
        LOGGER.warn("Fail to find config file {}", url);
        return;
      }
      Properties properties = new Properties();
      try {
        properties.load(inputStream);
        /** IoTDB通用参数 */
        config.VERSION = properties.getProperty("VERSION", "");
        config.ENABLE_THRIFT_COMPRESSION = Boolean.parseBoolean(properties
                .getProperty("ENABLE_THRIFT_COMPRESSION", config.ENABLE_THRIFT_COMPRESSION + ""));
        config.USE_CLUSTER_DB = Boolean.parseBoolean(properties.getProperty("USE_CLUSTER_DB",
                String.valueOf(config.USE_CLUSTER_DB)));
        /** 单节点IoTDB连接配置 */
        config.HOST = properties.getProperty("HOST", "no HOST");
        config.PORT = properties.getProperty("PORT", "no PORT");
        /** IoTDB集群连接配置 */
        config.CLUSTER_HOSTS = Arrays
                .asList(properties.getProperty("CLUSTER_HOSTS", "").split(","));
        /** Benchmark测试通用参数 */
        config.BENCHMARK_WORK_MODE = properties.getProperty("BENCHMARK_WORK_MODE", "");
        config.OPERATION_PROPORTION = properties.getProperty("OPERATION_PROPORTION", config.OPERATION_PROPORTION);
        config.LOOP = Long.parseLong(properties.getProperty("LOOP", config.LOOP + ""));
        config.OP_INTERVAL = Integer
                .parseInt(properties.getProperty("OP_INTERVAL", config.OP_INTERVAL + ""));
        config.IS_DELETE_DATA = Boolean
                .parseBoolean(properties.getProperty("IS_DELETE_DATA", config.IS_DELETE_DATA + ""));
        config.CLIENT_NUMBER = Integer
                .parseInt(properties.getProperty("CLIENT_NUMBER", config.CLIENT_NUMBER + ""));
        config.IS_CLIENT_BIND = Boolean
                .parseBoolean(properties.getProperty("IS_CLIENT_BIND", config.IS_CLIENT_BIND + ""));
        config.LOG_PRINT_INTERVAL = Integer
                .parseInt(properties.getProperty("LOG_PRINT_INTERVAL", config.LOG_PRINT_INTERVAL + ""));
        config.IS_QUIET_MODE = Boolean
                .parseBoolean(properties.getProperty("IS_QUIET_MODE", config.IS_QUIET_MODE + ""));
        /** 写入测试通用参数 */
        config.INSERT_MODE = properties.getProperty("INSERT_MODE", config.INSERT_MODE);
        config.GROUP_NUMBER = Integer
                .parseInt(properties.getProperty("GROUP_NUMBER", config.GROUP_NUMBER + ""));
        config.GROUP_NAME_PREFIX = properties
                .getProperty("GROUP_NAME_PREFIX", config.GROUP_NAME_PREFIX + "");
        config.SG_STRATEGY = properties.getProperty("SG_STRATEGY", "hash");
        config.CREATE_SCHEMA = Boolean
                .parseBoolean(properties.getProperty("CREATE_SCHEMA", config.CREATE_SCHEMA + ""));
        config.BATCH_SIZE = Integer
                .parseInt(properties.getProperty("BATCH_SIZE", config.BATCH_SIZE + ""));
        config.WORKLOAD_BUFFER_SIZE = Integer.parseInt(
                properties.getProperty("WORKLOAD_BUFFER_SIZE", config.WORKLOAD_BUFFER_SIZE + ""));
        /** 合成数据集数据量配置 */
        config.DEVICE_NUMBER = Integer
                .parseInt(properties.getProperty("DEVICE_NUMBER", config.DEVICE_NUMBER + ""));
        config.REAL_INSERT_RATE = Double
                .parseDouble(properties.getProperty("REAL_INSERT_RATE", config.REAL_INSERT_RATE + ""));
        if (config.REAL_INSERT_RATE <= 0 || config.REAL_INSERT_RATE > 1) {
          config.REAL_INSERT_RATE = 1;
          LOGGER.error("Invalid parameter REAL_INSERT_RATE: {}, whose value range should be (0, "
                  + "1], using default value 1.0", config.REAL_INSERT_RATE);
        }
        config.SENSOR_NUMBER = Integer
                .parseInt(properties.getProperty("SENSOR_NUMBER", config.SENSOR_NUMBER + ""));
        config.START_TIME = properties.getProperty("START_TIME", config.START_TIME);
        config.TIMESTAMP_PRECISION = properties
                .getProperty("TIMESTAMP_PRECISION", config.TIMESTAMP_PRECISION + "");
        switch (config.TIMESTAMP_PRECISION) {
          case "ms":
          case "us":
            break;
          default:
            throw new RuntimeException(
                    "not support timestamp precision: " + config.TIMESTAMP_PRECISION);
        }
        config.POINT_STEP = Long
                .parseLong(properties.getProperty("POINT_STEP", config.POINT_STEP + ""));
        /** 生成数据点时间戳顺序 */
        config.IS_OVERFLOW = Boolean
                .parseBoolean(properties.getProperty("IS_OVERFLOW", config.IS_OVERFLOW + ""));
        config.OVERFLOW_MODE = Integer
                .parseInt(properties.getProperty("OVERFLOW_MODE", config.OVERFLOW_MODE + ""));
        config.OVERFLOW_RATIO = Double
                .parseDouble(properties.getProperty("OVERFLOW_RATIO", config.OVERFLOW_RATIO + ""));
        config.LAMBDA = Double.parseDouble(properties.getProperty("LAMBDA", config.LAMBDA + ""));
        config.MAX_K = Integer.parseInt(properties.getProperty("MAX_K", config.MAX_K + ""));
        config.IS_RANDOM_TIMESTAMP_INTERVAL = Boolean.parseBoolean(properties
                .getProperty("IS_RANDOM_TIMESTAMP_INTERVAL", config.IS_RANDOM_TIMESTAMP_INTERVAL + ""));
        /** 生成数据类型 */
        config.INSERT_DATATYPE_PROPORTION = properties.getProperty("INSERT_DATATYPE_PROPORTION", config.INSERT_DATATYPE_PROPORTION);
        resolveDataTypeProportion();
        config.ENCODING_BOOLEAN = properties.getProperty("ENCODING_BOOLEAN", config.ENCODING_BOOLEAN);
        config.ENCODING_INT32 = properties.getProperty("ENCODING_INT32", config.ENCODING_INT32);
        config.ENCODING_INT64 = properties.getProperty("ENCODING_INT64", config.ENCODING_INT64);
        config.ENCODING_FLOAT = properties.getProperty("ENCODING_FLOAT", config.ENCODING_FLOAT);
        config.ENCODING_DOUBLE = properties.getProperty("ENCODING_DOUBLE", config.ENCODING_DOUBLE);
        config.ENCODING_TEXT = properties.getProperty("ENCODING_TEXT", config.ENCODING_TEXT);
        config.NUMBER_OF_DECIMAL_DIGIT = Integer.parseInt(
                properties.getProperty("NUMBER_OF_DECIMAL_DIGIT", config.NUMBER_OF_DECIMAL_DIGIT + ""));
        config.COMPRESSOR = properties.getProperty("COMPRESSOR", "UNCOMPRESSOR");
        /** 数据生成函数 */
        config.DATA_SEED = Long
                .parseLong(properties.getProperty("DATA_SEED", config.DATA_SEED + ""));
        config.LINE_RATIO = Double
                .parseDouble(properties.getProperty("LINE_RATIO", config.LINE_RATIO + ""));
        config.SIN_RATIO = Double
                .parseDouble(properties.getProperty("SIN_RATIO", config.SIN_RATIO + ""));
        config.SQUARE_RATIO = Double
                .parseDouble(properties.getProperty("SQUARE_RATIO", config.SQUARE_RATIO + ""));
        config.RANDOM_RATIO = Double
                .parseDouble(properties.getProperty("RANDOM_RATIO", config.RANDOM_RATIO + ""));
        config.CONSTANT_RATIO = Double
                .parseDouble(properties.getProperty("CONSTANT_RATIO", config.CONSTANT_RATIO + ""));
        /** 查询测试参数 */
        config.QUERY_SEED = Long
                .parseLong(properties.getProperty("QUERY_SEED", config.QUERY_SEED + ""));
        config.QUERY_DEVICE_NUM = Integer
                .parseInt(properties.getProperty("QUERY_DEVICE_NUM", config.QUERY_DEVICE_NUM + ""));
        config.QUERY_SENSOR_NUM = Integer
                .parseInt(properties.getProperty("QUERY_SENSOR_NUM", config.QUERY_SENSOR_NUM + ""));
        config.QUERY_INTERVAL = Long
                .parseLong(properties.getProperty("QUERY_INTERVAL", config.QUERY_INTERVAL + ""));
        config.STEP_SIZE = Integer
                .parseInt(properties.getProperty("STEP_SIZE", config.STEP_SIZE + ""));
        config.QUERY_AGGREGATE_FUN = properties
                .getProperty("QUERY_AGGREGATE_FUN", config.QUERY_AGGREGATE_FUN);
        config.WRITE_OPERATION_TIMEOUT_MS = Integer
                .parseInt(properties.getProperty("WRITE_OPERATION_TIMEOUT_MS", config.WRITE_OPERATION_TIMEOUT_MS + ""));
        config.READ_OPERATION_TIMEOUT_MS = Integer
                .parseInt(properties.getProperty("READ_OPERATION_TIMEOUT_MS", config.READ_OPERATION_TIMEOUT_MS + ""));
        /** CSV文件导入参数 */
        config.IMPORT_DATA_FILE_PATH = properties.getProperty("IMPORT_DATA_FILE_PATH", "");
        config.METADATA_FILE_PATH = properties.getProperty("METADATA_FILE_PATH", "");
        config.BATCH_EXECUTE_COUNT = Integer
                .parseInt(properties.getProperty("BATCH_EXECUTE_COUNT", config.BATCH_EXECUTE_COUNT + ""));
        /** Server Mode 参数 */
        config.MONITOR_FLAG_PATH = properties.getProperty("MONITOR_FLAG_PATH", "/logs");
        String dataDir = properties.getProperty("IOTDB_DATA_DIR", "/home/liurui/data/data");
        Collections.addAll(config.IOTDB_DATA_DIR, dataDir.split(","));
        String walDir = properties.getProperty("IOTDB_WAL_DIR", "/home/liurui/data/wal");
        Collections.addAll(config.IOTDB_WAL_DIR, walDir.split(","));
        String systemDir = properties.getProperty("IOTDB_SYSTEM_DIR", "/home/liurui/data/system");
        Collections.addAll(config.IOTDB_SYSTEM_DIR, systemDir.split(","));
        for (String data_ : config.IOTDB_DATA_DIR) {
          config.SEQUENCE_DIR.add(data_ + "/sequence");
          config.UNSEQUENCE_DIR.add(data_ + "/unsequence");
        }
        config.NET_DEVICE = properties.getProperty("NET_DEVICE", "e");
        config.INTERVAL = Integer
                .parseInt(properties.getProperty("INTERVAL", config.INTERVAL + ""));
        /** 测试结果持久化参数 */
        config.TEST_DATA_PERSISTENCE = properties.getProperty("TEST_DATA_PERSISTENCE", "None");
        config.TEST_DATA_STORE_IP = properties
                .getProperty("TEST_DATA_STORE_IP", config.TEST_DATA_STORE_IP);
        config.TEST_DATA_STORE_PORT = properties
                .getProperty("TEST_DATA_STORE_PORT", config.TEST_DATA_STORE_PORT);
        config.TEST_DATA_STORE_DB = properties
                .getProperty("TEST_DATA_STORE_DB", config.TEST_DATA_STORE_DB);
        config.TEST_DATA_STORE_USER = properties
                .getProperty("TEST_DATA_STORE_USER", config.TEST_DATA_STORE_USER);
        config.TEST_DATA_STORE_PW = properties
                .getProperty("TEST_DATA_STORE_PW", config.TEST_DATA_STORE_PW);
        config.REMARK = properties.getProperty("REMARK", "-");
        config.CSV_OUTPUT = Boolean
                .parseBoolean(properties.getProperty("CSV_OUTPUT", config.CSV_OUTPUT + ""));
      } catch (IOException e) {
        e.printStackTrace();
      }
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          LOGGER.error("Fail to close config file input stream", e);
        }
      }
    } else {
      LOGGER.warn("{} No config file path, use default config", Constants.CONSOLE_PREFIX);
    }
  }

  private void loadListJson() {
    String filePath = System.getProperty(Constants.BENCHMARK_UDF_LIST, "conf/TestUDFList.json");
    if (filePath != null) {
      JSONReader jsonReader;
      try {
        jsonReader = new JSONReader(new FileReader(filePath));
      } catch (FileNotFoundException e) {
        LOGGER.warn("Fail to find UDF list file {}", filePath);
        return;
      }
      try {
        jsonReader.startArray();
        while (jsonReader.hasNext()) {
          JSONObject udfObject = (JSONObject) jsonReader.readObject();
          config.QUERY_UDF_NAME_LIST.add(udfObject.get("UDFName").toString());
          config.QUERY_UDF_FULL_CLASS_NAME.add(udfObject.get("FullClassName").toString());
        }
        jsonReader.endArray();
      } catch (Exception e) {
        LOGGER.error("Fail to parse UDF list because ", e);
      }
    }
  }

  public void resolveDataTypeProportion() {
    if (!config.OPERATION_PROPORTION.split(":")[0].equals("0")) {
      List<Double> proportion = new ArrayList<>();
      String[] split = config.INSERT_DATATYPE_PROPORTION.split(":");
      if (split.length != TSDataType.values().length) {
        LOGGER.error("INSERT_DATATYPE_PROPORTION error, please check this parameter.");
      }
      double[] proportions = new double[TSDataType.values().length];
      double sum = 0;
      for (int i = 0; i < split.length; i++) {
        proportions[i] = Double.parseDouble(split[i]);
        sum += proportions[i];
      }
      for (int i = 0; i < split.length; i++) {
        if (sum != 0) {
          proportion.add(proportions[i] / sum);
        } else {
          proportion.add(0.0);
          LOGGER.error("The sum of INSERT_DATATYPE_PROPORTION is zero!");
        }
      }
      Double[] p = new Double[TSDataType.values().length + 1];
      p[0] = 0.0;
      // split [0,1] to n regions, each region corresponds to a data type whose proportion
      // is the region range size.
      for (int i = 1; i <= TSDataType.values().length; i++) {
        p[i] = p[i - 1] + proportion.get(i - 1);
      }
      config.proportion = Arrays.asList(p);
    }
  }
}
