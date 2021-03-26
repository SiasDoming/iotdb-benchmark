package cn.edu.tsinghua.iotdb.benchmark.conf;

import cn.edu.tsinghua.iotdb.benchmark.utils.TimeUtils;

/**
 * 系统运行常量值
 */
public class Constants {
    private static Config config = ConfigDescriptor.getInstance().getConfig();
    public static final long START_TIMESTAMP = TimeUtils.convertDateStrToTimestamp(config.START_TIME);
    public static final String URL = "jdbc:iotdb://%s:%s/";
    public static final String USER = "root";
    public static final String PASSWD = "root";
    public static final String ROOT_SERIES_NAME = "root";
    public static final String CONSOLE_PREFIX = "IotDB-benchmark>";
    public static final String BENCHMARK_CONF = "benchmark-conf";
    public static final String BENCHMARK_UDF_LIST = "benchmark-udf-list";

    public static final String MYSQL_DRIVENAME = "com.mysql.jdbc.Driver";

    //different running mode
    public static final String MODE_IMPORT_DATA_FROM_CSV = "importDataFromCSV";
    public static final String MODE_WRITE_WITH_REAL_DATASET = "writeWithRealDataSet";
    public static final String MODE_QUERY_WITH_REAL_DATASET = "queryWithRealDataSet";
    public static final String MODE_TEST_WITH_DEFAULT_PATH = "testWithDefaultPath";
    public static final String MODE_SERVER_MODE = "serverMODE";
    public static final String MODE_CLIENT_SYSTEM_INFO = "clientSystemInfo";

    //different insert mode
    public static final String INSERT_USE_JDBC = "jdbc";
    public static final String INSERT_USE_SESSION_TABLET= "sessionByTablet";
    public static final String INSERT_USE_SESSION_RECORD = "sessionByRecord";
    public static final String INSERT_USE_SESSION_RECORDS = "sessionByRecords";

    // support test data persistence:
    public static final String TDP_NONE = "None";
    public static final String TDP_IOTDB = "IoTDB";
    public static final String TDP_MYSQL = "MySQL";

    // device and storage group assignment
    public static final String MOD_SG_ASSIGN_MODE = "mod";
    public static final String HASH_SG_ASSIGN_MODE = "hash";
    public static final String DIV_SG_ASSIGN_MODE = "div";

}
