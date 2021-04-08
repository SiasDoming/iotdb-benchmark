package cn.edu.tsinghua.iotdb.benchmark.conf;

import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionXml;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Config {

	/** IoTDB通用参数 */
	// IoTDB数据库版本
	public String VERSION = "";
	// 是否启用thrift的压缩机制
	public boolean ENABLE_THRIFT_COMPRESSION = false;
	// 是否使用IoTDB集群模式
	public boolean USE_CLUSTER_DB = false;

	/** 单节点IoTDB连接配置 */
	// IoTDB单节点服务器HOST和PORT
	public String HOST ="127.0.0.1";
	public String PORT ="6667";

	/** IoTDB集群连接配置 */
	// IoTDB集群服务器HOST和PORT
	public List<String> CLUSTER_HOSTS = Arrays.asList("127.0.0.1:55560","127.0.0.1:55561","127.0.0.1:55562");

	/** Benchmark测试通用参数 */
	// benchmark运行模式
	public String BENCHMARK_WORK_MODE="";
	// 各操作执行次数
	public String OPERATION_PROPORTION = "1:1:1:1";
	// 以上操作循环执行的重复次数
	public long LOOP = 10000;
	// 操作执行间隔
	public int OP_INTERVAL = 0;
	// 启动测试前是否删除旧数据
	public boolean IS_DELETE_DATA = false;
	// 测试客户端线程数量
	public int CLIENT_NUMBER = 2;
	// 设备和客户端是否绑定
	public boolean IS_CLIENT_BIND = true;
	// 输出进度日志的时间间隔
	public int LOG_PRINT_INTERVAL = 5;
	// 是否静默运行
	public boolean IS_QUIET_MODE = true;

	/** 写入测试通用参数 */
	// IoTDB数据写入方式
	public String INSERT_MODE = "jdbc";
	// 存储组数
	public int GROUP_NUMBER = 1;
	// 存储组名称前缀
	public String GROUP_NAME_PREFIX = "group_";
	// 存储组分配策略
	public String SG_STRATEGY="hash";
	// 是否写入前先创建schema
	public boolean CREATE_SCHEMA = true;
	// 批写入数据行数
	public int BATCH_SIZE = 1000;
	// workload预生成数值的缓存大小
	public int WORKLOAD_BUFFER_SIZE = 100;

	/** 合成数据集数据量配置 */
	// 总设备数量
	public int DEVICE_NUMBER = 2;
	// 实际写入设备数占的比例
	public double REAL_INSERT_RATE = 1.0;
	// 每个设备的传感器数量
	public int SENSOR_NUMBER = 5;
	// 传感器编号
	public List<String> SENSOR_CODES = new ArrayList<String>();
	// 合成数据起始时间
	public String START_TIME = "2018-8-30T00:00:00+08:00";
	// 时间戳精度
	public String TIMESTAMP_PRECISION = "ms";
	// 时间戳间隔
	public long POINT_STEP = 7000;

	/** 生成数据点时间戳顺序 */
	// 是否为批插入乱序模式
	public boolean IS_OVERFLOW = false;
	// 乱序模式选择
	public int OVERFLOW_MODE = 0;
	// 批插入乱序比例
	public double OVERFLOW_RATIO = 1.0;
	// 泊松分布参数
	public double LAMBDA = 3;
	public int MAX_K = 10;
	// 是否在顺序数据中使用随机时间间隔
	public boolean IS_RANDOM_TIMESTAMP_INTERVAL = false;

	/** 生成数据类型 */
	// 插入时各数据类型的比例
	public String INSERT_DATATYPE_PROPORTION = "1:1:1:1:1:1";
	public List<Double> proportion;
	// 各数据类型的编码方式
	public String ENCODING_BOOLEAN = "PLAIN";
	public String ENCODING_INT32 = "PLAIN";
	public String ENCODING_INT64 = "PLAIN";
	public String ENCODING_FLOAT = "PLAIN";
	public String ENCODING_DOUBLE = "PLAIN";
	public String ENCODING_TEXT = "PLAIN";
	// 生成数据的小数保留位数
	public int NUMBER_OF_DECIMAL_DIGIT = 2;
	// 数据压缩方式
	public String COMPRESSOR = "UNCOMPRESSED";

	/** 数据生成函数 */
	// 随机数生成器种子
	public long DATA_SEED = 666L;
	// 线性 默认 9个 0.054
	public double LINE_RATIO = 0.054;
	// 傅里叶函数 6个 0.036
	public double SIN_RATIO = 0.036;
	// 方波 9个 0.054
	public double SQUARE_RATIO = 0.054;
	// 随机数 默认 86个 0.512
	public double RANDOM_RATIO = 0.512;
	// 常数 默认 58个 0.352
	public double CONSTANT_RATIO = 0.352;
	// 内置函数参数列表
	private List<FunctionParam> LINE_LIST = new ArrayList<FunctionParam>();
	private List<FunctionParam> SIN_LIST = new ArrayList<FunctionParam>();
	private List<FunctionParam> SQUARE_LIST = new ArrayList<FunctionParam>();
	private List<FunctionParam> RANDOM_LIST = new ArrayList<FunctionParam>();
	private List<FunctionParam> CONSTANT_LIST = new ArrayList<FunctionParam>();
	// 传感器与其数据的生成函数之间的对应Map
	public Map<String, FunctionParam> SENSOR_FUNCTION = new HashMap<String, FunctionParam>();

	/** 查询测试参数 */
	// 查询随机种子
	public long QUERY_SEED = 1516580959202L;
	// 每条查询语句中查询涉及的设备数量
	public int QUERY_DEVICE_NUM = 1;
	// 每条查询语句中查询涉及的传感器数量
	public int QUERY_SENSOR_NUM = 1;
	// 带起止时间的查询中开始时间与结束时间之间的时间间隔
	public long QUERY_INTERVAL = DEVICE_NUMBER * POINT_STEP;
	// 相邻两次查询时间过滤条件的起点变化步长
	public int STEP_SIZE = 1;
	// 聚合查询使用的聚合函数名
	public String QUERY_AGGREGATE_FUN = "";
	// ************************************************* //
	// 更新UDF参数，支持列表，写初始化函数从文件解析
	// 查询测试的UDF函数列表
	public List<String> QUERY_UDF_NAME_LIST = new ArrayList<>();
	public List<String> QUERY_UDF_FULL_CLASS_NAME = new ArrayList<>();
	// UDF测试循环计数
	public int QUERY_UDF_LOOP = 0;
	// 整个写操作的超时时间
	public int WRITE_OPERATION_TIMEOUT_MS = 120000;
	// 整个读操作的超时时间
	public int READ_OPERATION_TIMEOUT_MS = 300000;

	/** CSV文件导入参数 */
	// CSV文件路径
	public String IMPORT_DATA_FILE_PATH = "";
	// 数据集元数据文件路径
	public String METADATA_FILE_PATH = "";
	// 批写入数据行数
	public int BATCH_EXECUTE_COUNT = 5000;

	/** Server Mode 参数 */
	// 监测状态文件目录
	public String MONITOR_FLAG_PATH;
	// IoTDB数据盘所在目录
	public List<String> IOTDB_DATA_DIR = new ArrayList<>();
	public List<String> IOTDB_WAL_DIR = new ArrayList<>();
	public List<String> IOTDB_SYSTEM_DIR = new ArrayList<>();
	public List<String> SEQUENCE_DIR = new ArrayList<>();
	public List<String> UNSEQUENCE_DIR = new ArrayList<>();
	// 系统性能检测网卡设备名
	public String NET_DEVICE = "e";
	// 系统性能检测时间间隔-2s
	public int INTERVAL = 0;

	/** 测试结果持久化参数 */
	// 选择结果持久化的写入数据库
	public String TEST_DATA_PERSISTENCE = "None";
	// 测试结果持久化数据库连接参数
	public String TEST_DATA_STORE_IP = "";
	public String TEST_DATA_STORE_PORT = "";
	public String TEST_DATA_STORE_DB = "";
	public String TEST_DATA_STORE_USER = "";
	public String TEST_DATA_STORE_PW = "";
	// 本次实验的备注信息
	public String REMARK = "";
	// 是否将结果输出至CSV文件
	public boolean CSV_OUTPUT = true;

	/**
	 * 初始化内置数据生成函数
	 */
	public void initInnerFunction() {
		FunctionXml xml = null;
		try {
			InputStream input = Function.class.getResourceAsStream("function.xml");
			JAXBContext context = JAXBContext.newInstance(FunctionXml.class, FunctionParam.class);
			Unmarshaller unmarshaller = context.createUnmarshaller();
			xml = (FunctionXml) unmarshaller.unmarshal(input);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		List<FunctionParam> xmlFuctions = xml.getFunctions();
		for (FunctionParam param : xmlFuctions) {
			if (param.getFunctionType().indexOf("_mono_k") != -1) {
				LINE_LIST.add(param);
			} else if (param.getFunctionType().indexOf("_mono") != -1) {
				// 如果min==max则为常数，系统没有非常数的
				if (param.getMin() == param.getMax()) {
					CONSTANT_LIST.add(param);
				}
			} else if (param.getFunctionType().indexOf("_sin") != -1) {
				SIN_LIST.add(param);
			} else if (param.getFunctionType().indexOf("_square") != -1) {
				SQUARE_LIST.add(param);
			} else if (param.getFunctionType().indexOf("_random") != -1) {
				RANDOM_LIST.add(param);
			}
		}
	}

	/**
	 * 初始化传感器函数 Constants.SENSOR_FUNCTION
	 */
	public void initSensorFunction() {
		// 根据传进来的各个函数比例进行配置
		double sumRatio = CONSTANT_RATIO + LINE_RATIO + RANDOM_RATIO + SIN_RATIO + SQUARE_RATIO;
		if (sumRatio != 0 && CONSTANT_RATIO >= 0 && LINE_RATIO >= 0 && RANDOM_RATIO >= 0 && SIN_RATIO >= 0
				&& SQUARE_RATIO >= 0) {
			double constantArea = CONSTANT_RATIO / sumRatio;
			double lineArea = constantArea + LINE_RATIO / sumRatio;
			double randomArea = lineArea + RANDOM_RATIO / sumRatio;
			double sinArea = randomArea + SIN_RATIO / sumRatio;
			double squareArea = sinArea + SQUARE_RATIO / sumRatio;
			Random r = new Random(DATA_SEED);
			for (int i = 0; i < SENSOR_NUMBER; i++) {
				double property = r.nextDouble();
				FunctionParam param = null;
				Random fr = new Random(DATA_SEED + 1 + i);
				double middle = fr.nextDouble();
				if (property >= 0 && property < constantArea) {// constant
					int index = (int) (middle * CONSTANT_LIST.size());
					param = CONSTANT_LIST.get(index);
				}
				if (property >= constantArea && property < lineArea) {// line
					int index = (int) (middle * LINE_LIST.size());
					param = LINE_LIST.get(index);
				}
				if (property >= lineArea && property < randomArea) {// random
					int index = (int) (middle * RANDOM_LIST.size());
					param = RANDOM_LIST.get(index);
				}
				if (property >= randomArea && property < sinArea) {// sin
					int index = (int) (middle * SIN_LIST.size());
					param = SIN_LIST.get(index);
				}
				if (property >= sinArea && property < squareArea) {// square
					int index = (int) (middle * SQUARE_LIST.size());
					param = SQUARE_LIST.get(index);
				}
				if (param == null) {
					System.err.println(" initSensorFunction() 初始化函数比例有问题！");
					System.exit(0);
				}
				SENSOR_FUNCTION.put(SENSOR_CODES.get(i), param);
			}
		} else {
			System.err.println("function ration must >=0 and sum>0");
			System.exit(0);
		}
	}

	/**
	 * 根据传感器数，初始化传感器编号
	 */
	void initSensorCodes() {
		for (int i = 0; i < SENSOR_NUMBER; i++) {
			String sensorCode = "s_" + i;
			SENSOR_CODES.add(sensorCode);
		}
	}

	public int getAndIncrementQueryUDFLoop() {
		QUERY_UDF_LOOP ++;
		if (QUERY_UDF_LOOP == QUERY_UDF_NAME_LIST.size()) {
			QUERY_UDF_LOOP = 0;
			return QUERY_UDF_NAME_LIST.size()-1;
		} else {
			return QUERY_UDF_LOOP-1;
		}
	}

}
