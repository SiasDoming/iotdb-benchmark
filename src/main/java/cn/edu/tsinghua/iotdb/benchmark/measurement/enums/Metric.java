package cn.edu.tsinghua.iotdb.benchmark.measurement.enums;

import cn.edu.tsinghua.iotdb.benchmark.client.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

public enum Metric {
  AVG_LATENCY("AVG"),
  MIN_LATENCY("MIN"),
  P10_LATENCY("P10"),
  P25_LATENCY("P25"),
  MEDIAN_LATENCY("MEDIAN"),
  P75_LATENCY("P75"),
  P90_LATENCY("P90"),
  P95_LATENCY("P95"),
  P99_LATENCY("P99"),
  P999_LATENCY("P999"),
  MAX_LATENCY("MAX"),
  MAX_THREAD_LATENCY_SUM("SLOWEST_THREAD");

  public Map<Operation, Map<String, Double>> getTypeValueMap() {
    return typeValueMap;
  }

  public Map<Operation, Map<String, Double>> typeValueMap;

  public String getName() {
    return name;
  }

  public String name;

  Metric(String name) {
    this.name = name;
    typeValueMap = new EnumMap<>(Operation.class);
    Config config = ConfigDescriptor.getInstance().getConfig();
    for (Operation operation : Operation.values()) {
      typeValueMap.put(operation, new HashMap<>());
      switch (operation) {
        case INGESTION:
        case RANGE_QUERY:
          typeValueMap.get(operation).put("NONE", 0D);
          break;
        case AGG_RANGE_QUERY:
          typeValueMap.get(operation).put(config.QUERY_AGGREGATE_FUN, 0D);
          break;
        case RANGED_UDF_QUERY:
          for (String funcName : config.QUERY_UDF_NAME_LIST) {
            typeValueMap.get(operation).put(funcName, 0D);
          }
          break;
      }
    }
  }

}
