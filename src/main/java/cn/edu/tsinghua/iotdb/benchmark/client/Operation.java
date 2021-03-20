package cn.edu.tsinghua.iotdb.benchmark.client;

public enum Operation {
  INGESTION("INGESTION"),
  RANGE_QUERY("TIME_RANGE"),
  AGG_RANGE_QUERY("AGG_RANGE"),
  RANGED_UDF_QUERY("RANGED_UDF");

  public String getName() {
    return name;
  }

  String name;

  Operation(String name) {
    this.name = name;
  }
}
