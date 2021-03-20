package cn.edu.tsinghua.iotdb.benchmark.tsdb;


import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangedUDFQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import java.util.List;

public interface IDatabase {

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  void init() throws TsdbException;

  /**
   * Cleanup any state for this DB, including the old data deletion.
   * Called once before each test if IS_DELETE_DATA=true.
   */
  void cleanup() throws TsdbException;

  /**
   * Close the DB instance connections.
   * Called once per DB instance.
   */
  void close() throws TsdbException;

  /**
   * Called once before each test if CREATE_SCHEMA=true.
   * @param schemaList schema of devices to register
   */
  void registerSchema(List<DeviceSchema> schemaList) throws TsdbException;

  /**
   * Insert one batch into the database, the DB implementation needs to resolve the data in batch
   * which contains device schema and Map[Long, List[String]] records. The key of records is a
   * timestamp and the value is a list of sensor value data.
   * @param batch universal insertion data structure
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status insertOneBatch(Batch batch);

  /**
   * Query data of one or multiple sensors in a time range.
   * e.g. select v1... from data where time >= ? and time <= ? and device in ?
   * @param rangeQuery universal range query condition parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status rangeQuery(RangeQuery rangeQuery);

  /**
   * Query aggregated data of one or multiple sensors in a time range using aggregation function.
   * e.g. select func(v1)... from data where device in ? and time >= ? and time <= ?
   * @param aggRangeQuery contains universal aggregation query with time filter parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status aggRangeQuery(AggRangeQuery aggRangeQuery);

  /**
   * UDF query of one or multiple sensors within a time range.
   * e.g. select udf(v1)... from data where device in ? and time >= ? and time <= ?
   * @param rangedUDFQuery contains universal UDF query with time filters parameters
   * @return status which contains successfully executed flag, error message and so on.
   */
  Status rangedUDFQuery(RangedUDFQuery rangedUDFQuery);
}
