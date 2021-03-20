package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.client.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.PersistenceFactory;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb.IoTDB;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb.IoTDBClusterSession;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb.IoTDBSession;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangedUDFQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DBWrapper implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBWrapper.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private IDatabase db;
  private static final double NANO_TO_SECOND = 1000000000.0d;
  private static final double NANO_TO_MILLIS = 1000000.0d;
  private Measurement measurement;
  private static final String ERROR_LOG = "Failed to do {} because unexpected exception: ";
  private ITestDataPersistence recorder;

  public DBWrapper(Measurement measurement) {
    try {
      switch (config.INSERT_MODE) {
        case Constants.INSERT_USE_JDBC:
          db = new IoTDB();
        case Constants.INSERT_USE_SESSION_RECORD:
        case Constants.INSERT_USE_SESSION_RECORDS:
        case Constants.INSERT_USE_SESSION_TABLET:
          if (config.USE_CLUSTER_DB) {
            db = new IoTDBClusterSession();
          } else {
            db = new IoTDBSession();
          }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to get database because", e);
    }
    this.measurement = measurement;
    PersistenceFactory persistenceFactory = new PersistenceFactory();
    recorder = persistenceFactory.getPersistence();
  }

  @Override
  public void init() throws TsdbException {
    db.init();
  }

  @Override
  public void cleanup() throws TsdbException {
    db.cleanup();
  }

  @Override
  public void close() throws TsdbException {
    db.close();
    if (recorder != null) {
      recorder.close();
    }
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    Status status = null;
    Operation operation = Operation.INGESTION;
    try {

      long st = System.nanoTime();
      status = db.insertOneBatch(batch);
      long en = System.nanoTime();
      status.setTimeCost(en - st);

      if (status.isOk()) {
        measureOkOperation(status, operation, batch.pointNum());
        if (!config.IS_QUIET_MODE) {
          double timeInMillis = status.getTimeCost() / NANO_TO_MILLIS;
          String formatTimeInMillis = String.format("%.2f", timeInMillis);
          double throughput = batch.pointNum() * 1000 / timeInMillis;
          LOGGER.info("{} insert one batch latency (device: {}, sg: {}) ,{}, ms, throughput ,{}, points/s",
              Thread.currentThread().getName(), batch.getDeviceSchema().getDevice(),
              batch.getDeviceSchema().getGroup(), formatTimeInMillis, throughput);
        }
      } else {
        measurement.addFailOperationNum(operation);
        measurement.addFailPointNum(operation, batch.pointNum());
        recorder.saveOperationResult(operation.getName(), 0, batch.pointNum(), 0,
            status.getException().toString());
        LOGGER.error("Insert batch failed because", status.getException());
      }
    } catch (Exception e) {
      measurement.addFailOperationNum(operation);
      measurement.addFailPointNum(operation, batch.pointNum());
      recorder.saveOperationResult(operation.getName(), 0, batch.pointNum(), 0, e.toString());
      LOGGER.error("Failed to insert one batch because unexpected exception: ", e);
    }
    return status;
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    Status status = null;
    Operation operation = Operation.RANGE_QUERY;
    try {
      long st = System.nanoTime();
      status = db.rangeQuery(rangeQuery);
      long en = System.nanoTime();
      status.setTimeCost(en - st);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e);
    }
    return status;
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    Status status = null;
    Operation operation = Operation.AGG_RANGE_QUERY;
    try {
      long st = System.nanoTime();
      status = db.aggRangeQuery(aggRangeQuery);
      long en = System.nanoTime();
      status.setTimeCost(en - st);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e);
    }
    return status;
  }

  @Override
  public Status rangedUDFQuery(RangedUDFQuery rangedUDFQuery) {
    Status status = null;
    Operation operation = Operation.RANGED_UDF_QUERY;
    try {
      long st = System.nanoTime();
      status = db.rangedUDFQuery(rangedUDFQuery);
      long en = System.nanoTime();
      status.setTimeCost(en - st);
      handleQueryOperation(status, operation);
    } catch (Exception e) {
      handleUnexpectedQueryException(operation, e);
    }
    return status;
  }

  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    double createSchemaTimeInSecond;
    long en = 0;
    long st = 0;
    LOGGER.info("Registering schema...");
    try {
      if (config.CREATE_SCHEMA) {
        st = System.nanoTime();
        db.registerSchema(schemaList);
        en = System.nanoTime();
      }
      createSchemaTimeInSecond = (en - st) / NANO_TO_SECOND;
      measurement.setCreateSchemaTime(createSchemaTimeInSecond);
    } catch (Exception e) {
      measurement.setCreateSchemaTime(0);
      throw new TsdbException(e);
    }
  }

  private void handleUnexpectedQueryException(Operation operation, Exception e) {
    measurement.addFailOperationNum(operation);
    // currently we do not have expected result point number for query
    LOGGER.error(ERROR_LOG, operation, e);
    recorder.saveOperationResult(operation.getName(), 0, 0, 0, e.toString());
  }

  private void measureOkOperation(Status status, Operation operation, int okPointNum) {
    double latencyInMillis = status.getTimeCost() / NANO_TO_MILLIS;
    if(latencyInMillis < 0) {
      LOGGER.warn("Operation {} may have exception since the latency is negative, set it to zero",
          operation.getName());
      latencyInMillis = 0;
    }
    measurement.addOperationLatency(operation, latencyInMillis);
    measurement.addOkOperationNum(operation);
    measurement.addOkPointNum(operation, okPointNum);
    recorder.saveOperationResult(operation.getName(), okPointNum, 0, latencyInMillis, "");
  }

  private void handleQueryOperation(Status status, Operation operation) {
    if (status.isOk()) {
      measureOkOperation(status, operation, status.getQueryResultPointNum());
      if(!config.IS_QUIET_MODE) {
        double timeInMillis = status.getTimeCost() / NANO_TO_MILLIS;
        String formatTimeInMillis = String.format("%.2f", timeInMillis);
        String currentThread = Thread.currentThread().getName();
        LOGGER
            .info("{} complete {} with latency ,{}, ms ,{}, result points", currentThread, operation,
                formatTimeInMillis, status.getQueryResultPointNum());
      }
    } else {
      LOGGER.error("Execution fail: {}", status.getErrorMessage(), status.getException());
      measurement.addFailOperationNum(operation);
      // currently we do not have expected result point number for query
      recorder
          .saveOperationResult(operation.getName(), 0, 0, 0, status.getException().toString());
    }
  }

}
