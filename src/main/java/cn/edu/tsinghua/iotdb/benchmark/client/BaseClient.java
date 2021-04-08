package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.workload.IWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.SingletonWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DataSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 负责人造数据的写入、查询，真实数据的查询。 根据OPERATION_PROPORTION的比例执行写入和查询, 具体的查询和写入数据由workload确定。
 */
public abstract class BaseClient extends Client implements Runnable {

  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseClient.class);

  private OperationController operationController;
  private IWorkload syntheticWorkload;
  private final SingletonWorkload singletonWorkload;
  private long insertLoopIndex;
  private DataSchema dataSchema = DataSchema.getInstance();
  private ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
  private long loopIndex;

  public BaseClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier,
      IWorkload workload) {
    super(id, countDownLatch, barrier);
    syntheticWorkload = workload;
    singletonWorkload = SingletonWorkload.getInstance();
    operationController = new OperationController();
    insertLoopIndex = 0;
  }

  void doTest() {
    String currentThread = Thread.currentThread().getName();
    //Equals device number when the rate is 1.
    double actualDeviceFloor = config.DEVICE_NUMBER * config.REAL_INSERT_RATE;

    // print current progress periodically
    service.scheduleAtFixedRate(() -> {
      String percent = String.format("%.2f", (loopIndex + 1) * 100.0D / config.LOOP);
      LOGGER.info("{} {}% syntheticWorkload is done.", currentThread, percent);
    }, 1, config.LOG_PRINT_INTERVAL, TimeUnit.SECONDS);
    long start = 0;
    for (loopIndex = 0; loopIndex < config.LOOP; loopIndex++) {
      do {
        Operation operation = operationController.getNextOperationType();
        if (config.OP_INTERVAL > 0) {
          start = System.currentTimeMillis();
        }
        switch (operation) {
          case INGESTION:
            if (config.IS_CLIENT_BIND) {
              try {
                List<DeviceSchema> schemas = dataSchema.getClientBindSchema().get(clientThreadId);
                for (DeviceSchema deviceSchema : schemas) {
                  if (deviceSchema.getDeviceId() < actualDeviceFloor) {
                    Batch batch = syntheticWorkload.getOneBatch(deviceSchema, insertLoopIndex);
                    dbWrapper.insertOneBatch(batch);
                  }
                }
              } catch (Exception e) {
                LOGGER.error("Failed to insert one batch data because ", e);
              }
              insertLoopIndex++;
            } else {
              try {
                Batch batch = singletonWorkload.getOneBatch();
                if (batch.getDeviceSchema().getDeviceId() < actualDeviceFloor) {
                  dbWrapper.insertOneBatch(batch);
                }
              } catch (Exception e) {
                LOGGER.error("Failed to insert one batch data because ", e);
              }
            }
            break;
          case RANGE_QUERY:
            try {
              dbWrapper.rangeQuery(syntheticWorkload.getRangeQuery());
            } catch (Exception e) {
              LOGGER.error("Failed to do range query because ", e);
            }
            break;
          case AGG_RANGE_QUERY:
            try {
              dbWrapper.aggRangeQuery(syntheticWorkload.getAggRangeQuery());
            } catch (WorkloadException e) {
              LOGGER.error("Failed to do aggregation range query because ", e);
            }
            break;
          case UDF_RANGE_QUERY:
            try {
              for (int i = 0; i < config.QUERY_UDF_INFO_LIST.size(); i++) {
                dbWrapper.udfRangeQuery(syntheticWorkload.getUDFRangeQuery());
              }
            } catch (WorkloadException e) {
              LOGGER.error("Fail to do ranged UDF query because ", e);
            }
            break;
          default:
            LOGGER.error("Unsupported operation type {}", operation);
        }
        if (config.OP_INTERVAL > 0) {
          long elapsed = System.currentTimeMillis() - start;
          if (elapsed < config.OP_INTERVAL) {
            try {
              Thread.sleep(config.OP_INTERVAL - elapsed);
            } catch (InterruptedException e) {
              LOGGER.error("Wait for next operation failed because ", e);
            }
          }
        }
      } while (!operationController.isNewOperationLoop());
    }
    service.shutdown();
  }

}

