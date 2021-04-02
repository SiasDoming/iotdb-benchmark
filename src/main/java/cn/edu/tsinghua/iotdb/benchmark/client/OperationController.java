package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;

public class OperationController {

  private static final Logger LOGGER = LoggerFactory.getLogger(OperationController.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private int operationIndex = 0;

  OperationController() {

  }

  /**
   * @return Operation the next operation for client to execute
   */
  Operation getNextOperationType() {
    List<Integer> proportion = resolveOperationProportion();
    int i;
    for (i = 0; i < Operation.values().length; i++) {
      if (operationIndex < proportion.get(i)) {
        break;
      }
    }
    operationIndex++;
    if (operationIndex == proportion.get(Operation.values().length - 1)) {
      operationIndex = 0;
    }
    switch (i) {
      case 0:
        return Operation.INGESTION;
      case 1:
        return Operation.RANGE_QUERY;
      case 2:
        return Operation.AGG_RANGE_QUERY;
      case 3:
        return Operation.UDF_RANGE_QUERY;
      default:
        LOGGER.error("Unsupported operation {}, use default operation: INGESTION.", i);
        return Operation.INGESTION;
    }
  }

  List<Integer> resolveOperationProportion() {

    List<Integer> proportion = new ArrayList<>(Operation.values().length);
    String[] split = config.OPERATION_PROPORTION.split(":");
    if (split.length != Operation.values().length) {
      LOGGER.error("OPERATION_PROPORTION error, please check this parameter.");
    }
    proportion.add(Integer.parseInt(split[0]));
    for (int i = 1; i < split.length; i++) {
      proportion.add(proportion.get(i-1) + Integer.parseInt(split[i]));
    }
    if (proportion.get(Operation.values().length - 1) == 0) {
      LOGGER.error("The sum of operation proportions is zero!");
    }
    return proportion;
  }

  boolean isNewOperationLoop() {
    return operationIndex==0;
  }

}
