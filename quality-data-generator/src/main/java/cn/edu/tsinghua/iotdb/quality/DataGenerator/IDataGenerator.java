package cn.edu.tsinghua.iotdb.quality.DataGenerator;

import cn.edu.tsinghua.iotdb.quality.utils.Batch;

public interface IDataGenerator {

    /*
     * Initialize generator, including but not limited to: random module initialization and default arguments.
     * Called multiple times each instance is constructed or reset.
     */
    void init();

    /*
     * Generate one batch sample data of given size with pre-set arguments.
     */
    Batch generateBatchData(int batchSize);

    /*
     * Get last generated batch
     */
    Batch getBatchData();

    /*
     * Write batch to given csv file.
     */
    boolean writeToCsv(String csvFilePath);

    /*
     * Write batch to IoTDB at given host and port.
     */
    boolean writeToIoTDB(String host, String port, String user, String passwd);

}
