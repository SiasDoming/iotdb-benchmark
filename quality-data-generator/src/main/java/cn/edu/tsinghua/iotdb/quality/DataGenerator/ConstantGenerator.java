package cn.edu.tsinghua.iotdb.quality.DataGenerator;

import cn.edu.tsinghua.iotdb.quality.utils.Batch;
import cn.edu.tsinghua.iotdb.quality.utils.DeviceSchema;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

public class ConstantGenerator implements IDataGenerator{

    // generation schema
    private DeviceSchema deviceSchema;
    // given constants
    private List<Object> constants;
    // timeseries start timestamp
    private long startTimestamp;
    // data point interval
    private long interval;
    // build-in buffer for synthesized data batch
    private Batch batch;

    @Override
    public void init() {
        this.deviceSchema = new DeviceSchema();
        this.constants = new ArrayList<>();
        this.startTimestamp = 0;
        this.interval = 0;
        this.batch = new Batch();
    }

    @Override
    public Batch generateBatchData(int batchSize) {
        for (int i = 0; i < batchSize; i++) {
            this.batch.addRecord(startTimestamp, new ArrayList<>(constants));
        }
        return batch;
    }

    @Override
    public Batch getBatchData() {
        return this.batch;
    }

    @Override
    public boolean writeToCsv(String csvFilePath) {
        try {
            FileWriter fileWriter = new FileWriter(csvFilePath);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            // output storage infomation
            bufferedWriter.write(deviceSchema.getGroup() + "." + deviceSchema.getDevice());
            for (int i = 0; i < deviceSchema.getSensors().size(); i++) {
                bufferedWriter.write("," + deviceSchema.getSensors().get(i));
            }
            bufferedWriter.newLine();
            // output data type schema
            bufferedWriter.write("TIMESPTAMP");
            for (int i = 0; i < deviceSchema.getDataTypes().size(); i++) {
                bufferedWriter.write("," + deviceSchema.getDataTypes().get(i).getDataTypeName());
            }
            bufferedWriter.newLine();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean writeToIoTDB(String host, String port, String user, String passwd) {
        try {
            Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
            org.apache.iotdb.jdbc.Config.rpcThriftCompressionEnable = false;
            Connection connection = DriverManager.getConnection(String.format("jdbc:iotdb://%s:%s/", host, port), user, passwd);
            // construct and submit sql to IoTDB jdbc
            /*
            * ***********************************
             */
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public DeviceSchema getDeviceSchema() {
        return deviceSchema;
    }

    public void setDeviceSchema(DeviceSchema deviceSchema) {
        this.deviceSchema = deviceSchema;
    }

    public List<Object> getConstants() {
        return constants;
    }

    public void setConstants(List<Object> constants) {
        this.constants = constants;
    }

    public double getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public double getInterval() {
        return interval;
    }

    public void setInterval(long interval) {
        this.interval = interval;
    }
}
