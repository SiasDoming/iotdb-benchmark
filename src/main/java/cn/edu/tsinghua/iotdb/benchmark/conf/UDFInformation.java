package cn.edu.tsinghua.iotdb.benchmark.conf;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class UDFInformation {

    private static final Logger LOGGER = LoggerFactory.getLogger(UDFInformation.class);

    private final String udfName;
    private final String fullClassName;
    private final List<List<TSDataType>> timeSeries;
    private final Map<String, String> arguments;

    public UDFInformation(String udfName, String fullClassName) {
        this.udfName = udfName;
        this.fullClassName = fullClassName;
        this.timeSeries = new ArrayList<>();
        this.arguments = new HashMap<>();
    }

    public void addArgument(String key, String value) {
        this.arguments.put(key, value);
    }

    public void addTimeSeries(String dataTypes) {
        EnumSet<TSDataType> singleTimeSeries = EnumSet.noneOf(TSDataType.class);
        for (String dataType : dataTypes.split("\\|")) {
            switch (dataType.toUpperCase()) {
                case "BOOLEAN":
                    singleTimeSeries.add(TSDataType.BOOLEAN);
                    break;
                case "INT32":
                    singleTimeSeries.add(TSDataType.INT32);
                    break;
                case "INT64":
                    singleTimeSeries.add(TSDataType.INT64);
                    break;
                case "FLOAT":
                    singleTimeSeries.add(TSDataType.FLOAT);
                    break;
                case "DOUBLE":
                    singleTimeSeries.add(TSDataType.DOUBLE);
                    break;
                default:
                    LOGGER.error("UDF {} time series data type error, get {}", this.udfName, dataType);
                    break;
            }
        }
        this.timeSeries.add(new ArrayList<>(singleTimeSeries));
    }

    public String getUdfName() {
        return udfName;
    }

    public String getFullClassName() {
        return fullClassName;
    }

    public List<List<TSDataType>> getTimeSeries() {
        return timeSeries;
    }

    public Map<String, String> getArguments() {
        return arguments;
    }
}
