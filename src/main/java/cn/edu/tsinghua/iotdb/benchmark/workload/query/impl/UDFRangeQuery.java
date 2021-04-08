package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import java.util.List;
import java.util.Map;

public class UDFRangeQuery extends RangeQuery{

    private String udfName;
    private String udfFullClassName;
    private int timeSeriesNumber;
    private Map<String, String> arguments;

    public UDFRangeQuery(
            List<DeviceSchema> deviceSchema, long startTimestamp, long endTimestamp,
            String udfName, String udfFullClassName, int timeSeriesNumber, Map<String, String> arguments) {
        super(deviceSchema, startTimestamp, endTimestamp);
        this.udfName = udfName;
        this.udfFullClassName = udfFullClassName;
        this.timeSeriesNumber = timeSeriesNumber;
        this.arguments = arguments;
    }

    public String getUdfName() {
        return this.udfName;
    }

    public String getUdfFullClassName() {
        return udfFullClassName;
    }

    public int getTimeSeriesNumber() {
        return timeSeriesNumber;
    }

    public Map<String, String> getArguments() {
        return arguments;
    }
}
