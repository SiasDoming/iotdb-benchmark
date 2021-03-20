package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import java.util.List;

public class RangedUDFQuery extends RangeQuery{
    private String udfName;
    private String udfFullClassName;

    public RangedUDFQuery(
            List<DeviceSchema> deviceSchema, long startTimestamp, long endTimestamp,
            String udfName, String udfFullClassName) {
        super(deviceSchema, startTimestamp, endTimestamp);
        this.udfName = udfName;
        this.udfFullClassName = udfFullClassName;
    }

    public String getUdfName() {
        return this.udfName;
    }

    public String getUdfFullClassName() {
        return udfFullClassName;
    }
}
