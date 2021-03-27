package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import java.util.List;

public class UDFRangeQuery extends RangeQuery{
    // 增加更多信息，匹配JSON输入内容
    private String udfName;
    private String udfFullClassName;

    public UDFRangeQuery(
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
