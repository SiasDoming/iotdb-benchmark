package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.List;

public class RangedUDFQuery extends RangeQuery{
    private String udfName;

    public RangedUDFQuery(
            List<DeviceSchema> deviceSchema, long startTimestamp, long endTimestamp,
            String udfName) {
        super(deviceSchema, startTimestamp, endTimestamp);
        this.udfName = udfName;
    }

    public String getUdfName() {
        return this.udfName;
    }

}
