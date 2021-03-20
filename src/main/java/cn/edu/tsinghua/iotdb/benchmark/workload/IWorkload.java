package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangedUDFQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

public interface IWorkload {

  Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex) throws WorkloadException;

  RangeQuery getRangeQuery() throws WorkloadException;

  AggRangeQuery getAggRangeQuery() throws WorkloadException;

  RangedUDFQuery getRangedUDFQuery() throws WorkloadException;
}
