package cn.edu.tsinghua.iotdb.quality.ErrorInjector;

import cn.edu.tsinghua.iotdb.quality.utils.Batch;

public interface IErrorInjector {
    /*
     * Initialize injector, including but not limited to: random module initialization and default arguments.
     * Called multiple times each instance is constructed or reset.
     */
    void init();

    /*
     * Inject error data into given batch.
     */
    Batch injectError(Batch batch);
}
