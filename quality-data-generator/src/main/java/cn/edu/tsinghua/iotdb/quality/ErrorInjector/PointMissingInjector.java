package cn.edu.tsinghua.iotdb.quality.ErrorInjector;

import cn.edu.tsinghua.iotdb.quality.utils.Batch;

import java.util.Random;

public class PointMissingInjector implements IErrorInjector{

    // missing rate, probability of each single point to be missing actually
    double missingRate;
    // random number generator
    Random randomEngine;

    @Override
    public void init() {
        this.missingRate = 0.0;
        this.randomEngine = new Random();
    }

    @Override
    public Batch injectError(Batch batch) {
        int pointIndex = 0;
        while (pointIndex < batch.size()) {
            if (randomEngine.nextDouble() < missingRate) {
                batch.getRecords().remove(pointIndex);
            }
            else {
                pointIndex ++;
            }
        }
        return batch;
    }

    public double getMissingRate() {
        return missingRate;
    }

    public void setMissingRate(double missingRate) {
        this.missingRate = missingRate;
    }

    public void setRandomSeed(long seed) {
        this.randomEngine = new Random(seed);
    }
}
