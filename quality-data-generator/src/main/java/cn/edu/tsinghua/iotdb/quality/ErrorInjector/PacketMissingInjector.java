package cn.edu.tsinghua.iotdb.quality.ErrorInjector;

import cn.edu.tsinghua.iotdb.quality.utils.Batch;

import java.util.Random;

public class PacketMissingInjector implements IErrorInjector{

    // packet size, number of points in each point
    int packetSize;
    // missing rate, probability of each single point to be missing actually
    double missingRate;
    // random number generator
    Random randomEngine;

    @Override
    public void init() {
        this.packetSize = 1;
        this.missingRate = 0.0;
        this.randomEngine = new Random();
    }

    @Override
    public Batch injectError(Batch batch) {
        int packetIndex = 0;
        while (packetIndex < batch.size()) {
            if (randomEngine.nextDouble() < missingRate) {
                int packetCount = 0;
                while (packetCount < packetSize && packetIndex < batch.size()) {
                    batch.getRecords().remove(packetIndex);
                    packetCount ++;
                }
            }
            else {
                packetIndex += packetSize;
            }
        }
        return batch;
    }

    public int getPacketSize() {
        return packetSize;
    }

    public void setPacketSize(int packetSize) {
        this.packetSize = packetSize;
    }

    public double getMissingRate() {
        return missingRate;
    }

    public void setMissingRate(double missingRate) {
        this.missingRate = missingRate;
    }

    public Random getRandomEngine() {
        return randomEngine;
    }

    public void setRandomEngine(Random randomEngine) {
        this.randomEngine = randomEngine;
    }
}
