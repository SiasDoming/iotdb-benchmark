package cn.edu.tsinghua.iotdb.quality.utils;

import java.util.ArrayList;
import java.util.List;

public class DeviceSchema {

    // storage group
    private String group;
    // device id
    private String device;
    // sensor id list
    private List<String> sensors;
    // data type corresponding to each given sensor time series
    private List<DataType> dataTypes;

    public DeviceSchema() {
        this.sensors = new ArrayList<>();
        this.dataTypes = new ArrayList<>();
    }

    public DeviceSchema(String group, String device, List<String> sensors, List<DataType> dataTypes) {
        this.group = group;
        this.device = device;
        this.sensors = sensors;
        this.dataTypes = dataTypes;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public List<String> getSensors() {
        return sensors;
    }

    public void setSensors(List<String> sensors) {
        this.sensors = sensors;
    }

    public List<DataType> getDataTypes() {
        return dataTypes;
    }

    public void setDataTypes(List<DataType> dataTypes) {
        this.dataTypes = dataTypes;
    }

}
