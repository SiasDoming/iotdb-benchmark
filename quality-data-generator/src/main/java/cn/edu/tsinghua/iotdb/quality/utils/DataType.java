package cn.edu.tsinghua.iotdb.quality.utils;

public enum DataType {

    // supported data types in IoTDB:
    BOOLEAN("BOOLEAN"),
    INT32("INT32"),
    INT64("INT64"),
    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),
    TEXT("TEXT");

    DataType(String dataTypeName) {
        this.dataTypeName = dataTypeName;
    }

    public String dataTypeName;

    public String getDataTypeName() {
        return dataTypeName;
    }
}
