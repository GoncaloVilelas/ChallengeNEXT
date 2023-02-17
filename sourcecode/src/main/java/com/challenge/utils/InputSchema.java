package utils;

import org.apache.spark.sql.types.StructType;

/** 123 .**/
public final class InputSchema {

    private static StructType schema;

    private InputSchema() {

    }

    /** 123 .*/
    public static StructType getSchema() {
        schema = new StructType()
        .add("PhoneNumber", "string");
        return schema;
    }

}
