package utils;

import org.apache.spark.sql.types.StructType;

/** 123 .**/
public final class TxtSchema {

    private static StructType schema;

    private TxtSchema() {

    }

    /** 123 .*/
    public static StructType getSchema() {
        schema = new StructType()
        .add("Country code", "string")
        .add("Phone number", "int");
        return schema;
    }

}
