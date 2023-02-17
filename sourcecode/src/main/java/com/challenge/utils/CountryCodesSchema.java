package utils;

import org.apache.spark.sql.types.StructType;

/** 123 .**/
public final class CountryCodesSchema {

    private static StructType schema;

    private CountryCodesSchema() {

    }

    /** 123 .*/
    public static StructType getSchema() {
        schema = new StructType()
        .add("Country", "string")
        .add("Code", "string");
        return schema;
    }

}
