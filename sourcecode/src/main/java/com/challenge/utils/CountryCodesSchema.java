package utils;

import org.apache.spark.sql.types.StructType;

/**
 * Class that represents a Country Code file.
*/
public final class CountryCodesSchema {

    private static StructType schema;

    private CountryCodesSchema() {

    }

    /**
     * Method to retrieve the schema of a Country code file.
    */
    public static StructType getSchema() {
        schema = new StructType()
        .add("Country", "string")
        .add("Code", "string");
        return schema;
    }

}
