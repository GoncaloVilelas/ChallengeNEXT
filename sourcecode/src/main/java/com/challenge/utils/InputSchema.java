package utils;

import org.apache.spark.sql.types.StructType;

/**
 * Class that represents a Input file.
*/
public final class InputSchema {

    private static StructType schema;

    private InputSchema() {

    }

    /**
     * Method to retrieve the schema of a Input file.
    */
    public static StructType getSchema() {
        schema = new StructType()
        .add("PhoneNumber", "string");
        return schema;
    }

}
