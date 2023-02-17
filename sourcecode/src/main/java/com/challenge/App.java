package com.challenge;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import utils.CountryCodesSchema;
import utils.InputSchema;

/** */
public final class App {

    private static final int MIN_SHORT_DIGITS = 4;
    private static final int MAX_SHORT_DIGITS = 6;

    // A text dataset is pointed to by path.
    // The path can be either a single text file or a directory of text files
    private String pathCountryCodes = "src/main/java/com/challenge/utils/coutryCodes.txt";
    private SparkSession spark;
    private Dataset<Row> dfCountryCodes;
    private Dataset<Row> dfInput;
    private StructType countryCodeSchema;
    private StructType inputSchema;

    /** */
    private App(final String pathInputFile) {
        countryCodeSchema = CountryCodesSchema.getSchema();
        inputSchema = InputSchema.getSchema();
        spark = SparkSession
            .builder()
            .master("local")
            .appName("NEXT Challenge")
            .getOrCreate();
        dfCountryCodes = spark.read()
                .format("csv")
                .option("header", false)
                .option("delimiter", "-")
                .schema(countryCodeSchema)
                .load(pathCountryCodes);
        dfInput = spark.read()
                .format("csv")
                .option("header", false)
                .option("delimiter", "-")
                .schema(inputSchema)
                .load(pathInputFile);
    }

    /** */
    public void filterShortNumbers() throws Exception {
        dfInput.createOrReplaceTempView("tempView");
        dfInput = spark.sql("SELECT PhoneNumber FROM tempView"
        + " WHERE LENGTH(PhoneNumber) >= " + MIN_SHORT_DIGITS + " AND"
        + " LENGTH(PhoneNumber) <= " + MAX_SHORT_DIGITS + " AND"
        + " PhoneNumber NOT LIKE '0%' AND INSTR(PhoneNumber,' ') = 0");
        dfInput.show();
    }

    /**
     * Says hello to the world.

     * @param args The arguments of the program.
     */
    public static void main(final String[] args) throws Exception {
        App appTest = new App(args[0]);
        appTest.filterShortNumbers();
    }
}
