package com.challenge;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import utils.CountryCodesSchema;
import utils.InputSchema;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lpad;
import static org.apache.spark.sql.functions.regexp_replace;

/** */
public final class App {

    private static Dataset<Row> dfInput;
    private static final int MIN_SHORT_DIGITS = 4;
    private static final int MAX_SHORT_DIGITS = 6;
    private static final int MIN_LONG_DIGITS = 9;
    private static final int MAX_LONG_DIGITS = 14;
    private static final int PADDING_COUNTRY_CODE = 4;

    // A text dataset is pointed to by path.
    // The path can be either a single text file or a directory of text files
    private String pathCountryCodes = "src/main/java/com/challenge/utils/coutryCodes.txt";
    private SparkSession spark;
    private Dataset<Row> dfCountryCodes;
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
    public void paddCountryCodes() {
        Dataset<Row> df = dfCountryCodes.withColumn("Code",
            lpad(col("Code"), PADDING_COUNTRY_CODE, "0")
        );

        df.show();
    }

    /**
     *  @throws Exception ee
     *  @return tempDf
    */
    public Dataset<Row> filterShortNumbers() throws Exception {
        dfInput.createOrReplaceTempView("tempView");
        Dataset<Row> tempDf = spark.sql("SELECT PhoneNumber FROM tempView"
            + " WHERE LENGTH(PhoneNumber) >= " + MIN_SHORT_DIGITS + " AND"
            + " LENGTH(PhoneNumber) <= " + MAX_SHORT_DIGITS + " AND"
            + " PhoneNumber NOT LIKE '0%' AND INSTR(PhoneNumber,' ') = 0");
        tempDf.show();

        return tempDf;
    }

    /**
     *  @throws Exception ee
     *  @return tempDf
    */
    public Dataset<Row> filterLongNumbers()
        throws Exception {
        Dataset<Row> tempDf = spark.sql("SELECT * from tempView where "
            + "rlike(PhoneNumber,'^[0-9]*$') OR PhoneNumber LIKE '+%'");

        Dataset<Row> tempDf1 = tempDf.withColumn("PhoneNumber",
            regexp_replace(col("PhoneNumber"), "[^A-Z0-9_]", ""));

        tempDf1.createOrReplaceTempView("tempViewLong2");
        tempDf1 = spark.sql("SELECT PhoneNumber FROM tempViewLong2"
        + " WHERE LENGTH(PhoneNumber) >= " + MIN_LONG_DIGITS + " AND"
        + " LENGTH(PhoneNumber) <= " + MAX_LONG_DIGITS);
        tempDf1.show();

        return tempDf1;
    }

    /**
     * Says hello to the world.

     * @param args The arguments of the program.
     * @throws Exception ee
     */
    public static void main(final String[] args) throws Exception {
        App appTest = new App(args[0]);
        appTest.paddCountryCodes();
        appTest.filterShortNumbers();
        appTest.filterLongNumbers();
    }
}
