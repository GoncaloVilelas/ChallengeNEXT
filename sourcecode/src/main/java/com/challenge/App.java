package com.challenge;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import utils.TxtSchema;

/** */
public final class App {

    // A text dataset is pointed to by path.
    // The path can be either a single text file or a directory of text files
    private String path = "src/main/java/com/challenge/utils/";
    private SparkSession spark = SparkSession
        .builder()
        .master("local")
        .appName("NEXT Challenge")
        .getOrCreate();
    private Dataset<Row> df1;
    private StructType schema;

    private App(String textFile) {
        path = path + textFile;
        schema = TxtSchema.getSchema();
        df1 = spark.read()
            .format("csv")
            .option("header", false)
            .option("delimiter", "-")
            .schema(schema)
            .load(path);
    }

    public void showDataframe() {
        df1.show();
    }

    public void filterShortNumbers() {
        df1.printSchema();
    }

    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {
        App appTest = new App(args[0]);
        appTest.showDataframe();
    }
}
