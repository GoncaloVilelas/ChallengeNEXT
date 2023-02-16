package com.challenge;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** */
public final class App {

    // A text dataset is pointed to by path.
    // The path can be either a single text file or a directory of text files
    private String path = "sourcecode/src/utils/coutryCodes.txt";
    private SparkSession spark = SparkSession
        .builder()
        .appName("NEXT Challenge")
        .getOrCreate();
    private Dataset<Row> df1 = spark.read().text(path);

    private App() {
    }

    public void showDataframe() {
        df1.show();
    }

    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {
        App appTest = new App();
        appTest.showDataframe();
    }
}
