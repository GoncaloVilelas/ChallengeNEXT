package com.challenge;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for NEXT challenge.
 */
class AppTest {

    private SparkSession spark =  SparkSession
        .builder()
        .master("local")
        .appName("NEXT Challenge")
        .getOrCreate();

    /**
     * Test method filterShortNumbers() with mocked file and expected
     * dataframe.
     */
    @Test
    void testFilterShortNumbers() throws Exception {

        StructType structType = new StructType();
        structType = structType.add("PhoneNumber", DataTypes.StringType, false);

        List<Row> expectedNums = new ArrayList<Row>();
        expectedNums.add(RowFactory.create("9874"));
        expectedNums.add(RowFactory.create("65468"));
        expectedNums.add(RowFactory.create("88876"));
        expectedNums.add(RowFactory.create("446670"));

        Dataset<Row> df = spark.createDataFrame(expectedNums, structType);

        App test = new App("src/test/java/com/challenge/input.txt");
        Dataset<Row> shortNumbersDf = test.filterShortNumbers();
        assertEquals(df.collectAsList(), shortNumbersDf.collectAsList());
    }

    /**
     * Test method filterLongNumbers() with mocked file and expected
     * dataframe.
     */
    @Test
    void testFilterLongNumbers() throws Exception {

        StructType structType = new StructType();
        structType = structType.add("PhoneNumber", DataTypes.StringType, false);

        List<Row> expectedNums = new ArrayList<Row>();
        expectedNums.add(RowFactory.create("4465465444"));
        expectedNums.add(RowFactory.create("351918878443"));
        expectedNums.add(RowFactory.create("198798798"));
        expectedNums.add(RowFactory.create("42298798798"));

        Dataset<Row> df = spark.createDataFrame(expectedNums, structType);

        App test = new App("src/test/java/com/challenge/input.txt");
        Dataset<Row> longNumbersDf = test.filterLongNumbers();
        assertEquals(df.collectAsList(), longNumbersDf.collectAsList());
    }

    /**
     * Test method joinBothDataframes() with mocked dataframes.
     */
    @Test
    void testJoinBothDataframes() throws Exception {

        StructType structType = new StructType();
        structType = structType.add("PhoneNumber",
            DataTypes.StringType, false);

        StructType expectedStructType = new StructType();
        expectedStructType = expectedStructType.add("Country",
            DataTypes.StringType, false);
        expectedStructType = expectedStructType.add("count",
            DataTypes.IntegerType, false);

        List<Row> expectedList = new ArrayList<Row>();
        expectedList.add(RowFactory.create("Bangladesh", 1));
        expectedList.add(RowFactory.create("Portugal", 1));

        List<Row> phoneNumbersList = new ArrayList<Row>();
        phoneNumbersList.add(RowFactory.create("351918878443"));
        phoneNumbersList.add(RowFactory.create("880918878443"));

        Dataset<Row> phoneNumbersDf = spark.createDataFrame(
            phoneNumbersList, structType);

        Dataset<Row> expectedDf = spark.createDataFrame(
            expectedList, expectedStructType);

        App test = new App("src/test/java/com/challenge/input.txt");
        Dataset<Row> resultDf = test.joinBothDataframes(phoneNumbersDf);
        assertEquals(expectedDf.collectAsList(), resultDf.collectAsList());
    }

    /**
     * Test method addShortNumbers() with mocked dataframes.
     */
    @Test
    void testAddShortNumbers() throws Exception {

        final int portugalExpected = 5;

        StructType structType = new StructType();
        structType = structType.add("PhoneNumber",
            DataTypes.StringType, false);

        StructType expectedStructType = new StructType();
        expectedStructType = expectedStructType.add("Country",
            DataTypes.StringType, false);
        expectedStructType = expectedStructType.add("count",
            DataTypes.IntegerType, false);

        List<Row> expectedList = new ArrayList<Row>();
        expectedList.add(RowFactory.create("Portugal", portugalExpected));
        expectedList.add(RowFactory.create("Bangladesh", 1));

        List<Row> shortNumbersList = new ArrayList<Row>();
        shortNumbersList.add(RowFactory.create("9874"));
        shortNumbersList.add(RowFactory.create("65468"));
        shortNumbersList.add(RowFactory.create("88876"));
        shortNumbersList.add(RowFactory.create("446670"));

        List<Row> sumNumbersList = new ArrayList<Row>();
        sumNumbersList.add(RowFactory.create("Bangladesh", 1));
        sumNumbersList.add(RowFactory.create("Portugal", 1));

        Dataset<Row> shortNumbersDf = spark.createDataFrame(
            shortNumbersList, structType);

        Dataset<Row> sumNumbersDf = spark.createDataFrame(
            sumNumbersList, expectedStructType);

        Dataset<Row> expectedDf = spark.createDataFrame(
            expectedList, expectedStructType);

        App test = new App("src/test/java/com/challenge/input.txt");
        Dataset<Row> resultDf = test.addShortNumbers(shortNumbersDf,
            sumNumbersDf);
        assertEquals(expectedDf.collectAsList(), resultDf.collectAsList());
    }
}
