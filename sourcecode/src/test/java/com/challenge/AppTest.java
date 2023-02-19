package com.challenge;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for simple App.
 */
@ExtendWith(SparkExtension.class)
class AppTest {

    @SparkSessionField private SparkSession sparkSession;

    /**
     * Test spark session is not null.
     */
    @Test
    void testSparkSession() {
        assertNotNull(sparkSession);
    }

    /**
     * Rigorous Test.
     */
    @Test
    void testApp() {
        assertEquals(1, 1);
    }
}
