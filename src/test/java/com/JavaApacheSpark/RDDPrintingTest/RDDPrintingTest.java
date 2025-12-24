package com.JavaApacheSpark.RDDPrintingTest;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RDDPrintingTest {
    final SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDPrintingTest");

    static final List<Double> data = new ArrayList<>();

    @BeforeAll
    static void beforeAll() {
        final var dataSize = 20;
        for (int i = 0; i < dataSize; i++) {
            data.add(100 * ThreadLocalRandom.current().nextDouble() + 47);
        }
        assertEquals(data.size(), dataSize);
    }

    @Test
    @DisplayName("Test printing spark RDD using collect foreach() only")
    void testPrintingSparkRDDElementsUsingForeachandcollect() {
        try (final JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            var myRDD = sc.parallelize(data);
            final Instant start = Instant.now();
//            Throwable sparkException = assertThrows(SparkException.class,
//                    () -> {
//                        myRDD.foreach(x -> System.out.println(x));
//                    });

            myRDD.collect().forEach(data -> System.out.println(data));

            System.out.printf("Time taken : %d ms%n", Duration.between(start, Instant.now()).toMillis());

        }
    }


    @Test
    @DisplayName("Test printing spark RDD using foreach with take() ")
    void testPrintingSparkRDDElementsUsingForEachTake() {
        try (final var SC = new JavaSparkContext(sparkConf)) {
        var myRDD= SC.parallelize(data);
            final Instant start = Instant.now();
            myRDD.take(10).forEach(data -> System.out.println(data));
            System.out.printf("Time taken : %d ms%n", Duration.between(start, Instant.now()).toMillis());
        }
    }
}
