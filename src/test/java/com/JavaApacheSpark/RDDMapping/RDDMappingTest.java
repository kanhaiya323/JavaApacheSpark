package com.JavaApacheSpark.RDDMapping;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.SparkConf;
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

public class RDDMappingTest {
    private final SparkConf sparkconf = new SparkConf()
            .setAppName("RDDMappingTest")
            .setMaster("local[*]");

    private static final List<String> data = new ArrayList<>();
    private static final int noOfIterations = 10;

    @BeforeAll
    static void beforeAll() {
        final var dataSize = 1_000_000;
        for (int i = 0; i < dataSize; i++) {
            data.add(RandomStringUtils.randomAscii(ThreadLocalRandom.current().nextInt(10)));
        }
        assertEquals(dataSize, data.size());
    }

    @Test
    @DisplayName("Test map operation using Spark RDD count()")
    void testMapOperationUsingSparkRDDCount() {
        try (final var sc = new JavaSparkContext(sparkconf)) {
            final var myRDD = sc.parallelize(data);
            final Instant start = Instant.now();
            for (int i = 0; i < noOfIterations; i++) {
                final var stringLenght = myRDD.map(data -> data.length()).count();

                assertEquals(data.size(), stringLenght);
            }

            final var timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / 10;

            System.out.printf("Spark RDD Count() method time taken : %d  ms%n%n", timeElapsed);


        }
    }

    @Test
    @DisplayName("Test Map operation using Spark RDD collect() method ")
    void testMapOperationUisngSparkRDDCollect() {
        try (final var sc = new JavaSparkContext(sparkconf)) {
            final var myRDD = sc.parallelize(data);
            final Instant start = Instant.now();
            for (int i = 0; i < noOfIterations; i++) {

                final var stringLenght = myRDD.map(x -> x.length()).collect();
                assertEquals(data.size(), stringLenght.size());
            }
            final var timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / 10;
            System.out.printf("spark RDD Collect() method time elapsed : %d ms%n", timeElapsed);

        }

    }

    @Test
    @DisplayName("Test Map Operation using Spark RDD Reduce")
    void testMapoperationUsingSparkRDDReduce(){
        try(final var sc= new JavaSparkContext(sparkconf)) {
            final var myRDD = sc.parallelize(data);
            final var start = Instant.now();
            for (int i = 0; i < noOfIterations; i++) {
                final var stringlength = myRDD.map(x -> 1l)
                        .reduce((a, b) -> a + b);

                assertEquals(data.size(), stringlength);
            }
            final var timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / 10;
            System.out.printf("Spark RDD reduce() method time elapsed : %d ms%n", timeElapsed);
        } 

    }

}
