package com.JavaApacheSpark.RDDReduceTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.json4s.ThreadLocal;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RDDReduceTest {
    private   SparkConf sparkConf=new SparkConf()
            .setAppName("RDDReduceTest")
            .setMaster("local[*]");

    private static  final List<Double>  data = new ArrayList<>();

    @BeforeAll
    static  void beforeAll() {
        final var dataSize = 1_000_000;
        for (int i = 0; i < dataSize; i++) {

            data.add(100 * ThreadLocalRandom.current().nextDouble() + 47);
        }
        assertEquals(dataSize, data.size());
    }

    @Test
    @DisplayName("Test Spark Reduce Method")
    void testSparkReduceMethod(){
        try(final var sc= new JavaSparkContext(sparkConf)) {

            final var myRDD = sc.parallelize(data);
            final Instant start = Instant.now();
            for (int i = 0; i < 10; i++) {

                final var sum = myRDD.reduce(Double::sum);
                System.out.println("spark RDD Reduce sum : "+ sum);
            }
            final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / 10;
            System.out.printf("spark RDD Resuce Time Taken :%d%n%n", timeElapsed);
        }
    }

    @Test
    @DisplayName("Test Spark RDD Reduce Method")
    void testSparkRDDFoldMethod(){
        try(final var sc= new JavaSparkContext(sparkConf)){
            final var myRDD= sc.parallelize(data);
            final Instant start = Instant.now();
            for(int i=0;i<10;i++){
                final var sum=myRDD.fold(0D,Double::sum);
                System.out.println("Spark RDD Fold sum :  "+ sum);
            }

            final long timeElapsed = (Duration.between(start,Instant.now()).toMillis())/10;
            System.out.printf("spark RDD fold Time Taken :%d%n%n", timeElapsed);
        }
    }
    @Test
    @DisplayName("Test Spark RDD Aggregate method")
    void testSparkRDDAggegrateMethod(){
        try(final var sc= new JavaSparkContext(sparkConf)){

            final var myRDD= sc.parallelize(data);
            final Instant start =Instant.now();
            for(int i=0;i<10;i++){
                final var sum=myRDD.aggregate(0D,Double::sum,Double::sum);
                final var max=myRDD.aggregate(0D,Double::sum,Double::max);
                final var min=myRDD.aggregate(0D,Double::sum,Double::min);
                System.out.println("Spark RDD Aggregate sum : " +sum);
                System.out.println("Spark RDD Aggregate max for all partition : " +max);
                System.out.println("Spark RDD Aggregate min for all partition : " +min);
            }

            final long timeElapsed = (Duration.between(start,Instant.now()).toMillis())/10;
            System.out.println("Spark RDD Aggregate : " +timeElapsed);

        }

    }
}
