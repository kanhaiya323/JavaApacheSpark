package com.JavaApacheSpark.CreateRDDusingParallelize;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

public class CrateRDDusingParallelizeTest {

    private final SparkConf sparkConf = new SparkConf()
            .setAppName("CrateRDDusingParallelizeTest")
            .setMaster("local[*]");

    @Test
    @DisplayName("Create an Empty RDD With No Partition In Spark")
    void CreateEmptyRDDWithNoPartitionInSpark() {
        try (final var sc = new JavaSparkContext(sparkConf)) {

            final var emptyRDD = sc.emptyRDD();

            System.out.println(emptyRDD);
            System.out.printf("Number of partition : %d%n", emptyRDD.getNumPartitions());

        }
    }

    @Test
    @DisplayName("Create an Empty RDD with default partition in spark")
    void CreateEmptyRDDWithDefaultPartitionInSpark() {
        try (final var sc = new JavaSparkContext(sparkConf)) {
            final var emptyRDD = sc.parallelize(List.of());
            System.out.println(emptyRDD);
            System.out.printf("Number of Partition:%d%n", emptyRDD.getNumPartitions());
        }
    }


    @Test
    @DisplayName("Create Spark RDD from java collection using Parallize method")
    void CreateSparkRDDUsingParallizeMethod(){
        try(final var sc= new JavaSparkContext(sparkConf)){
            final var data= Stream.iterate(1,n->n+1)
                    .limit(8L)
                    .toList();
            final var myRDD= sc.parallelize(data,7);
            System.out.println(myRDD);
            System.out.printf("Number of partition :%d%n",myRDD.getNumPartitions());
            System.out.printf("Total number of element :%d%n",myRDD.count());
            System.out.println("Elements of RDD :");
            myRDD.collect().forEach(System.out::println);

            final var max =myRDD.reduce(Integer::max);
            final var min =myRDD.reduce(Integer::min);
            final var sum =myRDD.reduce(Integer::sum);

            System.out.printf("Max~%d,Min~%d,Sum~%d", max,min,sum);

        }
    }
}
