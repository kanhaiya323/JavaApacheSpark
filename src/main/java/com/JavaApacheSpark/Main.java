package com.JavaApacheSpark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {
    public static void main(String[] args) {

     try (final var spark =   SparkSession.builder()
                                .appName("Spark Tutorial")
                                .master("local[*]")
                                //.config("spark.driver.bindAddress", "127.0.0.1")
                                .getOrCreate();
          final var sc= new JavaSparkContext(spark.sparkContext())) {

         final var data = Stream.iterate(1, n -> n + 1)
                 .limit(5)
                 .toList();

         final var myRdd=sc.parallelize(data);
         System.out.printf("Total elements in RDD: %d%n",myRdd.count());

         System.out.printf("Number of partition: %d%n", myRdd.getNumPartitions());

         final var max = myRdd.reduce(Integer::max);
         final var min =myRdd.reduce(Integer::min);
         final var sum =myRdd.reduce(Integer::sum);

         System.out.printf("Max~%d,Min~%d,Sum~%d", max,min,sum);
        try (final var scanner = new Scanner(System.in)){
            scanner.nextLine();
        }
     }
    }
}