package com.JavaApacheSpark.RDDPairTest;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class testPairInSparkRDD {
    final SparkConf sparkConf=new SparkConf().setMaster("local[*]").setAppName("testMapToPairInSparkRDD");

    @ParameterizedTest
    @MethodSource("getFilePath")
    @DisplayName("Test MapToPair in Spark RDD")
    public void testMapToPairInSparkRDD(String filePath) {
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> myRDD = jsc.textFile(filePath);

            var pairRDD = myRDD.mapToPair(line ->new Tuple2<>(line.length(),line)).sortByKey(false);
            System.out.println("---------------------");
            assertEquals(myRDD.count(),pairRDD.count());

            pairRDD.take(10).forEach(x->System.out.println("Line Length: "+ x._1+", Line: "+x._2));
        }
    }

    @ParameterizedTest
    @MethodSource("getFilePath")
    @DisplayName("Test reduceByKey using Spark RDD")
    void testReduceByKeyUsingSparkRDD(String filePath) {
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> myRDD = jsc.textFile(filePath);
            var pairRDD= myRDD.mapToPair(line ->new Tuple2<>(line.length(),1L));
            System.out.println("---------------------");
            assertEquals(pairRDD.count(),pairRDD.count());

            var countRDD = pairRDD.reduceByKey((a,b)->a+b);
            countRDD.take(5).forEach(x->System.out.printf("Line Length - %d , count - %d\n", x._1+1, x._2));
        }
    }

    @ParameterizedTest
    @MethodSource("getFilePath")
    @DisplayName("Test groupByKey using Spark RDD")
    void testGroupByKeyUsingSparkRDD(String filePath) {
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> myRDD = jsc.textFile(filePath);
            System.out.println("------------------------");
            var pairRDD= myRDD.mapToPair(line ->new Tuple2<>(line.length(),1L))
                            //  .reduceByKey((a,b)->a+b)
                              .groupByKey();
            pairRDD.take(5).forEach(result->System.out.printf("Line Length - %d , count - %d\n", result._1, size( result._2)));
            System.out.println("-----------------------");
            assertEquals(pairRDD.count(),pairRDD.count());
        }
    }


    @ParameterizedTest
    @MethodSource("getFilePath")
    @DisplayName(" Test Distinct using Spark RDD")
    void testDistinctUsingSparkRDD(String filePath) {
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> myRDD= jsc.textFile(filePath);
            System.out.println("-----------------");
            myRDD.mapToPair(word -> new Tuple2<>(word.length(),word)).distinct()
                    .take(10)
                    .forEach(x-> System.out.println("word: "+ x._2 + " length: "+x._1));

        }

    }

    private static Stream<Arguments> getFilePath(){

        return Stream.of(Arguments.of(Path.of("src","test","resources","1000words.txt").toString()),
                Arguments.of(Path.of("src","test","resources","wordslist.txt.gz").toString())
        );
    }
}
