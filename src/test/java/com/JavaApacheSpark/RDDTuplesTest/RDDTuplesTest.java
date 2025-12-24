package com.JavaApacheSpark.RDDTuplesTest;

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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RDDTuplesTest {

    final SparkConf sparkConf = new SparkConf().setAppName("RDDTuplesTest").setMaster("local[*]");

    @ParameterizedTest
    @MethodSource("getFilePath")
    @DisplayName("Test Tuple in Spark RDD")
    void testTuplesWithSparkRDD(String filePath){
        try(JavaSparkContext sc = new JavaSparkContext(sparkConf)){
            JavaRDD<String> myRDD = sc.textFile(filePath);

            JavaRDD<Tuple2<String,Integer>> tuple2JavaRDD
                    = myRDD.map(line -> new Tuple2<>(line,line.length()));

            assertEquals(myRDD.count(),tuple2JavaRDD.count());

         tuple2JavaRDD.filter(x->x._2>5).groupBy(x->x._2).take(10).forEach(data -> System.out.println(data));
            System.out.println("-----------------------");


        }
    }

    private static Stream<Arguments> getFilePath(){
        return Stream.of(
                Arguments.of(Path.of("src","test","resources","1000words.txt").toString()),
                Arguments.of(Path.of("src","test","resources","wordslist.txt.gz").toString())
        );
    }
}
