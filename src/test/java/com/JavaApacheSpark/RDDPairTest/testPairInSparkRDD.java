package com.JavaApacheSpark.RDDPairTest;

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

    private static Stream<Arguments> getFilePath(){

        return Stream.of(Arguments.of(Path.of("src","test","resources","1000words.txt").toString()),
                Arguments.of(Path.of("src","test","resources","wordslist.txt.gz").toString())
        );
    }
}
