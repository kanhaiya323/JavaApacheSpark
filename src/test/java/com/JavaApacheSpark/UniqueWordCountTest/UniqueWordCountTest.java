package com.JavaApacheSpark.UniqueWordCountTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class UniqueWordCountTest {
    final SparkConf sparkConf = new SparkConf().setAppName("UniqueWordCountTest").setMaster("local[*]");


    @ParameterizedTest
    @MethodSource("getFilePath")
    @DisplayName("")
    void testUniqueWordCountUsingSparkRDD(String filePath) {
        try(JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> myRdd= jsc.textFile(filePath);

            myRdd.map(data->data.replaceAll("[^a-zA-Z\\s]","").toLowerCase())
                    .flatMap(line -> List.of( line.split("\\s")).iterator())
                    .filter(word-> !(word.trim().isEmpty()))
                    .mapToPair(word->new Tuple2<>(word,1L)).reduceByKey((a,b)->a+b)
                    .take(10).forEach(x-> System.out.println(x));
            System.out.println("----------------------------------------------------");
 myRdd.map(data->data.replaceAll("[^a-zA-Z\\s]","").toLowerCase())
                    .flatMap(line -> List.of( line.split("\\s")).iterator())
                    .filter(word-> !(word.trim().isEmpty()))
                    .mapToPair(word->new Tuple2<>(word,1L))
                .reduceByKey((a,b)->a+b)
                .mapToPair(rdW->new Tuple2<>(rdW._2,rdW._1))
                .sortByKey(false)
                .take(10).forEach(x-> System.out.println(x));
        }

    }

    static Stream<Arguments> getFilePath() {
        return Stream.of(Arguments.of(Path.of("src", "test", "resources", "magna-carta.txt.gz").toString()));

    }
}
