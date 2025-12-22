package com.JavaApacheSpark.FlatMapTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

public class RDDFlatMapTest {

    final SparkConf sparkConf = new SparkConf()
                                        .setAppName("RDDFlatMapTest")
                                        .setMaster("local[*]");
    @Test
    @DisplayName("test Flat Map Using Spark RDD")
    void testFlatMappinSparkRDD(){
        try(final var sc= new JavaSparkContext(sparkConf)){
            final String filepath = Path.of("src","test","resources","magna-carta.txt.gz").toString();
            var lines = sc.textFile(filepath);
            System.out.printf("Total lines: %s\n", lines.count());
           JavaRDD<List<String>> mappedLines =  lines.map(line-> {
                                                    String regex = "\\s";
                                                    return List.of(line.split(regex));
                                                    });
            System.out.printf("Total mapped lines: %s\n", mappedLines.count());
          JavaRDD<String> words = lines.flatMap(line->List.of(line.split("\\s")).iterator());
            System.out.printf("Total flatMapped lines: %s\n", words.count());
            System.out.printf("first 10 words: %s\n", words.take(10));
        }
    }
}
