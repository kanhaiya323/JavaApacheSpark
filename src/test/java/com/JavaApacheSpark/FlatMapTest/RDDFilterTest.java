package com.JavaApacheSpark.FlatMapTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

public class RDDFilterTest {
    final SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDFilterTest");

    @Test
    void testRDDFilter() {
        try (final var sc = new JavaSparkContext(sparkConf)) {
            final String filePath = Path.of("src", "test", "resources", "magna-carta.txt.gz").toString();

            JavaRDD<String> myRDD = sc.textFile(filePath);

            var words = myRDD.flatMap(word -> List.of(word.split("\\s")).iterator())
                    .filter(word -> (word != null) && (!word.trim().isEmpty()));
            System.out.printf("Total lines: %s\n", words.count());
            System.out.printf("first 10 words: %s\n", words.take(10));
        }
    }
}
