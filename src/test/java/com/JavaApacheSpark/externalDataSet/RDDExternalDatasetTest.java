package com.JavaApacheSpark.externalDataSet;

import net.bytebuddy.asm.Advice;
import net.bytebuddy.implementation.bind.annotation.Argument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.stream.Stream;

public class RDDExternalDatasetTest {

    private final SparkConf sparkConf = new SparkConf()
            .setAppName("RDDExternalDatasetTest")
            .setMaster("local[*]");

    @ParameterizedTest
    @ValueSource(strings = {
            "src/test/resources/1000words.txt",
            "src/test/resources/wordslist.txt.gz"
    })
    @DisplayName("Test loading local test files into Spark RDD ")
    void testLoadingLocalTextFilesIntoSparkRDD(String localFilePath) {
        try (final var sc = new JavaSparkContext(sparkConf)) {
            var myRDD = sc.textFile(localFilePath);

            System.out.printf("Total lines in the file :%d%n", myRDD.count());
            System.out.println("Printing first 10 lines ");
            myRDD.take(10).forEach(System.out::println);

            System.out.println("----------------------------\n");
        }
    }


    @ParameterizedTest
    @MethodSource("getFilePath")
    @DisplayName("Test loading local test file into spark RDD using Method Source")
    void testLoadingLocalTestFilesIntoSparkRDDUsingMethodSource(final String localFilePath) {
        try (final var sc = new JavaSparkContext(sparkConf)) {
            var myRDD = sc.textFile(localFilePath);

            System.out.printf("Total lines in the file :%d%n", myRDD.count());
            System.out.println("Printing first 10 lines ");
            myRDD.take(10).forEach(System.out::println);

            System.out.println("----------------------------\n");

        }
    }

    @Test
    @DisplayName("Test loading whole directory into Spark RDD")
    void testLoadingWholeDirectoryFilesIntoSparkRDD(){
        try(final var sc = new JavaSparkContext(sparkConf)){
            var directoryFilePath = Path.of("src","test","resources").toString();

            var myRDD= sc.wholeTextFiles(directoryFilePath);

            System.out.printf("Total number of files in Directory %s = %d%n", directoryFilePath,myRDD.count());

            myRDD.collect().forEach(tuple->{
                System.out.printf("File Name : %s%n",tuple._1);
                System.out.println("------------------------");
                if(tuple._1.endsWith("properties")){
                    System.out.printf("content of [%s]:%n",tuple._1);
                    System.out.println(tuple._2);
                }
            });

        }

    }

    @Test
    @DisplayName("Test loading CSV file in Spark RDD")
    void testLoadingCSVFileinSparkRDD(){
        try(final var sc= new JavaSparkContext(sparkConf)){

            final var testCSVFilePath = Path.of("src","test","resources","dma.csv").toString();

            var myRDD= sc.textFile(testCSVFilePath);

            System.out.printf("Total number of lines in CSV file [%s] = %d%n",testCSVFilePath,myRDD.count());

            System.out.println("CSV Header");
            System.out.println(myRDD.first());
            System.out.println("---------------------------------");
            System.out.println("Printing first 10 lines ->");
            myRDD.take(10).forEach(System.out::println);
            System.out.println("---------------------------------");

            final var csvFields = myRDD.map(line->line.split(","));
            csvFields.take(5).forEach(field->System.out.println(String.join("|",field)));
        }

    }

    private static Stream<Arguments> getFilePath() {
        return Stream.of(
                Arguments.of(Path.of("src/test/resources/1000words.txt").toString()),
                Arguments.of(Path.of("src/test/resources/wordslist.txt.gz").toString())
        );
    }

}
