package com.sparkTutorial.rdd.sumOfNumbers;

import java.util.Arrays;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SumOfNumbersProblem {

  public static void main(String[] args) {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */

    Logger.getLogger("org").setLevel(Level.ERROR);

    SparkConf conf = new SparkConf().setAppName("Union Log").setMaster("local[*]");
    JavaSparkContext context = new JavaSparkContext(conf);

    JavaRDD<String> input = context.textFile("in/prime_nums.text");
    JavaRDD<String> primeNumberStrings = input
        .flatMap((String line) -> Arrays.asList(line.split("\\s+")).iterator())
        .filter((String s) -> !s.isEmpty());

    JavaRDD<Long> primeNumbers = primeNumberStrings.map(Long::valueOf);
    Long primeNumbersSum = primeNumbers.reduce(Long::sum);

    System.out.println(primeNumbersSum);
  }
}
