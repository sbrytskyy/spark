package com.sparkTutorial.pairRdd.sort;


import com.sparkTutorial.rdd.commons.Utils;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger logger = Logger.getLogger(SortedWordCountProblem.class);

    SparkConf conf = new SparkConf().setAppName("SortedWordCountProblem").setMaster("local[2]");
    JavaSparkContext ctx = new JavaSparkContext(conf);

    JavaRDD<String> inputRDD = ctx.textFile("in/word_count.text");

    JavaRDD<String> wordsRDD = inputRDD
        .flatMap(line -> Arrays.asList(line.split("\\W+")).iterator());
    JavaPairRDD<String, Integer> wordPairsRDD = wordsRDD
        .mapToPair(word -> new Tuple2<>(word, 1));

    JavaPairRDD<String, Integer> wordsCounterRDD = wordPairsRDD.reduceByKey(Integer::sum);
    JavaPairRDD<String, Integer> sortedWordsCounterRDD = wordsCounterRDD.sortByKey();

    List<Tuple2<String, Integer>> collect = sortedWordsCounterRDD.collect();
    for (Tuple2<String, Integer> pair : collect) {
      logger.info(pair._1() + ":" + pair._2());
    }
  }
}

