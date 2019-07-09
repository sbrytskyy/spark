package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SameHostsProblem {

  public static void main(String[] args) {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */

    Logger.getLogger("org").setLevel(Level.ERROR);

    SparkConf conf = new SparkConf().setAppName("Union Log").setMaster("local[1]");
    JavaSparkContext context = new JavaSparkContext(conf);

    JavaRDD<String> input1 = context.textFile("in/nasa_19950701.tsv");
    JavaRDD<String> input2 = context.textFile("in/nasa_19950801.tsv");

    JavaRDD<String> inputFiltered1 = input1.filter(SameHostsProblem::isNotHeader);
    JavaRDD<String> inputFiltered2 = input2.filter(SameHostsProblem::isNotHeader);

    JavaRDD<String> hosts1 = inputFiltered1.map((String line) -> line.split("\\s+")[0]);
    JavaRDD<String> hosts2 = inputFiltered2.map((String line) -> line.split("\\s+")[0]);

    JavaRDD<String> intersection = hosts1.intersection(hosts2);
    intersection.saveAsTextFile("out/nasa_logs_same_hosts.csv");
  }

  private static Boolean isNotHeader(String line) {
    return !line.startsWith("host") && !line.endsWith("bytes");
  }
}
