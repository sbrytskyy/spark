package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UnionLogProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
           take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */

        SparkConf conf = new SparkConf().setAppName("Union Log").setMaster("local[1]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> input1 = context.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> input2 = context.textFile("in/nasa_19950801.tsv");

        JavaRDD<String> inputFiltered1 = input1.filter((String line) -> !line.startsWith("host"));
        JavaRDD<String> inputFiltered2 = input2.filter((String line) -> !line.startsWith("host"));

        JavaRDD<String> union = inputFiltered1.union(inputFiltered2);
        JavaRDD<String> sample = union.sample(true, 0.001);

        sample.saveAsTextFile("out/sample_nasa_logs.tsv");

    }
}
