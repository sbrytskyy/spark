package com.sparkTutorial.pairRdd.filter;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsNotInUsaProblem {

    public static void main(String[] args) {

        /* Create a Spark program to read the airport data from in/airports.text;
           generate a pair RDD with airport name being the key and country name being the value.
           Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located,
           IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "Canada")
           ("Wewak Intl", "Papua New Guinea")
           ...
         */

        SparkConf sparkConf = new SparkConf().setAppName("Filter Test").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> input = sc.textFile("in/airports.text");
        JavaRDD<String[]> arrays = input.map((String line) -> line.split(Utils.COMMA_DELIMITER));
        JavaRDD<Tuple2<String, String>> tuples = arrays.map((String[] arr) -> new Tuple2<>(arr[1], arr[3]));
        JavaRDD<Tuple2<String, String>> notUSA = tuples
            .filter(t -> !t._2().equals("\"United States\""));
        notUSA.saveAsTextFile("out/airports_not_in_usa_pair_rdd.text");
    }
}
