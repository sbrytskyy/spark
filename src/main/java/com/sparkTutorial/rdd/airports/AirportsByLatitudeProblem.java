package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsByLatitudeProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
           Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "St Anthony", 51.391944
           "Tofino", 49.082222
           ...
         */

        SparkConf conf = new SparkConf().setAppName("Airports by Latitude").setMaster("local[2]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> input = context.textFile("in/airports.text");
        JavaRDD<String[]> inputSplit = input.map((String line) -> line.split(Utils.COMMA_DELIMITER));
        JavaRDD<String[]> filteredByLatitude = inputSplit
            .filter((String[] split) -> Double.compare(Double.valueOf(split[6]), 40.0) > 0);
        JavaRDD<String> output = filteredByLatitude
            .map((String[] split) -> String.join(",", split[2], split[6]));
        output.saveAsTextFile("out/airports_by_latitude.text");
    }
}
