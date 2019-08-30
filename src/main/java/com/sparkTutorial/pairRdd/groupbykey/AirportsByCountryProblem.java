package com.sparkTutorial.pairRdd.groupbykey;

// import java.util.Arrays;
// import java.util.List;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsByCountryProblem {

  public static void main(String[] args) {

        /* Create a Spark program to read the airport data from in/airports.text,
           output the the list of the names of the airports located in each country.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           "Canada", ["Bagotville", "Montreal", "Coronation", ...]
           "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
           "Papua New Guinea",  ["Goroka", "Madang", ...]
           ...
         */

    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger logger = Logger.getLogger(AirportsByCountryProblem.class);

    SparkConf conf = new SparkConf().setAppName("AirportsByCountryProblem").setMaster("local[2]");
    JavaSparkContext ctx = new JavaSparkContext(conf);

    JavaRDD<String> inputRDD = ctx.textFile("in/airports.text");

    JavaRDD<String[]> airportInfoAsArrayRDD = inputRDD.map(line -> line.split(","));

    // List<String[]> airportsData = airportInfoAsArrayRDD.collect();
    // for (String[] airport : airportsData) {
    //   logger.info(Arrays.toString(airport));
    // }

    JavaPairRDD<String, String> airportCountryNamePairsRDD = airportInfoAsArrayRDD
        .mapToPair(arr -> new Tuple2<>(arr[3], arr[1]));

    JavaPairRDD<String, Iterable<String>> airportsByCountryRDD = airportCountryNamePairsRDD
        .groupByKey();

    List<Tuple2<String, Iterable<String>>> collect = airportsByCountryRDD.sortByKey().collect();
    for (Tuple2<String, Iterable<String>> entry : collect) {
      logger.info(entry._1() + ":" + entry._2());
    }
  }
}
