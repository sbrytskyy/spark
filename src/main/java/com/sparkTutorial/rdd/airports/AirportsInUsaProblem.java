package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsInUsaProblem {

  public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
           and output the airport's name and the city's name to out/airports_in_usa.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "Putnam County Airport", "Greencastle"
           "Dowagiac Municipal Airport", "Dowagiac"
           ...
         */

    // Logger.getLogger("org").setLevel(Level.ERROR);
    SparkConf conf = new SparkConf().setAppName("US airports").setMaster("local[3]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> rdd = sc.textFile("in/airports.text");
    JavaRDD<String[]> split = rdd.map((String s) -> s.split(Utils.COMMA_DELIMITER));
    JavaRDD<String[]> usAirpotsWithCode = split
        .filter((String[] s) -> (s[4].length() > 2 && s[3].equals("\"United States\"")));
    // .filter((String[] s) -> s[2].equals("\"San Jose\"") || s[2].equals("\"San Francisco\""));

    JavaRDD<String> cityAirport = usAirpotsWithCode
        .map((String[] s) -> String.join(",", s[2], s[4], s[2]));

    // List<String> output = cityAirport.collect();
    // for (String airport : output) {
    //   System.out.println(airport);
    // }

    cityAirport.saveAsTextFile("out/airports_in_usa.text");
  }
}
