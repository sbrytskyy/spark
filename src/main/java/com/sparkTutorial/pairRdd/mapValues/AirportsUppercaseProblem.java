package com.sparkTutorial.pairRdd.mapValues;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsUppercaseProblem {

    public static void main(String[] args) {

        /* Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
           being the key and country name being the value. Then convert the country name to uppercase and
           output the pair RDD to out/airports_uppercase.text

           Each row of the input file contains the following columns:

           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "CANADA")
           ("Wewak Intl", "PAPUA NEW GUINEA")
           ...
         */

        SparkConf sparkConf = new SparkConf().setAppName("AirportsUppercase").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> input = sc.textFile("in/airports.text");
        JavaRDD<String[]> arrays = input.map((String line) -> line.split(Utils.COMMA_DELIMITER));

        JavaPairRDD<String, String> mapKeyCountry = arrays
            .mapToPair((String[] arr) -> new Tuple2<>(arr[1], arr[3]));

        JavaPairRDD<String, String> upperCaseCountries = mapKeyCountry
            .mapValues(String::toUpperCase);

        upperCaseCountries.saveAsTextFile("out/airports_uppercase.text");
    }
}
