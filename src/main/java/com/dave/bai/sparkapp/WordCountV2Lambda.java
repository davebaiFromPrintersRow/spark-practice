package com.dave.bai.sparkapp;

import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountV2Lambda {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
            .setAppName("Word.Count.App")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryo.registrator", WordCount.WCRegistrator.class.getName())
            .setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> hemingwayInput = sc.textFile("src/main/resources/wordcount/theOldManAndTheSea.txt");

        JavaPairRDD<String, Integer> wcMap = hemingwayInput.flatMap(words -> Arrays.asList(words.split(" ")).iterator())
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey((x, y) -> x + y).cache();

        List<Tuple2<String, Integer>> collectWc = wcMap.collect();
        for (Tuple2<String, Integer> wcRecord : collectWc) {
            LOGGER.info("word: {}, count: {}", wcRecord._1(), wcRecord._2());
        }

        sc.stop();
        sc.close();
    }

}
