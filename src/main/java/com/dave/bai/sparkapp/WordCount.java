package com.dave.bai.sparkapp;

import scala.Tuple2;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoRegistrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dave.bai.sparkapp.stages.NovelTextSplitter;
import com.dave.bai.sparkapp.stages.WordCollector;
import com.dave.bai.sparkapp.stages.WordCounterUpper;
import com.esotericsoftware.kryo.Kryo;

import static org.junit.Assert.assertEquals;

public class WordCount {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
            .setAppName("Word.Count.App")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryo.registrator", WordCount.WCRegistrator.class.getName())
            .setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> hemingwayInput = sc.textFile("src/main/resources/wordcount/theOldManAndTheSea.txt");

        // split by whitespace into words
        long wordsCount = hemingwayInput.flatMap(new NovelTextSplitter()).cache().count();
        LOGGER.debug("words count: {}", wordsCount);

        JavaPairRDD<String, Integer> collectEveryWord = hemingwayInput
            .flatMap(new NovelTextSplitter())
            .mapToPair(new WordCollector()).cache();

        LOGGER.info("collectEveryWord count: {}", collectEveryWord.count());
        LOGGER.info("collectEveryWord UNIQUE count: {}", collectEveryWord.distinct().count());

        JavaPairRDD<String, Integer> finalWordCount = collectEveryWord.reduceByKey(new WordCounterUpper()).cache();

        assertEquals(finalWordCount.count(), collectEveryWord.distinct().count());

        LOGGER.info("Total partitions size before coalesce: {}", finalWordCount.partitions().size());

        List<Tuple2<String, Integer>> collectFinalWordCount = finalWordCount.collect();

        for (Tuple2<String, Integer> wcRecord : collectFinalWordCount) {
            LOGGER.info("word: {}, count: {}", wcRecord._1(), wcRecord._2());
        }

        sc.stop();
        sc.close();
    }

    public static class WCRegistrator implements KryoRegistrator {

        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(String.class);
            kryo.register(Object[].class);
        }
    }
}
