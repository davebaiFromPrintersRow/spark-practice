package com.dave.bai.sparkapp.stages;

import scala.Tuple2;

import org.apache.spark.api.java.function.PairFunction;

public class WordCollector implements PairFunction<String, String, Integer> {

    @Override
    public Tuple2<String, Integer> call(String word) throws Exception {
        return new Tuple2<>(word, 1);
    }
}
