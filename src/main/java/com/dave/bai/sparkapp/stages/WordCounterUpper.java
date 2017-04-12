package com.dave.bai.sparkapp.stages;

public class WordCounterUpper implements org.apache.spark.api.java.function.Function2<Integer, Integer, Integer> {

    @Override
    public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
    }
}
