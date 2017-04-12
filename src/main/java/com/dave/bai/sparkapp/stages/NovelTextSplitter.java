package com.dave.bai.sparkapp.stages;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

public class NovelTextSplitter implements FlatMapFunction<String, String> {

    @Override
    public Iterator<String> call(String bookRawText) throws Exception {
        return Arrays.asList(bookRawText.split(" ")).iterator();
    }
}
