package com.dave.bai.sparkpractice;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class AvgCount implements Serializable {

    public AvgCount(int total, int num) {
        total_ = total;
        num_ = num;
    }

    public int total_;
    public int num_;

    public float avg() {
        return total_ / (float) num_;
    }

    static Function<Integer, AvgCount>             createAcc         = new Function<Integer, AvgCount>() {

                                                                         public AvgCount call(Integer x) {
                                                                             return new AvgCount(x, 1);
                                                                         }
                                                                     };
    static Function2<AvgCount, Integer, AvgCount>  addAndCount       = new Function2<AvgCount, Integer, AvgCount>() {

                                                                         public AvgCount call(AvgCount a, Integer x) {
                                                                             a.total_ += x;
                                                                             a.num_ += 1;
                                                                             return a;
                                                                         }
                                                                     };
    static Function2<AvgCount, AvgCount, AvgCount> combine           = new Function2<AvgCount, AvgCount, AvgCount>() {

                                                                         public AvgCount call(AvgCount a, AvgCount b) {
                                                                             a.total_ += b.total_;
                                                                             a.num_ += b.num_;
                                                                             return a;
                                                                         }
                                                                     };

    static Function<Integer, AvgCount>             createAccLambda   = (Function<Integer, AvgCount>) x -> new AvgCount(x, 1);

    static Function2<AvgCount, Integer, AvgCount>  addAndCountLambda = (Function2<AvgCount, Integer, AvgCount>) (a, x) -> {
                                                                         a.total_ += x;
                                                                         a.num_ += 1;
                                                                         return a;
                                                                     };

    static Function2<AvgCount, AvgCount, AvgCount> combineLambda     = (Function2<AvgCount, AvgCount, AvgCount>) (a, b) -> {
                                                                         a.total_ += b.total_;
                                                                         a.num_ += b.num_;
                                                                         return a;
                                                                     };

    public static void main(String[] args) {
        List<Tuple2> inputData = Arrays.asList(
            new Tuple2(3, 2),
            new Tuple2(3, 1),
            new Tuple2(3, 1),
            new Tuple2(5, 8),
            new Tuple2(5, 1),
            new Tuple2(9, 2));
        SparkConf conf = new SparkConf().setAppName("test-spark").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD inputDataRdd = sc.parallelize(inputData);
        JavaPairRDD kvPairs = JavaPairRDD.fromJavaRDD(inputDataRdd);

        JavaPairRDD<Integer, AvgCount> avgCounts =
            kvPairs.combineByKey(createAccLambda, addAndCountLambda, combineLambda);
        Map<Integer, AvgCount> avgCountMap = avgCounts.collectAsMap();

        for (Map.Entry<Integer, AvgCount> entry : avgCountMap.entrySet()) {
            System.out.println(entry.getKey() + "---> " + entry.getValue().avg());
        }

        System.out.println("partitions size: " + kvPairs.partitions().size());

        Map<Integer, Integer> mapGroupingKeys = kvPairs.groupByKey().collectAsMap();
        for (Map.Entry<Integer, Integer> entry : mapGroupingKeys.entrySet()) {
            System.out.println(entry.getKey() + "===> " + entry.getValue());
        }

        sc.close();
    }
}
