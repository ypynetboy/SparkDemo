package com.parquet;

import com.richstonedt.nokia_api.peopleflow.Record;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by yangpy on 2017/8/23.
 */
public class MultifileRDD {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Highway20MinStatistics").setMaster("local[12]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        System.out.println(sparkContext.textFile("d:/tmp/data*.json").mapPartitions(new FlatMapFunction<Iterator<String>, Record>() {
            @Override
            public Iterable<Record> call(Iterator<String> iterator) throws Exception {
                ArrayList<Record> result = new ArrayList<>();
                while (iterator.hasNext()) {
                    try {
                        result.add(new Record(iterator.next()));
                    } catch (ParseException | ArrayIndexOutOfBoundsException e) {
                    }
                }
                return result;
            }
        }).filter(new Function<Record, Boolean>() {
            @Override
            public Boolean call(Record record) throws Exception {
                return ("13538796630".equals(record.getPhone()));
            }
        }).count());
    }
}
