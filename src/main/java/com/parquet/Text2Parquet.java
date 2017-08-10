package com.parquet;

import com.richstonedt.nokia_api.peopleflow.Record;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by yangpy on 2017/8/7.
 */
public class Text2Parquet {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                //.config("spark.some.config.option", "some-value")
//                .master("local[4]")
                .getOrCreate();
        Encoder<Record> recordEncoder = Encoders.bean(Record.class);

        spark.read().textFile("/highway/demodata/parquet/data.json")
//        spark.read().textFile("D:\\data.json")
                .mapPartitions(new MapPartitionsFunction<String, Record>() {
                    @Override
                    public Iterator<Record> call(Iterator<String> iterator) throws Exception {
                        ArrayList<Record> result = new ArrayList<>();
                        while (iterator.hasNext()) {
                            try {
                                result.add(new Record(iterator.next()));
                            } catch (ParseException|ArrayIndexOutOfBoundsException e) {
                            }
                        }
                        return result.iterator();
                    }
                }, recordEncoder)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("/highway/demodata/parquet/abc.parquet");
    }
}
