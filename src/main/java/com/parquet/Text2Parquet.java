package com.parquet;

import com.richstonedt.nokia_api.peopleflow.Record;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import java.io.IOException;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by yangpy on 2017/8/7.
 */
public class Text2Parquet {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Highway20MinStatistics").setMaster("local[12]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(javaSparkContext);

        JavaRDD<Record> recordJavaRDD = javaSparkContext.textFile("/highway/demodata/parquet/data.json").mapPartitions(new FlatMapFunction<Iterator<String>, Record>() {
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
        });
        sqlContext.createDataFrame(recordJavaRDD, Record.class)
                .write()
                .partitionBy("cgi")
                .mode(SaveMode.Append)
                .parquet("/highway/demodata/parquet/data.parquet");
    }
}
