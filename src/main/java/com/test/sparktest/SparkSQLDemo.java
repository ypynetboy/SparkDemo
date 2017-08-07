package com.test.sparktest;

import com.richstonedt.nokia_api.peopleflow.Record;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

/**
 * Created by richstone on 2017/6/8.
 */
public class SparkSQLDemo {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Java Spark JSON Example")
                .setMaster("local[4]");
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        System.out.println("main");

        Encoder<Record> recordEncoder = Encoders.bean(Record.class);
        Dataset<String> dataset = spark.read().textFile("D:\\data.json");
//        Dataset<Row> dataset = spark.read().json("D:\\data.json");
//        dataset.printSchema();
        Dataset<Record> recordDataset = dataset.map(new MapFunction<String, Record>() {
            @Override
            public Record call(String s) throws Exception {
                return new Record(s);
            }
        }, recordEncoder);
        recordDataset.createOrReplaceTempView("records");
        // 话单数据集
//        Dataset<Record> recordDataset = dataset.flatMap(new FlatMapFunction<Row, Record>() {
//            public Iterator<Record> call(Row row) throws Exception {
//                List<Record> result = new ArrayList<Record>();
//                List<String> records = row.getList(row.fieldIndex("records"));
//                for (String r : records) {
//                    result.add(new Record(r));
//                }
//                System.out.println("Thread ID: " + Thread.currentThread().getId());
//                return result.iterator();
//            }
//        }, recordEncoder).persist(StorageLevel.MEMORY_AND_DISK());
//        recordDataset.createOrReplaceTempView("records");
//        recordDataset.printSchema();

        spark.sql("select cgi, count(cgi), phone from records group by cgi").foreach(new ForeachFunction<Row>() {
            public void call(Row row) throws Exception {
                System.out.println(String.format("%s: %d", row.getString(0), row.getLong(1)));
                System.out.println("Thread ID: " + Thread.currentThread().getId());
            }
        });

        System.out.println(dataset.count());
    }
}
