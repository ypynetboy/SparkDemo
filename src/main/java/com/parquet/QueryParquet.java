package com.parquet;

import com.richstonedt.nokia_api.peopleflow.Record;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by yangpy on 2017/8/9.
 */
public class QueryParquet {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
//                .master("local[4]")
                .getOrCreate();
        Encoder<Record> recordEncoder = Encoders.bean(Record.class);

        spark.read().parquet("hdfs:///highway/demodata/parquet/abc.parquet")
                .createOrReplaceTempView("record");
        spark.sql("select * from record where lastTime > 1000")
                .foreachPartition(new ForeachPartitionFunction<Row>() {
                    @Override
                    public void call(Iterator<Row> iterator) throws Exception {
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            System.out.println(String.format("%s, %d", row.getString(2), row.getString(1)));
                        }
                    }
                });
    }
}
