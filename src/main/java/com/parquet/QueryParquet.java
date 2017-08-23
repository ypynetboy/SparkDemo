package com.parquet;

import com.richstonedt.nokia_api.peopleflow.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * Created by yangpy on 2017/8/9.
 */
public class QueryParquet {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[4]")
                .getOrCreate();

        ArrayList<CGI> cigs = new ArrayList<>();
//        cigs.add(new CGI("172540-3"));
        cigs.add(new CGI("681071-1"));
        Dataset<CGI> cgiDataset = spark.createDataset(cigs, Encoders.bean(CGI.class));
        cgiDataset.createOrReplaceTempView("highway_cgis");

//        spark.read().parquet("hdfs://192.168.6.25:9000/highway/demodata/parquet/abc.parquet")
        spark.read().parquet("d:/tmp/abc.parquet")
                .createOrReplaceTempView("record");
        clientShow(spark);
        //clusterShow(spark);
    }

    private static void clientShow(SparkSession spark) {
//        List<Row> list = spark.sql("select * from highway_cgis")
//                .collectAsList();
        List<Row> list = spark.sql("select * from record r, highway_cgis where r.cgi=highway_cgis.cgi")
                .collectAsList();
        for (Row row : list) {
            System.out.println(String.format("%s, %d", row.getString(2), row.getLong(1)));
//            System.out.println(String.format("%s", row.getString(0)));
        }
    }

    private static void clusterShow(SparkSession spark) {
        spark.sql("select * from record where lastTime > 1000")
                .foreachPartition(new ForeachPartitionFunction<Row>() {
                    @Override
                    public void call(Iterator<Row> iterator) throws Exception {
                        Configuration conf = new Configuration();
                        FileSystem fs = FileSystem.get(conf);
                        Path logFile = new Path("/highway/demodata/parquet/", UUID.randomUUID().toString());
                        FSDataOutputStream osLog = fs.create(logFile);
                        try {
                            while (iterator.hasNext()) {
                                Row row = iterator.next();
                                osLog.write(String.format("%s, %d\n", row.getString(2), row.getLong(1)).getBytes());
                            }
                        } finally {
                            osLog.close();
                        }
                    }
                });
    }

    public static class CGI {
        private String cgi;

        public CGI(String cgi) {
            this.cgi = cgi;
        }

        public void setCgi(String cgi) {
            this.cgi = cgi;
        }

        public String getCgi() {
            return cgi;
        }
    }
}
