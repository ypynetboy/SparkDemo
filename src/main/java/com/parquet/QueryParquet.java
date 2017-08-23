package com.parquet;

import com.richstonedt.nokia_api.peopleflow.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
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
        SparkConf sparkConf = new SparkConf().setAppName("Highway20MinStatistics").setMaster("local[12]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(javaSparkContext);

        ArrayList<CGI> cgis = new ArrayList<>();
//        cgis.add(new CGI("172540-3"));
        cgis.add(new CGI("681071-1"));
        sqlContext.createDataFrame(javaSparkContext.parallelize(cgis), CGI.class)
                .registerTempTable("highway_cgis");

        sqlContext.read().parquet("hdfs://192.168.6.25:9000/highway/demodata/parquet/data.parquet")
                .registerTempTable("record");
//        sqlContext.read().parquet("d:/tmp/abc.parquet").registerTempTable("record");
        clientShow(sqlContext);
        //clusterShow(spark);
    }

    private static void clientShow(SQLContext sqlContext) {
//        List<Row> list = spark.sql("select * from highway_cgis")
//                .collectAsList();
        List<Row> list = sqlContext.sql("select * from record r, highway_cgis where r.cgi=highway_cgis.cgi")
                .collectAsList();
        for (Row row : list) {
            System.out.println(String.format("%s, %d", row.getString(row.fieldIndex("phone")), row.getLong(row.fieldIndex("lastTime"))));
//            System.out.println(String.format("%s", row.getString(0)));
        }
    }

    private static void clusterShow(SQLContext sqlContext) {
        sqlContext.sql("select * from record where lastTime > 1000")
                .foreachPartition(new AbstractFunction1<scala.collection.Iterator<Row>, BoxedUnit>() {
                    @Override
                    public BoxedUnit apply(scala.collection.Iterator<Row> iterator) {
                        Configuration conf = new Configuration();
                        try {
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
                        } catch (IOException e) {
                        }
                        return null;
                    }
                });
    }

    public static class CGI implements Serializable {
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
