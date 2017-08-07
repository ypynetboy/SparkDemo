package com.test.sparktest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Locale;

/**
 * Created by yangpy on 2017/6/18.
 */
public class SparkDemo {
    // 一天的毫秒数
    private static final long MILLISECOND_PER_DAY = 24 * 60 * 60 * 1000;
    // 北京时区
    private static final long BEIJING_TIMEZONE_MILLISECOND = 8 * 60 * 60 * 1000;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[8]");

        System.out.println("Master: " + conf.get("spark.master"));

        JavaSparkContext sc = new JavaSparkContext(conf);
        TestSparkJob job = new TestSparkJob("D:\\data.json");
        job.temp = "aaaaaaaa";
        job.execute(sc);
    }
}
