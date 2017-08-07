package com.test.sparktest;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Created by yangpy on 2017/6/19.
 */
public class SparkJob implements Serializable {
    protected static final String INVALID_KEY = "INVALID_KEY";

    protected SparkJob() {
    }

    public void execute(JavaSparkContext sparkContext) {
    }
}
