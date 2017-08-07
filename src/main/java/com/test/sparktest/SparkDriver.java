package com.test.sparktest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by yangpy on 2017/6/16.
 */
public class SparkDriver {
    private final SparkConf mSparkConf;
    private final JavaSparkContext mSparkContext;
    private final ConcurrentLinkedQueue<String> mJobs;
    private boolean mIsDestroyed = false;


    public SparkDriver() {
        mJobs = new ConcurrentLinkedQueue<>();
        mSparkConf = new SparkConf()
                .setAppName("Java Spark JSON Example");
//                .setMaster("local[4]");
        System.out.println(mSparkConf.get("spark.master"));
        mSparkContext = new JavaSparkContext(mSparkConf);
    }

    public void newPeopleFlowTask(String path) {
        mJobs.add(path);
    }

    // Spark driver thread
    private Runnable mWorker = new Runnable() {
        @Override
        public void run() {
            String path;
            while (!mIsDestroyed) {
                path = mJobs.poll();
                if (null == path) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                    }
                } else {
                    // 创建Spark job
                    SparkJob job = new TestSparkJob(path);
                    job.execute(mSparkContext);
                }
            }
        }
    };
}
