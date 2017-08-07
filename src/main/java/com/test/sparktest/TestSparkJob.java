package com.test.sparktest;

import com.richstonedt.nokia_api.peopleflow.ReportByCGI;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Created by yangpy on 2017/6/19.
 */
public class TestSparkJob extends SparkJob {
    private String path;
    public String temp = "default value";

    public TestSparkJob(String path) {
        this.path = path;
    }

    @Override
    public void execute(JavaSparkContext sparkContext) {
        JavaRDD<String> records = sparkContext.textFile(path);

        JavaPairRDD<String, ReportByCGI> reportRDD = records.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains(",");
            }
        }).mapToPair(new PairFunction<String, String, ReportByCGI>() {
            @Override
            public Tuple2<String, ReportByCGI> call(String s) throws Exception {
                String[] p = s.split(",");
                String phone = p[1];
                String type = p[2];
                String date = p[9];
                String cgi = p[10];
                return new Tuple2(cgi, new ReportByCGI(cgi, phone, 0, phone.substring(0, 4), phone.charAt(1)=='5'));
            }
        }).reduceByKey(new Function2<ReportByCGI, ReportByCGI, ReportByCGI>() {
            @Override
            public ReportByCGI call(ReportByCGI reportByCGI, ReportByCGI reportByCGI2) throws Exception {
                for (ReportByCGI.FromDistrict dist2 : reportByCGI2.from.values()) {
                    ReportByCGI.FromDistrict dist1 = reportByCGI.from.get(dist2.getKey());
                    if (dist1 != null) {
                        dist1.count += dist2.count;
                        dist1.countOfNonresident += dist2.countOfNonresident;
                    } else
                        reportByCGI.from.put(dist2.getKey(), dist2);
                }
                reportByCGI.phoneList.addAll(reportByCGI2.phoneList);
                return reportByCGI;
            }
        });
        for (Tuple2<String, ReportByCGI> report : reportRDD.collect()) {
            ReportByCGI reportByCGI = report._2;
            System.out.println(String.format("CGI: %s", reportByCGI.cgi));
            for (ReportByCGI.FromDistrict district : reportByCGI.from.values()) {
                System.out.println(String.format("%s: %d, %d", district.name, district.count, district.countOfNonresident));
            }
            for (String misidn : reportByCGI.phoneList)
                System.out.println(misidn);
            System.out.println(temp);
            System.out.println("_____________________________________________");
        }

//        reportRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, ReportByCGI>>>() {
//            @Override
//            public void call(Iterator<Tuple2<String, ReportByCGI>> tuple2Iterator) throws Exception {
//                while (tuple2Iterator.hasNext()) {
//                    Tuple2<String, ReportByCGI> report = tuple2Iterator.next();
//                    ReportByCGI reportByCGI = report._2;
//                    System.out.println(String.format("CGI: %s", reportByCGI.cgi));
//                    for (ReportByCGI.FromDistrict district : reportByCGI.from.values()) {
//                        System.out.println(String.format("%s: %d, %d", district.name, district.count, district.countOfNonresident));
//                    }
//                    for (String misidn : reportByCGI.phoneList)
//                        System.out.println(misidn);
//                    System.out.println(temp);
//                    System.out.println("_____________________________________________");
//                }
//            }
//        });
//        reportRDD.foreach(new VoidFunction<Tuple2<String, ReportByCGI>>() {
//            @Override
//            public void call(Tuple2<String, ReportByCGI> reportPair) throws Exception {
//                ReportByCGI reportByCGI = reportPair._2;
//                System.out.println(String.format("CGI: %s", reportByCGI.cgi));
//                for (ReportByCGI.FromDistrict district : reportByCGI.from.values()) {
//                    System.out.println(String.format("%s: %d, %d", district.name, district.count, district.countOfNonresident));
//                }
//                for (String misidn : reportByCGI.phoneList)
//                    System.out.println(misidn);
//                System.out.println(temp);
//                System.out.println("_____________________________________________");
//            }
//        });
//        super.execute(sparkContext);
    }
}
