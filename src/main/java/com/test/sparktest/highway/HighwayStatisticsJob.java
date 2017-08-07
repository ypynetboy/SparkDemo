package com.test.sparktest.highway;

import com.richstonedt.nokia_api.peopleflow.Record;
import com.richstonedt.nokia_api.peopleflow.ReportByCGI;
import com.test.sparktest.SparkJob;
import com.test.sparktest.SpringContextInstance;
import com.test.sparktest.highway.pojo.Cell;
import com.test.sparktest.utils.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;

/**
 * Created by yangpy on 2017/7/13.
 */
public class HighwayStatisticsJob extends SparkJob {
    @Override
    public void execute(JavaSparkContext sparkContext) {
        JavaRDD<String> dataRDD = sparkContext.textFile("D:\\data.json");
        JavaPairRDD<String, List<Record>> pairRDD = dataRDD.mapToPair(new PairFunction<String, String, List<Record>>() {
            @Override
            public Tuple2<String, List<Record>> call(String s) throws Exception {
                try {
                    Record record = new Record(s);
                    // TODO 过滤常住用户
                    ArrayList<Record> list = new ArrayList<>(1);
                    list.add(record);
                    return new Tuple2(record.getPhone(), list);
                } catch (Exception e) {
                    return new Tuple2<>(INVALID_KEY, null);
                }
            }
        }).filter(new Function<Tuple2<String, List<Record>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, List<Record>> tuple2) throws Exception {
                return !(INVALID_KEY.equals(tuple2._1));
            }
        }).reduceByKey(new Function2<List<Record>, List<Record>, List<Record>>() {
            @Override
            public List<Record> call(List<Record> list, List<Record> list2) throws Exception {
                list.addAll(list2);
                return list;
            }
        });

        // 计算单个用户的速度
        pairRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String,List<Record>>>, String, TemporarySectionReport>() {
            @Override
            public Iterator<Tuple2<String, TemporarySectionReport>> call(Iterator<Tuple2<String, List<Record>>> tuple2Iterator) throws Exception {
                HighwayCellMap highwayCellMap = SpringContextInstance.getBean("highwayCellMap", HighwayCellMap.class);
                if (null == highwayCellMap)
                    throw new RuntimeException("Can't load \"HighwayCellMap\" bean.");

                ArrayList<Tuple2<String, TemporarySectionReport>> result = new ArrayList<>();
                while (tuple2Iterator.hasNext()) {
                    // 把CGI按所属路段分解，再计算相同路段CGI的距离和方向
                    List<Record> records = tuple2Iterator.next()._2;
                    for (List<Pair<Record, Cell>> pairs : highwayCellMap.getCellAndSectionByCGI(records).values()) {
                        String sectionID = pairs.get(0).getValue().getSectionID();
                        TemporarySectionReport report = makeReport(highwayCellMap, pairs);
                        if (report != null) {
                            result.add(new Tuple2<>(sectionID, report));
                        }
                    }
                }
                return result.iterator();
            }
        }).reduceByKey(new Function2<TemporarySectionReport, TemporarySectionReport, TemporarySectionReport>() {
            // 按路段归并速度
            @Override
            public TemporarySectionReport call(TemporarySectionReport sectionOfHighwayReport, TemporarySectionReport sectionOfHighwayReport2) throws Exception {
                sectionOfHighwayReport.velocitys.addAll(sectionOfHighwayReport2.velocitys);
                return sectionOfHighwayReport;
            }
        });
    }

    // 生成单个用户的路段报告
    private static TemporarySectionReport makeReport(HighwayCellMap highwayCellMap, List<Pair<Record, Cell>> pairs) {
        // 按时间降序排列
        Collections.sort(pairs, comparator);
//        pairs.sort(comparator);

        long startTime = Long.MAX_VALUE; // 开始时间
        long endTime = Long.MIN_VALUE; // 结束时间
        boolean bPositive; // 方向是否正向的
        Cell lastCell = null; // 上个Cell
        double distance = 0.0; // 运动距离
        for (Pair<Record, Cell> pair : pairs) {
            if (pair.getKey().getLastTime() < startTime)
                startTime = pair.getKey().getLastTime();
            if (pair.getKey().getLastTime() > endTime)
                endTime = pair.getKey().getLastTime();
            // 计算方向和距离
            if (null == lastCell) {
                // 判断方向
                bPositive = pair.getValue().getCellID() > lastCell.getCellID();
                // 计算距离
                distance += getDistance(highwayCellMap, lastCell, pair.getValue());
            }
        }
        // 计算速度(单位千米/小时)
        double time = (endTime - startTime) / 60 / 60 / 1000; // 时间（小时）
        double speed = distance / time / 1000;
        // TODO
        return new TemporarySectionReport(speed);
    }

    // 计算两个Cell之间的距离
    private static double getDistance(HighwayCellMap highwayCellMap, Cell cell, Cell cell2) {
        Cell minCell = cell, maxCell = cell2;
        if (cell.getCellID() > cell2.getCellID()) {
            minCell = cell2;
            maxCell = cell;
        }
        // 如果是相邻Cell，直接计算距离
        if (maxCell.getCellID()-minCell.getCellID() == 1)
            return minCell.getDistance();
        // 遍历[minCell, maxCell)的数据，统计距离
        double distance = minCell.getDistance();
        for (int i=minCell.getCellID()+1; i<maxCell.getCellID(); i++) {
            Cell tmp = highwayCellMap.getCellById(i);
            if (tmp != null)
                distance += tmp.getDistance();
        }
        return distance;
    }

    // 排序比较器
    private static Comparator<Pair<Record, Cell>> comparator = new Comparator<Pair<Record, Cell>>() {
        @Override
        public int compare(Pair<Record, Cell> o1, Pair<Record, Cell> o2) {
            long t1 = o1.getKey().getLastTime();
            long t2 = o2.getKey().getLastTime();
            if (t1 == t2)
                return 0;
            return (t1 > t2) ? 1 : -1;
        }
    };

    protected static class TemporarySectionReport {
        protected List<Double> velocitys;

        public TemporarySectionReport(double velocity) {
            velocitys = new ArrayList<>();
            velocitys.add(velocity);
        }
    }
}
