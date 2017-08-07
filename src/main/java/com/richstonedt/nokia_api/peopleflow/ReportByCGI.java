package com.richstonedt.nokia_api.peopleflow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yangpy on 2017/6/18.
 */
public class ReportByCGI implements Serializable {
    public final String cgi;
    public final ConcurrentHashMap<String, FromDistrict> from;
    public final ArrayList<String> phoneList;

    public ReportByCGI(String cgi, String phone, int type, String name, boolean isResident) {
        this.cgi = cgi;
        this.from = new ConcurrentHashMap<>();
        FromDistrict district = new FromDistrict(type, name, 1, isResident?0:1);
        this.from.put(district.getKey(), district);
        // 用户详情
        phoneList = new ArrayList<>();
        phoneList.add(phone);
    }

    // 来源地统计结果
    public static class FromDistrict implements Serializable {
        public int type; // 来源地属性(1:本地，2：省内，3：外省，4、国际)
        public String name; // 详细来源地
        public int count; // 用户总数
        public int countOfNonresident; // 非常住用户总数

        public FromDistrict(int type, String name, int count, int countOfNonresident) {
            this.type = type;
            this.name = name;
            this.count = count;
            this.countOfNonresident = countOfNonresident;
        }

        public String getKey() {
            return String.format("%d-%s", type, name);
        }
    }
}
