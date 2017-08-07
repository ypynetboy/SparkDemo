package com.richstonedt.nokia_api.peopleflow;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by richstone on 2017/6/9.
 */
public class Record implements Serializable {
    private String phone;
    private long lastTime;
    private String cgi;

    public Record() {
    }

    public Record(String data) throws ParseException, ArrayIndexOutOfBoundsException {
        String[] p = data.split(",");
        if (p == null) {
            throw new NullPointerException();
        }
        SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        phone = p[1];
        lastTime = DATE_FORMAT.parse(p[9]).getTime();
        cgi = p[10];
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getLastTime() {
        return lastTime;
    }

    public void setLastTime(long lastTime) {
        this.lastTime = lastTime;
    }

    public String getCgi() {
        return cgi;
    }

    public void setCgi(String cgi) {
        this.cgi = cgi;
    }
}
