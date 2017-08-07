package com.test.sparktest.highway.pojo;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by yangpy on 2017/7/18.
 */
public class Cell {
    private int cellID; // Cell ID
    private String cgi; // CGI
    private double distance; // 与上一个基站的距离
    private String sectionID; // 所属路段ID

    public Cell(int cellID, String cgi, double distance, String sectionID) {
        this.cellID = cellID;
        this.cgi = cgi;
        this.distance = distance;
        this.sectionID = sectionID;
    }

    public int getCellID() {
        return cellID;
    }

    public String getCgi() {
        return cgi;
    }

    public double getDistance() {
        return distance;
    }

    public String getSectionID() {
        return sectionID;
    }
}
