package com.test.sparktest.highway.dao.impl;

import com.test.sparktest.highway.dao.HighwayCellDao;
import com.test.sparktest.highway.pojo.Cell;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by yangpy on 2017/7/19.
 */
@Repository
@Primary
public class HighwayCellDao4MySQL implements HighwayCellDao {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public List<Cell> getAllCells() {
        return jdbcTemplate.query("SELECT CELL_ID, CGI, DISTANCE, SECTION_ID FROM d_hx_highway_cell", mapper);
    }

    private static RowMapper<Cell> mapper = new RowMapper<Cell>() {
        @Override
        public Cell mapRow(ResultSet resultSet, int i) throws SQLException {
            int cellID = resultSet.getInt("CELL_ID");
            String cgi = resultSet.getString("CGI");
            double distance = resultSet.getDouble("DISTANCE");
            String sectionID = resultSet.getString("SECTION_ID");
            return new Cell(cellID, cgi, distance, sectionID);
        }
    };
}
