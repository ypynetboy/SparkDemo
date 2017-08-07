package com.test.sparktest.highway;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yangpy on 2017/7/14.
 */
public class SectionOfHighwayReport {
    private String sectionID;
    private List<Double> velocitys;

    public SectionOfHighwayReport(String sectionID, double velocity) {
        this.sectionID = sectionID;
        velocitys = new ArrayList<>();
        velocitys.add(velocity);
    }

    public String getSectionID() {
        return sectionID;
    }

    public List<Double> getVelocitys() {
        return velocitys;
    }

    public void addVelocity(double velocity) {
        velocitys.add(velocity);
    }
}
