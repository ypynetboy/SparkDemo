package com.test.sparktest.highway.pojo;

/**
 * Created by yangpy on 2017/7/17.
 */
public class HighwaySection {
    private String id;
    private String name;

    public HighwaySection(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
