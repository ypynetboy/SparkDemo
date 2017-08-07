package com.test.sparktest.highway;

import com.richstonedt.nokia_api.peopleflow.Record;
import com.test.sparktest.SpringContextInstance;
import com.test.sparktest.highway.dao.HighwayCellDao;
import com.test.sparktest.highway.pojo.Cell;
import com.test.sparktest.utils.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yangpy on 2017/7/14.
 */
@Component
public class HighwayCellMap {
    @Autowired
    HighwayCellDao highwayCellDao;
    ConcurrentHashMap<String, List<Cell>> cgi2CellMap; // CGI与Cell对应表
    ConcurrentHashMap<Integer, Cell> cellsMap; // Cell表

    public static void main(String[] args) {
        ApplicationContext context = SpringContextInstance.getInstance();
        HighwayCellMap obj = context.getBean("highwayCellMap", HighwayCellMap.class);
        obj.load();
    }

    public Cell getCellById(int cellId) {
        return cellsMap.get(cellId);
    }

    public List<Cell> getCellByCGI(String cgi) {
        return cgi2CellMap.get(cgi);
    }

    // 加载高速路段表数据
    public void load() {
        ConcurrentHashMap<String, List<Cell>> tmpCGI2CellMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, Cell> tmpCellsMap = new ConcurrentHashMap<>();
        for (Cell cell : highwayCellDao.getAllCells()) {
            // CGI与Cell对应表
            List<Cell> value = tmpCGI2CellMap.get(cell.getCgi());
            if (null == value) {
                value = new ArrayList<>();
                tmpCGI2CellMap.put(cell.getCgi(), value);
            }
            value.add(cell);
            // Cell表
            tmpCellsMap.put(cell.getCellID(), cell);
        }
        this.cgi2CellMap = tmpCGI2CellMap;
        this.cellsMap = tmpCellsMap;
    }

    // 获取CGI列表对应的路段Cell对象
    public Map<String, List<Pair<Record, Cell>>> getCellAndSectionByCGI(List<Record> records) {
        HashMap<String, List<Pair<Record, Cell>>> result = new HashMap<>();
        for (Record r : records) {
            // 获取CGI对应的Cell对象，可能会有多个Cell对象，如果有多个都做记录
            for (Cell cell : cgi2CellMap.get(r.getCgi())) {
                List<Pair<Record, Cell>> value = result.get(cell.getSectionID());
                if (value == null) {
                    value = new ArrayList<>();
                    result.put(cell.getSectionID(), value);
                }
                value.add(new Pair<>(r, cell));
            }
        }
        // 过滤少于2个Cell的路段
        for (String key : result.keySet()) {
            if (result.get(key).size() < 2)
                result.remove(key);
        }
        return result;
    }
}

