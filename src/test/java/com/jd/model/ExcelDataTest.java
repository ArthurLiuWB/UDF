package com.jd.model;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ExcelDataTest {
    @Test
    public void excelDataTest() {
        String filePath = "D:\\regulation.xls";
        File file = new File(filePath);
        try{
            Map<String, List<List<Map<String, String>>>> resultMap =  ExcelData.getExcelData(file);
            if (resultMap.get("1") != null) {
                List<List<Map<String, String>>> rowList = resultMap.get("1");
                for(List<Map<String, String>> row: rowList) {
                    if (row!=null) {
                        for (Map<String, String> cell: row) {
                            for(Map.Entry<String, String> entry: cell.entrySet()) {
                                System.out.print(entry.getKey()+ ":" + entry.getValue() + " ");
                            }
                        }
                        System.out.println();
                    }

                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
