package com.jd.model.udf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class MapCacheManager {
    // 恶汉模式获取类实例；线程安全
    private volatile static MapCacheManager ourInstance = new MapCacheManager();
    // 缓存map
    private volatile static Map<String, Map<String, String>> mapCache = new ConcurrentHashMap<>();
    // 私有构造方法
    private MapCacheManager() {}
    // 获取实例
    public static MapCacheManager getInstance() {
        return ourInstance;
    }
    // hdfs URI
    private static final String URI = "hdfs://localhost:9000";
    // hdfs path
    private static final String PATH = "/input/t1.txt";

    /**
     * 设置cacheMap
     */
    void setMapCache() {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI(URI), conf);
            Path file = new Path(PATH);
            FSDataInputStream getIt = fs.open(file);
            BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
            String s = "";
            int rowNumber = 0;
            Map<String, String> headMap = new HashMap<String, String>();
            Map<String, String> lineMap = new HashMap<String, String>();
            while ((s = d.readLine()) != null) {
                String[] headRow = s.split("\t");
                if (rowNumber == 0) {
                    for (int i = 0; i< headRow.length; i++) {
                        headMap.put(String.valueOf(i), headRow[i]);
                    }
                }
                if (rowNumber != 0) {
                    for (int i = 0; i< headRow.length; i++) {
                        headMap.put(headMap.get(String.valueOf(i)), headRow[i]);
                    }
                    Map<String, String> flagMap = new HashMap<String, String>();
                    if (lineMap.get("keyword_flag") != null) {
                        flagMap.put("keyword_flag", lineMap.get("keyword_flag"));
                    }
                    if (lineMap.get("shop_flag") != null) {
                        flagMap.put("shop_flag", lineMap.get("shop_flag"));
                    }
                    if (lineMap.get("order_flag") != null) {
                        flagMap.put("order_flag", lineMap.get("order_flag"));
                    }
                    if (lineMap.get("sku_flag") != null) {
                        flagMap.put("sku_flag", lineMap.get("sku_flag"));
                    }
                    if (lineMap.get("keyword_parse_type") != null) {
                        flagMap.put("keyword_parse_type", lineMap.get("keyword_parse_type"));
                    }
                    if (lineMap.get("event_id") != null && !"".equals(lineMap.get("event_id"))) {
                        mapCache.put(lineMap.get("event_id"), flagMap);
                    }
                }
                rowNumber++;
            }
            d.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * 获取cacheMap
     */
    Map<String, Map<String, String>> getMapCache() {
        return mapCache;
    }

    /**
     * 清空cacheMap容器里面的内容
     */
    void clearMapCache(){
        mapCache.clear();
    }

    /**
     * 更新数据
     */
    public void updateMapCache() {
        ourInstance.clearMapCache();
        ourInstance.setMapCache();
    }
}
