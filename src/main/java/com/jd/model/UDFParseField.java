package com.jd.model;

import com.google.gson.JsonObject;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

@Description(
        name = "parseField",
        value = "app、m：_FUNC_(event_id, event_param, flag) - from the input event_id, event_param field and flag String, " +
                "returns the result\n" +
                "pc、wq：_FUNC_(tar_url, flag) - from the input tar_url field and flag String, " +
                "returns the result\n",
        extended = "Example:\n" +
                " > select _FUNC_(event_id, event_param, flag) from src;\n" +
                " > select _FUNC_(tar_url, flag) from src;"
)
public class UDFParseField extends UDF { // 继承UDF
    private final static Log logger = LogFactory.getLog(UDFParseField.class);
    // 标记数
    private static int count = 0;

    /**
     *  重写UDFevaluate方法：app、m平台解析字段
     * @param eventId event_id字段
     * @param eventParam event_param字段
     * @param flag keyword_flag shop_flag order_flag sku_flag
     * @return 解析出的结果
     */
    public String evaluate(String eventId, String eventParam, String flag) {
        if (count == 0) {
            updateMapCache();
            count++;
        }
        if (MapCacheManager.getInstance().getMapCache().get(eventId) != null) {
            Map<String, String> tagMap = MapCacheManager.getInstance().getMapCache().get(eventId);
            if(tagMap.get(flag) != null) {
                String positionOrKey = tagMap.get(flag);
                return isJson(eventParam) ? getSkuFromJson(eventParam, positionOrKey) : getSkuFromString(eventParam, positionOrKey);
            }
        }
        return null;
    }

    /**
     *  重写UDFevaluate方法：pc、wq平台解析字段
     * @param tar_url tar_url字段
     * @param flag keyword_flag shop_flag order_flag sku_flag
     * @return 解析出的结果
     */
    public String evaluate(String tar_url, String flag) {
        if (count == 0) {
            updateMapCache();
            count++;
        }
        if (MapCacheManager.getInstance().getMapCache().get(tar_url) != null) {
            Map<String, String> tagMap = MapCacheManager.getInstance().getMapCache().get(tar_url);
            String host = getHost(tar_url);
            if (!"".equals(host)) {
                if (tagMap.get(flag) != null) {
                    String regex = tagMap.get(flag);
                    String[] regexArr = regex.split("###");
                    if (regexArr.length == 1) {
                        if (!regexArr[0].contains("&")) {
                            return StringUtils.substringAfter(tar_url, regexArr[0]);
                        }else {
                            return StringUtils.substringBefore(StringUtils.substringAfter(tar_url, regexArr[0]),"&");
                        }
                    } else if (regexArr.length == 2){
                        return StringUtils.substringBefore(StringUtils.substringAfter(tar_url, regexArr[0]), regexArr[1]);
                    }
                }
            }
        }
        return null;
    }

    public void updateMapCache(){
        // 每次用户调用UDF时，同步Excel的数据到内存
        MapCacheManager.getInstance().updateMapCache();
    }

    // 判断eventParam是否是json
    public boolean isJson(String eventParam) {
        if (StringUtils.isBlank(eventParam)) {
            return false;
        }
        try {
            return new JsonParser().parse(eventParam).isJsonObject();
        } catch (JsonParseException e) {
            logger.error("bad json: " + eventParam);
            return false;
        }
    }

    // 从Json解析
    public String getSkuFromJson(String eventParam, String key) {
        JsonObject json = new JsonParser().parse(eventParam).getAsJsonObject();
        if(json.get(key) != null) {
            return json.get(key).getAsString();
        }
        return null;
    }

    // 从String解析
    public String getSkuFromString(String eventParam, String position) {
        String[] array = eventParam.split("_");
        if(Integer.valueOf(position) < array.length) {
            return array[Integer.valueOf(position)];
        }
        return null;
    }

    // 解析host
     public static String getHost(String url){
        if(url==null||url.trim().equals("")){
            return "";
        }
        String host = "";
        Pattern p =  Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+");
        Matcher matcher = p.matcher(url);
        if(matcher.find()){
            host = matcher.group();
        }
        return host;
    }
}