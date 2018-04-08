package com.jd.model.udf;

import com.google.gson.JsonObject;
import com.jd.model.util.CommonUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.google.gson.JsonParser;

import java.util.Map;

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
/**
 * 点击模型参数解析
 */
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
        // 若标记数等于0，说明第一次调用evaluate，更新Map
        if (count == 0) {
            updateMapCache();
            count++;
        }
        if (MapCacheManager.getInstance().getMapCache().get(eventId) != null) {
            Map<String, String> flagMap = MapCacheManager.getInstance().getMapCache().get(eventId);
            if(flagMap.get(flag) != null) {
                // 获取提取规则
                String regex = flagMap.get(flag);
                if(flagMap.get("keyword_parse_type") != null && !"".equals(flagMap.get("keyword_parse_type"))){
                    // 获取解析方式:json、url、_
                    String keyword_parse_type = flagMap.get("keyword_parse_type");
                    switch (keyword_parse_type) {
                        case "json":
                            if (CommonUtil.isJson(eventParam)) return getResultFromJson(eventParam, regex);
                            break;
                        case "url":
                            return getResultFromUrl(eventParam, regex);
                        case "_":
                            return getResultFromStringWith_(eventParam, regex);
                        default:
                            return null;
                    }
                }
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
            Map<String, String> flagMap = MapCacheManager.getInstance().getMapCache().get(tar_url);
            String host = CommonUtil.getHost(tar_url);
            if (!"".equals(host)) {
                if (flagMap.get(flag) != null) {
                    String regex = flagMap.get(flag);
                    return getResultFromUrl(tar_url, regex);
                }
            }
        }
        return null;
    }

    /**
     * 更新cacheMap
     */
    public void updateMapCache(){
        // 每次用户调用UDF时
        MapCacheManager.getInstance().updateMapCache();
    }

    /**
     * 通过解析url的方式获取要提取的字符串
     * @param url 要解析的url
     * @param regex 截取url的规则
     * @return 截取后的结果
     */
    public String getResultFromUrl(String url, String regex) {
        String[] regexArr = regex.split("##");
        if (regexArr.length == 1) {
            if (!regexArr[0].contains("&")) {
                return StringUtils.substringAfter(url, regexArr[0]);
            }else {
                return StringUtils.substringBefore(StringUtils.substringAfter(url, regexArr[0]),"&");
            }
        } else if (regexArr.length == 2){
            return StringUtils.substringBefore(StringUtils.substringAfter(url, regexArr[0]), regexArr[1]);
        }
        return null;
    }

    /**
     * 从Json获取结果
     * @param json 要解析的json字符串
     * @param key 获取结果对应的key
     * @return value
     */
    public String getResultFromJson(String json, String key) {
        JsonObject jsonObject = new JsonParser().parse(json).getAsJsonObject();
        if(jsonObject.get(key) != null) {
            return jsonObject.get(key).getAsString();
        }
        return null;
    }

    /**
     * 从字符串中获取要解析的字符串
     * @param param 要解析的字符串
     * @param position 位置
     * @return 位置对应的字符串
     */
    public String getResultFromStringWith_(String param, String position) {
        String[] array = param.split("_");
        if(Integer.valueOf(position) < array.length) {
            return array[Integer.valueOf(position)];
        }
        return null;
    }
}