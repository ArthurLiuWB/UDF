package com.jd.model.util;

import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 通用工具类
 */
public class CommonUtil {

    private final static Log logger = LogFactory.getLog(CommonUtil.class);

    /**
     * 通过正则获取url里面的host
     * @param url 参数
     * @return 解析出来的host
     */
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

    /**
     * 判断参数是否是json
     * @param param 要判断的参数
     * @return boolean
     */
    public static boolean isJson(String param) {
        if (StringUtils.isBlank(param)) {
            return false;
        }
        try {
            return new JsonParser().parse(param).isJsonObject();
        } catch (JsonParseException e) {
            logger.error("bad json: " + param);
            return false;
        }
    }
}
