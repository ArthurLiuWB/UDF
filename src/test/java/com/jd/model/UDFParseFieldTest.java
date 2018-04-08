package com.jd.model;

import com.jd.model.UDFParseField;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class UDFParseFieldTest {
    @Test
    public void UDFTest() {
        UDFParseField udf = new UDFParseField();
        String json = "{\"sku\":\"asdfsdfasd\"}";
        //System.out.println("解析Json：" + udf.evaluate(json));
        String jsonNull = "{\"sku1\":\"asdfsdfasd\"}";
        //System.out.println("解析Json：" + udf.evaluate(jsonNull));
        String str = "sku123_3214_sdfsdaf";
        //System.out.println("解析Json：" + udf.evaluate(str));
        String strNull = "123_3214_sdfsdaf";
        //System.out.println("解析Json：" + udf.evaluate(strNull));
    }

    @Test
    public void RegextTest() {
        String tar_url = "yao.jd.com/item/10628683378.html.html";
        String host = "yao.jd.com";
        String regex1 = "yao.jd.com/item/###.html";
        String regex2 = "wqitem.jd.com/item/view?sku=";
        String[] regexArr1 = regex1.split("###");
        String[] regexArr2 = regex2.split("###");
        for(String str: regexArr1) {
            System.out.println("1:" + str + "\t");
        }
        for(String str: regexArr2) {
            System.out.println("2:" + str + "\t");
        }
        System.out.println(StringUtils.substringBefore(StringUtils.substringAfter(tar_url, regexArr1[0]), regexArr1[1]));
        //String tar_url_1 = "wqitem.jd.com/item/view?sku=5512096";
    }

    @Test
    public void hostTest() {
        String url1 = "item.jd.hk/1976213839.html";
        String url2 = "wqitem.jd.com/item/view?sku=5512096";
        String url3 = "http://item.jd.hk/1976213839.html";
        String url4 = "https://item.jd.hk/1976213839.html";
        System.out.println("解析结果：" + UDFParseField.getHost(url1));
        System.out.println("解析结果：" + UDFParseField.getHost(url2));
        System.out.println("解析结果：" + UDFParseField.getHost(url3));
        System.out.println("解析结果：" + UDFParseField.getHost(url4));

    }
}
