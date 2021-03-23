package com.ray.esjd.utils;

import com.ray.esjd.domain.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

public class HtmlParseUtil {
/*
    public static void main(String[] args) throws Exception {

        new HtmlParseUtil().parseJD("java").forEach(System.out::println);
    }*/

    public List<Content> parseJD(String keyword) throws IOException {
        //获取请求 https://search.jd.com/Search?keyword=
        //前提需要联网， 而且无法取得ajax的数据
        String encode = URLEncoder.encode(keyword, "UTF-8");
        String url = "https://search.jd.com/Search?keyword=" + encode;
        //解析网页，返回内容就是 Document对象
        Document document = Jsoup.parse(new URL(url), 30000);

        Element element = document.getElementById("J_goodsList");
        //System.out.println(element.html());

        // 获取所有的 li元素
        Elements li = element.getElementsByTag("li");

        List<Content> goodsList = new ArrayList<>();

        //获取元素内容
        for (Element el : li) {
            String img = el.getElementsByTag("img").eq(0).attr("data-lazy-img");
            String price = el.getElementsByClass("p-price").eq(0).text();
            String name = el.getElementsByClass("p-name").eq(0).text();
            goodsList.add(new Content(img, price, name));
           }
        return goodsList;
    }
}
