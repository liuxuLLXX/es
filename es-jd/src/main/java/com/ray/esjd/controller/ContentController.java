package com.ray.esjd.controller;

import com.ray.esjd.service.ContentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
public class ContentController {


    @Autowired
    private ContentService contentService;


    @GetMapping("/parse/{keyword}")
    public boolean parse(@PathVariable("keyword") String keyword) throws IOException {
        return contentService.parseContent(keyword);
    }


    @GetMapping("/page")
    public List<Map<String, Object>> pageList(@RequestParam(value = "pageNum", required = false) Integer pageNum,
                                              @RequestParam(value= "pageSize", required = false) Integer pageSize,
                                              @RequestParam(value= "keyword", required = false) String keyword) throws IOException {
        return contentService.pageList(pageNum, pageSize, keyword);
    }

}
