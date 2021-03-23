package com.ray.esjd.service;

import com.alibaba.fastjson.JSON;
import com.ray.esjd.domain.Content;
import com.ray.esjd.utils.HtmlParseUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.jsoup.helper.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Service
public class ContentService {

    private static final String INDEX = "jd_goods";

    @Autowired
    private RestHighLevelClient restHighLevelClient;


    /**
     * 解析网页数据，转成实体，存入es
     * @param keyword
     */
    public boolean parseContent(String keyword) throws IOException {
        List<Content> contentList = new HtmlParseUtil().parseJD(keyword);
        Assert.state(!CollectionUtils.isEmpty(contentList), "未爬取到数据！");

        //创建批量插入的请求
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("2m");

        boolean exists = restHighLevelClient.indices().exists(new GetIndexRequest(INDEX), RequestOptions.DEFAULT);

        if (!exists) {
            restHighLevelClient.indices().create(new CreateIndexRequest(INDEX), RequestOptions.DEFAULT);
        }

        for (int i = 0; i < contentList.size(); i++) {
            bulkRequest.add(new IndexRequest(INDEX)
                .source(JSON.toJSONString(contentList.get(i)), XContentType.JSON));
        }

        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        //5.响应 判断是否执行成功
        return !bulkResponse.hasFailures();
    }


    /**
     * 查询
     * @param pageNum
     * @param pageSize
     * @param keyword
     * @return
     */
    public List<Map<String, Object>> pageList(Integer pageNum, Integer pageSize, String keyword) throws IOException {
        pageNum = (pageNum < 1) ? 1 : pageNum;

        //条件查询request
        SearchRequest searchRequest = new SearchRequest(INDEX);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //精准匹配
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", keyword);
        sourceBuilder.query(matchQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        //分页
        sourceBuilder.from(pageNum);
        sourceBuilder.size(pageSize);


        //高亮
        HighlightBuilder highlightBuilder =  new HighlightBuilder();
        highlightBuilder.field("name");
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        //同一个字段是否全部高亮显示
        highlightBuilder.requireFieldMatch(true);
        sourceBuilder.highlighter(highlightBuilder);
        
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);



        List<Map<String, Object>> list = new ArrayList<>();
        for (SearchHit documentFields : searchResponse.getHits().getHits() ) {
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            Map<String, HighlightField> highlightFields = documentFields.getHighlightFields();
            HighlightField nameField= highlightFields.get("name");

            Optional.ofNullable(nameField).ifPresent(o -> {
                Text[] fragments = o.getFragments();
                StringBuilder newName = new StringBuilder("");
                for (Text fragment : fragments) {
                    newName.append(fragment.toString());
                }
                sourceAsMap.put("name", newName.toString());
            });

            list.add(sourceAsMap);
        }

        return list;
    }



}
