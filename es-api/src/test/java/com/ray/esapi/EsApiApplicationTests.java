package com.ray.esapi;

import com.alibaba.fastjson.JSON;
import com.ray.esapi.domain.User;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;

@SpringBootTest
class EsApiApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;


    @Test
    void createIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("llxx");
        CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    @Test
    void getIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("llxx");
        System.out.println(restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT));
    }

    @Test
    void delIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("llxx");
        AcknowledgedResponse response = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(response.isAcknowledged());
    }


    //================================================================
    @Test
    void createUser() throws IOException {
        User user = new User("雷利", 78);
        //操作那个索引（数据库）
        IndexRequest indexRequest = new IndexRequest("llxx");

        // PUT llxx/_doc/id
        indexRequest.id("1");
        indexRequest.timeout(TimeValue.timeValueSeconds(1));

        //数据放入请求
        indexRequest.source(JSON.toJSONString(user), XContentType.JSON);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.toString());
        System.out.println(indexResponse.status());
    }

    @Test
    void getUser() throws IOException {
        GetRequest getRequest = new GetRequest("llxx", "1");
        GetResponse response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(response.getSourceAsString());
        System.out.println(response);
    }


    @Test
    void isExist() throws IOException {
        GetRequest getRequest = new GetRequest("llxx", "1");
        //如果只是判断是否存在，可以不需要返回 _source 上下文
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        System.out.println(restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT));
    }

        @Test
        void updateUser() throws IOException {
            UpdateRequest updateRequest = new UpdateRequest("llxx", "1");
            updateRequest.timeout("1s");
            User user = new User("库赞", 45);
            //更新的数据全部放入doc中
            updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);
            UpdateResponse response = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
            System.out.println(response.status());
        }

    @Test
    void delUser() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("llxx", "1");
        deleteRequest.timeout("1s");
        DeleteResponse response = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    @Test
    void testBulkRequest() throws IOException {
        //1.创建批量导入数据
        BulkRequest bulkRequest = new BulkRequest();
        //设置多长时间导入一次
        bulkRequest.timeout("10s");
        //2.定义一个集合
        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("maybe", 21));
        userList.add(new User("关羽", 22));
        userList.add(new User("张飞", 20));
        userList.add(new User("刘备", 23));
        //3.将数据批量添加
        for (int i = 0; i < userList.size(); i++) {
            //如果需要做批量删除或者批量更新，修改这里请求即可
            bulkRequest.add(
                    new IndexRequest("llxx")
                            //不填id时将会生成随机id
                            .id("" + (i + 1))
                            .source(JSON.toJSONString(userList.get(i)), XContentType.JSON)
            );
        }
        //4.执行请求
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        //5.响应 判断是否执行成功
        RestStatus status = bulkResponse.status();
        System.out.println(status.getStatus());
    }

    @Test
    void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("llxx");

        //构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        //query builder
        //QueryBuilders.bool()
        //QueryBuilders.match()
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "maybe");
        sourceBuilder.query(termQueryBuilder);

        //高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("name");

        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //所有结果封装在 Hits中
        System.out.println(JSON.toJSONString(searchResponse.getHits()));
    }




    @Test
    void contextLoads() {
    }


}
