package com.example.elasticdemo.resource;

import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/users/")
public class UsersResource {

    RestHighLevelClient client;

    public UsersResource() {
        client = new RestHighLevelClient(RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));
    }

    @GetMapping("/createUser")
    public String createIndex() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "Ajay");
        map.put("salary", 1200);
        map.put("teamName", "Development");

        IndexRequest request = new IndexRequest("employee", "id", "1").source(map);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        return response.getResult().toString();
    }

    @GetMapping("/getUser/{id}")
    public Map<String, Object> getUser(@PathVariable final String id) throws IOException {
        GetRequest request = new GetRequest("employee", "id", id);
        GetResponse response = client.get(request, RequestOptions.DEFAULT);

        return response.getSource();
    }

    @GetMapping("/update/{id}")
    public Map<String, Object> updateUser(@PathVariable final String id) throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("gender", "male");

        UpdateRequest request = new UpdateRequest("employee", "id", id).fetchSource(true);
        request.doc(map);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);

        return response.getGetResult().getSource();
    }

    @GetMapping("delete/{id}")
    public String deleteUser(@PathVariable final String id) throws IOException {
        DeleteRequest request = new DeleteRequest("employee", "id", id);
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);

        return response.getResult().toString();
    }

    @PreDestroy
    public void preDestroy() throws IOException {
        client.close();
    }


}
