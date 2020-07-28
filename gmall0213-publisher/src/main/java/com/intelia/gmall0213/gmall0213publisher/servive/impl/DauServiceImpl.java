package com.intelia.gmall0213.gmall0213publisher.servive.impl;

import com.intelia.gmall0213.gmall0213publisher.servive.DauService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.sql.DriverManager.println;

/**
 * @description TODO
 * @auther Intelia
 * @date 2020.7.24 6:54
 * @mogified By:
 */
@Service
public class DauServiceImpl implements DauService {

    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        String indexName = "gmall_dau_info_" + date.replace("-","")+"-query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(indexName)
                .addType("_doc")
                .build();

        try {
            SearchResult searchResult = jestClient.execute(search);
            return searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询异常");
        }

    }

    @Override
    public Map getDauHourCount(String date) {
        String indexName = "gmall_dau_info_" + date.replace("-","")+"-query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_hour").field("hr").size(24);
        searchSourceBuilder.aggregation(termsBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(indexName)
                .addType("_doc")
                .build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            Map resultMap = new HashMap();
            if(searchResult.getAggregations().getTermsAggregation("groupby_hour") != null){
                List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_hour").getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    resultMap.put(bucket.getKey(),bucket.getCount());
                }
                return resultMap;
            }else{
                return new HashMap();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询异常");
        }
    }
}
