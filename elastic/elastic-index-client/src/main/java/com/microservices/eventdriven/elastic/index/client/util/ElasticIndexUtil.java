package com.microservices.eventdriven.elastic.index.client.util;

import com.microservices.eventdriven.elastic.model.index.IndexModel;
import java.util.List;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;

public class ElasticIndexUtil<T extends IndexModel> {

  public List<IndexQuery> getIndexQueries(List<T> documents) {
    return documents.stream()
        .map(d -> new IndexQueryBuilder().withId(d.getId()).withObject(d).build())
        .toList();
  }
}
