package com.ucloudlink.css.elasticsearch.core;

import org.elasticsearch.client.Client;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.EntityMapper;
import org.springframework.data.elasticsearch.core.ResultsMapper;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.core.convert.MappingElasticsearchConverter;
import org.springframework.data.elasticsearch.core.mapping.SimpleElasticsearchMappingContext;
/**
 * 
 * 描述: 重构ElasticsearchTemplate
 * 时间: 2018年4月13日 下午2:19:04
 * @author yi.zhang
 * @since 1.0
 * JDK版本:1.8
 */
public class ElasticsearchHandleTemplate extends ElasticsearchTemplate {
	public ElasticsearchHandleTemplate(Client client) {
		this(client, new MappingElasticsearchConverter(new SimpleElasticsearchMappingContext()));
	}
	public ElasticsearchHandleTemplate(Client client, EntityMapper entityMapper) {
		this(client, new MappingElasticsearchConverter(new SimpleElasticsearchMappingContext()), entityMapper);
	}
	public ElasticsearchHandleTemplate(Client client, ElasticsearchConverter elasticsearchConverter,EntityMapper entityMapper) {
		super(client, elasticsearchConverter,new DefaultHandleResultMapper(elasticsearchConverter.getMappingContext(), entityMapper));
	}
	public ElasticsearchHandleTemplate(Client client, ResultsMapper resultsMapper) {
		super(client, new MappingElasticsearchConverter(new SimpleElasticsearchMappingContext()), resultsMapper);
	}
	public ElasticsearchHandleTemplate(Client client, ElasticsearchConverter elasticsearchConverter) {
		super(client, elasticsearchConverter, new DefaultHandleResultMapper(elasticsearchConverter.getMappingContext()));
	}
}
