package com.devzy.share.elasticsearch.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

import com.devzy.share.elasticsearch.spring.ElasticsearchSpringFactory;
import com.devzy.share.util.StringUtil;
/**
 * 描述: Elasticsearch初始化实例
 * 时间: 2018年1月9日 上午11:18:20
 * @author yi.zhang
 * @since 1.0
 * JDK版本:1.8
 */
public class ElasticsearchSingleton {
	private static Logger logger = LogManager.getLogger();
	private static class InitSingleton{
		private final static ElasticsearchSingleton INSTANCE = new ElasticsearchSingleton();
	}
	private ElasticsearchSpringFactory factory;
	private ElasticsearchSingleton(){
		try {
			String clusterName = ElasticConfig.getProperty("elasticsearch.cluster.name");
			String servers = ElasticConfig.getProperty("elasticsearch.cluster.servers");
			String username = ElasticConfig.getProperty("elasticsearch.cluster.username");
			String password = ElasticConfig.getProperty("elasticsearch.cluster.password");
//			String http_port = ElasticConfig.getProperty("elasticsearch.http.port");
			String transport_port = ElasticConfig.getProperty("elasticsearch.transport.port");
			factory=init(clusterName, servers, username, password, transport_port);
		} catch (Exception e) {
			logger.error("--Elasticsearch init Error!",e);
		}
	}
	public final static ElasticsearchSingleton getIntance(){
		return InitSingleton.INSTANCE;
	}
	public ElasticsearchTemplate template(){
		return factory.getTemplate();
	}
	
	/**
	 * 描述: Elasticsearch配置[Transport接口]
	 * 时间: 2018年1月9日 上午11:02:08
	 * @author yi.zhang
	 * @param clusterName	集群名
	 * @param servers		服务地址(多地址以','分割)
	 * @param username		认证用户名
	 * @param password		认证密码
	 * @param port			服务端口
	 * @return
	 */
	private ElasticsearchSpringFactory init(String clusterName,String servers,String username,String password,String port){
		try {
			ElasticsearchSpringFactory factory=new ElasticsearchSpringFactory(clusterName, servers, username, password);
			if(!StringUtil.isEmpty(port))factory = new ElasticsearchSpringFactory(clusterName, servers, username, password,Integer.valueOf(port));
			factory.init();
			return factory;
		} catch (Exception e) {
			logger.error("--Spring Elasticsearch init Error!",e);
		}
		return null;
	}
}
