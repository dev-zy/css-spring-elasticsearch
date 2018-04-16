package com.ucloudlink.css.elasticsearch.spring;

import java.net.InetAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

import com.ucloudlink.css.util.StringUtil;

public class ElasticsearchSpringFactory {
	private static Logger logger = LogManager.getLogger();
	protected static String regex = "[-,:,/\"]";
	protected ElasticsearchTemplate template;
	private String clusterName;
	private String servers;
	private String username;
	private String password;
	private int port=9300;
	
	public ElasticsearchSpringFactory(String clusterName, String servers) {
		super();
		this.clusterName = clusterName;
		this.servers = servers;
	}
	public ElasticsearchSpringFactory(String clusterName, String servers, int port) {
		super();
		this.clusterName = clusterName;
		this.servers = servers;
		this.port = port;
	}
	public ElasticsearchSpringFactory(String clusterName, String servers, String username, String password) {
		super();
		this.clusterName = clusterName;
		this.servers = servers;
		this.username = username;
		this.password = password;
	}
	public ElasticsearchSpringFactory(String clusterName, String servers, String username, String password, int port) {
		super();
		this.clusterName = clusterName;
		this.servers = servers;
		this.username = username;
		this.password = password;
		this.port = port;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getServers() {
		return servers;
	}

	public void setServers(String servers) {
		this.servers = servers;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * 描述: Elasticsearch服务初始化
	 * 时间: 2017年11月14日 上午10:55:02
	 * @author yi.zhang
	 */
	public void init(){
		try {
			PreBuiltTransportClient client;
			Builder builder = Settings.builder();
			builder.put("cluster.name", clusterName);
			builder.put("client.transport.sniff", true);
			builder.put("client.transport.ignore_cluster_name", true);
			if(!StringUtil.isEmpty(username)&&!StringUtil.isEmpty(password)){
				builder.put("xpack.security.user",username+":"+password);
				Settings settings = builder.build();
				client = new PreBuiltXPackTransportClient(settings);
			}else{
				Settings settings = builder.build();
				client = new PreBuiltTransportClient(settings);
			}
			for(String server : servers.split(",")){
				String[] address = server.split(":");
				String ip = address[0];
				int _port=port;
				if(address.length>1){
					_port = Integer.valueOf(address[1]);
				}
				client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), _port));
			}
			template = new ElasticsearchTemplate(client);
		} catch (Exception e) {
			logger.error("-----Elasticsearch Config init Error-----", e);
		}
	}
	public void close(){
		if(template!=null)template.getClient().close();
	}
	
	public ElasticsearchTemplate getTemplate(){
		return template;
	}
}
