package com.devzy.share.elasticsearch.api;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder.Field;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.devzy.share.elasticsearch.config.ElasticsearchSingleton;
import com.devzy.share.util.StringUtil;

public class SpringElasticsearch {
//	private static Logger logger = LogManager.getLogger();
	protected static String regex = "[-,:,/\"]";
	protected ElasticsearchTemplate template = ElasticsearchSingleton.getIntance().template();
	
	public String save(Object obj) {
		IndexQuery query = new IndexQuery();
		query.setObject(obj);
		String result = template.index(query);
		return result;
	}
	public String insert(String index, String type, Object obj) {
		if (!template.indexExists(index)) {  
			template.createIndex(index);  
		}
		IndexQuery query = new IndexQuery();
		query.setIndexName(index);
		query.setType(type);
		query.setObject(obj);
		String result = template.index(query);
		return result;
	}
	public String update(String index,String type,String id,Object json){
		UpdateRequest request = new UpdateRequest(index, type, id);
		request.doc(json);
		UpdateQuery query = new UpdateQuery();  
		query.setId(id);
		query.setUpdateRequest(request); 
		query.setIndexName(index);  
		query.setType(type);
		String result = template.update(query).toString();
		return result;
	}
	public String upsert(String index,String type,String id,Object json){
		UpdateRequest request = new UpdateRequest(index, type, id);
		request.doc(json);
		UpdateQuery query = new UpdateQuery();
		query.setDoUpsert(true);
		query.setId(id);
		query.setUpdateRequest(request); 
		query.setIndexName(index);  
		query.setType(type);
		String result = template.update(query).toString();
		return result;
	}
	public String delete(String index,String type,String id){
		String result = template.delete(index, type, id);
		return result;
	}
	public void bulkUpsert(String index, String type,List<Object> objs) {  
        int counter = 0;  
        try {  
        	if (!template.indexExists(index)) {  
    			template.createIndex(index);  
            } 
            List<UpdateQuery> queries = new ArrayList<UpdateQuery>();  
            for (Object obj : objs) {
            	int count = objs.indexOf(obj);
    			String source = obj instanceof String ?obj.toString():JSON.toJSONString(obj);
    			JSONObject json = JSON.parseObject(source);
    			String id = null;
    			if(json.containsKey("id")||json.containsKey("_id")){
    				if(json.containsKey("_id")){
    					id = json.getString("_id");
    					json.remove("_id");
    				}else{
    					id = json.getString("id");
    					json.remove("id");
    				}
    			}
    			if(StringUtil.isEmpty(id)){
    				id = UUIDs.base64UUID();
    			}
    			UpdateRequest request = new UpdateRequest(index, type, id);
				request.doc(obj);
				UpdateQuery query = new UpdateQuery();
				query.setDoUpsert(true);
				query.setId(id);
				query.setUpdateRequest(request); 
				query.setIndexName(index);  
				query.setType(type); 
    			if (queries.size()%500==0||count+1==objs.size()) {  
                    template.bulkUpdate(queries);
                    queries.clear();  
                    System.out.println("bulkIndex counter : " + counter);  
                } 
    		}
            System.out.println("bulkIndex completed.");  
        } catch (Exception e) {  
            System.out.println("IndexerService.bulkIndex e;" + e.getMessage());  
            throw e;  
        }  
    }
	public String bulkDelete(String index, String type, String... ids) {
//		TermsQueryBuilder query = QueryBuilders.termsQuery("id", ids);
//		DeleteQuery delete = new DeleteQuery();
//		delete.setIndex(index);
//		delete.setType(type);
//		delete.setQuery(query);
		List<String> _ids = new ArrayList<String>();
		for (String id : ids) {
			_ids.add(id);
		}
		Criteria criteria = new Criteria();
		CriteriaQuery delete = new CriteriaQuery(criteria);
		delete.setIds(_ids);
		delete.addIndices(index);
		delete.addTypes(type);
		delete.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		template.delete(delete, null);
		return null;
	}
	public String selectAll(String indexs, String types, String condition) {
		if(StringUtil.isEmpty(indexs))indexs="_all";
		Criteria criteria = new Criteria();
		criteria.expression(condition);
		CriteriaQuery query = new CriteriaQuery(criteria);
		query.addIndices(indexs.split(","));
		query.addTypes(types.split(","));
		query.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		String result = JSON.toJSONString(template.queryForList(query, JSONArray.class));
		return result;
	}
	public String selectMatchAll(String indexs,String types,String field,String value){
		try {
			Criteria criteria = new Criteria(field);
			criteria.contains(value);
			CriteriaQuery query = new CriteriaQuery(criteria);
			query.addIndices(indexs.split(","));
			query.addTypes(types.split(","));
			query.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
//			NativeSearchQuery query = new NativeSearchQuery(QueryBuilders.matchQuery(field, value));
//			query.addIndices(indexs.split(","));
//			query.addTypes(types.split(","));
//			query.addAggregation(AggregationBuilders.terms("data").field(field+".keyword"));
//			query.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
			String result = JSON.toJSONString(template.queryForPage(query, null));
			return result;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String selectMatchAll(String indexs, String types,Class<?> clazz, Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges, String order, boolean isAsc, int pageNo,int pageSize) {
		pageNo=pageNo<1?1:pageNo;
		pageSize=pageSize<1?10:pageSize;
		BoolQueryBuilder boolquery = QueryBuilders.boolQuery();
		List<Field> highlight = new ArrayList<Field>();
		if(must!=null&&must.size()>0){
			for (String field : must.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						BoolQueryBuilder child = QueryBuilders.boolQuery();
						List<Object> values = JSON.parseArray(value, Object.class);
						for (Object _value : values) {
							if(_value instanceof String && ((String)_value).matches(regex)){
								continue;
							}
							child.should(QueryBuilders.matchQuery(field, value));
						}
						boolquery.must(child);
					}else{
						if(!value.matches(regex)){
							boolquery.must(QueryBuilders.matchQuery(field, value));
						}
					}
				}
				highlight.add(new Field(field));
			}
		}
		if(should!=null&&should.size()>0){
			for (String field : should.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						BoolQueryBuilder child = QueryBuilders.boolQuery();
						List<Object> values = JSON.parseArray(value, Object.class);
						for (Object _value : values) {
							if(_value instanceof String && ((String)_value).matches(regex)){
								continue;
							}
							child.should(QueryBuilders.matchQuery(field, value));
						}
						boolquery.should(child);
					}else{
						if(!value.matches(regex)){
							boolquery.should(QueryBuilders.matchQuery(field, value));
						}
					}
				}
				highlight.add(new Field(field));
			}
		}
		if(must_not!=null&&must_not.size()>0){
			for (String field : must_not.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						BoolQueryBuilder child = QueryBuilders.boolQuery();
						List<Object> values = JSON.parseArray(value, Object.class);
						for (Object _value : values) {
							if(_value instanceof String && ((String)_value).matches(regex)){
								continue;
							}
							child.should(QueryBuilders.matchQuery(field, value));
						}
						boolquery.mustNot(child);
					}else{
						if(!value.matches(regex)){
							boolquery.mustNot(QueryBuilders.matchQuery(field, value));
						}
					}
				}
				highlight.add(new Field(field));
			}
		}
		if(ranges!=null&&ranges.size()>0){
			for (String key : ranges.keySet()) {
				if(key.matches(regex)){
					continue;
				}
				List<Object> between = ranges.get(key);
				if(between!=null&&!between.isEmpty()){
					Object start = between.get(0);
					Object end = between.size()>1?between.get(1):null;
					start = start!=null&&start instanceof Date?((Date)start).getTime():start;
					end = end!=null&&end instanceof Date?((Date)end).getTime():end;
					if(start!=null&&end!=null){
						double starttime = Double.valueOf(start.toString());
						double endtime = Double.valueOf(end.toString());
						if(starttime>endtime){
							Object temp = start;
							start = end;
							end = temp;
						}
					}
					RangeQueryBuilder range = QueryBuilders.rangeQuery(key);
					if(start!=null){
						range.gte(start);
					}
					if(end!=null){
						range.lt(end);
					}
					boolquery.must(range);
				}
			};
		}
		Field[] highlightFields = new Field[highlight.size()];
		highlight.toArray(highlightFields);
		NativeSearchQueryBuilder builder = new NativeSearchQueryBuilder();
		builder.withQuery(boolquery);
//		builder.withFilter(boolquery);
		builder.withHighlightFields(highlightFields);
		builder.withIndices(indexs.split(","));
		builder.withTypes(types.split(","));
		builder.withSort(SortBuilders.scoreSort());
		if(!StringUtil.isEmpty(order)){
			builder.withSort(SortBuilders.fieldSort(order).order(isAsc?SortOrder.ASC:SortOrder.DESC));
		}
		PageRequest pageable = PageRequest.of((pageNo-1)*pageSize, pageSize);
//		builder.withPageable(pageable);
		builder.withSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		SearchQuery query = builder.build();
//		query.addIndices(indexs.split(","));
//		query.addTypes(types.split(","));
		query.setPageable(pageable);
		AggregatedPage<?> response = template.queryForPage(query, clazz);
		return JSON.toJSONString(response);
	}
	public String selectPrefixMatchAll(String indexs, String types,Class<?> clazz, Map<String, String> prefix, Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges, int pageNo,int pageSize) {
		pageNo=pageNo<1?1:pageNo;
		pageSize=pageSize<1?10:pageSize;
		BoolQueryBuilder boolquery = QueryBuilders.boolQuery();
		if(must!=null&&must.size()>0){
			for (String field : must.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						BoolQueryBuilder child = QueryBuilders.boolQuery();
						List<Object> values = JSON.parseArray(value, Object.class);
						for (Object _value : values) {
							if(_value instanceof String && ((String)_value).matches(regex)){
								continue;
							}
							child.should(QueryBuilders.matchQuery(field, value));
						}
						boolquery.must(child);
					}else{
						if(!value.matches(regex)){
							boolquery.must(QueryBuilders.matchQuery(field, value));
						}
					}
				}
			}
		}
		if(should!=null&&should.size()>0){
			for (String field : should.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						BoolQueryBuilder child = QueryBuilders.boolQuery();
						List<Object> values = JSON.parseArray(value, Object.class);
						for (Object _value : values) {
							if(_value instanceof String && ((String)_value).matches(regex)){
								continue;
							}
							child.should(QueryBuilders.matchQuery(field, value));
						}
						boolquery.should(child);
					}else{
						if(!value.matches(regex)){
							boolquery.should(QueryBuilders.matchQuery(field, value));
						}
					}
				}
			}
		}
		if(must_not!=null&&must_not.size()>0){
			for (String field : must_not.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						BoolQueryBuilder child = QueryBuilders.boolQuery();
						List<Object> values = JSON.parseArray(value, Object.class);
						for (Object _value : values) {
							if(_value instanceof String && ((String)_value).matches(regex)){
								continue;
							}
							child.should(QueryBuilders.matchQuery(field, value));
						}
						boolquery.mustNot(child);
					}else{
						if(!value.matches(regex)){
							boolquery.mustNot(QueryBuilders.matchQuery(field, value));
						}
					}
				}
			}
		}
		if(ranges!=null&&ranges.size()>0){
			for (String key : ranges.keySet()) {
				if(key.matches(regex)){
					continue;
				}
				List<Object> between = ranges.get(key);
				if(between!=null&&!between.isEmpty()){
					Object start = between.get(0);
					Object end = between.size()>1?between.get(1):null;
					start = start!=null&&start instanceof Date?((Date)start).getTime():start;
					end = end!=null&&end instanceof Date?((Date)end).getTime():end;
					if(start!=null&&end!=null){
						double starttime = Double.valueOf(start.toString());
						double endtime = Double.valueOf(end.toString());
						if(starttime>endtime){
							Object temp = start;
							start = end;
							end = temp;
						}
					}
					RangeQueryBuilder range = QueryBuilders.rangeQuery(key);
					if(start!=null){
						range.gte(start);
					}
					if(end!=null){
						range.lt(end);
					}
					boolquery.must(range);
				}
			};
		}
		if(prefix!=null&&prefix.size()>0){
			for (String field : prefix.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				String value = prefix.get(field);
				boolquery.must(QueryBuilders.prefixQuery(field, value));
			}
		}
		NativeSearchQueryBuilder builder = new NativeSearchQueryBuilder();
		builder.withQuery(boolquery);
		builder.withIndices(indexs.split(","));
		builder.withTypes(types.split(","));
		PageRequest pageable = PageRequest.of(pageNo-1, pageSize);
		builder.withSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		SearchQuery query = builder.build();
		query.setPageable(pageable);
		AggregatedPage<?> response = template.queryForPage(query, clazz);
		return JSON.toJSONString(response);
	}
}
