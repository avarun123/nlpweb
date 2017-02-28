package com.sbux.loyalty.nlp.topicservice;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.sbux.loyalty.nlp.config.ModelBinding;
import com.sbux.loyalty.nlp.core.datasources.DatasourceClient;
import com.sbux.loyalty.nlp.core.datasources.DatasourceClient.DatasourceFile;
import com.sbux.loyalty.nlp.util.GenericUtil;
import com.sbux.loyalty.nlp.util.JsonConvertor;

/**
 * This class does rule based topic detection
 * @author aveettil
 *
 */
@Path("/getstats")
public class StatsService  {
	private static final Logger log = Logger.getLogger(StatsService.class);
	//private static Map<String,Map<String,Integer>> topicCountMap= new HashMap<>();
	@GET
	  @Produces("application/text")
	  public Response about() throws JSONException {

		JSONObject jsonObject = new JSONObject();
		 
		jsonObject.put("Description", " provides topic count statistics "); 
		String result = jsonObject.toString();
		return Response.status(200).entity(result).build();
	  }
	
	
	  @Path("{channel}/{namespace}/{modelName}/{startDate}/{endDate}")
	  @GET
	  @Produces("application/text")
	  public Response getStats(@PathParam("channel") String channel,@PathParam("namespace") String namespace,@PathParam("modelName") String modelName,@PathParam("startDate") String startDate,@PathParam("endDate") String endDate,@Context UriInfo ui) throws Exception {
		try {
            
			ModelBinding modelBinding = GenericUtil.getModelBinding(channel, namespace, modelName);
			DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			LocalDate start = LocalDate.parse(startDate),
			          end   = LocalDate.parse(endDate);
			Map<String,Map<String,Integer>> topicCountMap = new HashMap<>();
			Map<String,Integer> topicCountAggregate = new HashMap<>();
			topicCountMap.put("aggregate",topicCountAggregate);
			for (LocalDate date = start; !date.isAfter(end); date = date.plusDays(1)) {
			    
			    Map<String,Integer> topicCount = getTopicCount(modelBinding.getStatsOutputFolder());
			    topicCount.forEach((key,value)->{
			    	if(topicCountAggregate.get(key) == null) {
			    		topicCountAggregate.put(key,value);
			    	} else {
			    		topicCountAggregate.put(key,topicCountAggregate.get(key)+value);
			    	}
			    });
			    topicCountMap.put(format.format(date), topicCount);
			    
			}
			String json = JsonConvertor.getJson(topicCountMap);
			return Response.status(200).entity(json).build();
		} catch(Exception e){
			log.error(e);
			throw e;
		}
	  }
	  
	  /**
	   * Returns a map of topic counts
	   * @param channel
	   * @param namespace
	   * @param modelName
	   * @param date
	   * @return
	 * @throws Exception 
	 * @throws IOException 
	   */
	  protected Map<String,Integer> getTopicCount(String topicCOuntFolder) throws IOException, Exception {
		   List<DatasourceFile> files = DatasourceClient.getDefaultDatasourceClient().getListOfFilesInFolder(topicCOuntFolder);
		   Map<String,Integer> countMap = new HashMap<>();
		   
			for(DatasourceFile f:files){
				getTopicCount(f, countMap);
			}
		 
		    return countMap;
	  }
	  
	  /**
	   * Returns topic count for a given source directory, after recursively traversing through it. Actual count data is assumed to be 
	   * in a file called data.txt
	   * @param file
	   * @param topicCounts
	   * @throws IOException
	   * @throws Exception
	   */
	  protected void getTopicCount(DatasourceFile file,Map<String,Integer>  topicCounts) throws IOException, Exception {
		   List<DatasourceFile> files = DatasourceClient.getDefaultDatasourceClient().getListOfFilesInFolder(file.getName());
		   for(DatasourceFile df:files){
			   if(df.isDirecttory()){
				    getTopicCount(df,topicCounts);
			   } else {
				   if(df.getName().endsWith("data.txt")){
					   String[] countData = DatasourceClient.getDefaultDatasourceClient().readFileAsString(df.getName()).split("=");
					   topicCounts.put(countData[0],Integer.parseInt(countData[1]));
				   }
			   }
		   }
	  }
	  
	 
	  

}
