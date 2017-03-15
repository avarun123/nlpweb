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
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.sbux.loyalty.nlp.config.ModelBinding;
import com.sbux.loyalty.nlp.core.datasources.DatasourceClient;
import com.sbux.loyalty.nlp.core.datasources.DatasourceClient.DatasourceFile;
import com.sbux.loyalty.nlp.grammar.TopicGrammar;
import com.sbux.loyalty.nlp.grammar.TopicGrammerContainer;
import com.sbux.loyalty.nlp.util.GenericUtil;
import com.sbux.loyalty.nlp.util.JsonConvertor;

/**
 * This class does rule based topic detection
 * @author aveettil
 *
 */
@Path("/grammar")
public class GrammarService  {
	private static final Logger log = Logger.getLogger(GrammarService.class);
	public static final Map<String,Map<String,Integer>> topicCountCache = new HashMap<>();;
	//private static Map<String,Map<String,Integer>> topicCountMap= new HashMap<>();
	@GET
	  @Produces("application/text")
	  public Response about() throws JSONException {

		JSONObject jsonObject = new JSONObject();
		 
		jsonObject.put("Description", " provides topic count statistics "); 
		String result = jsonObject.toString();
		return Response.status(200).entity(result).build();
	  }
	
	
	  @Path("{modelName}")
	  @GET
	  @Produces("application/text")
	  public Response getModel(@PathParam("modelName") String modelName, @Context UriInfo ui) throws Exception {
		try {
			MultivaluedMap<String, String> queryParams = ui.getQueryParameters();
			
			String getRules = queryParams.getFirst("getRules"); 
			if(StringUtils.isBlank(getRules))
				getRules = "false";
			TopicGrammar grammar = TopicGrammerContainer.getTopicGrammar(modelName);
			
			String json = null;
			
			if("false".equalsIgnoreCase(getRules)) { // need only the node names. Strip rules from json
				json = JsonConvertor.getJson(grammar.getTopicNodes().keySet());
				
			} else {
				json = JsonConvertor.getJson(grammar.getTopicNodes());
			}
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
	  protected Map<String,Map<String,Integer>> getTopicCount(String topicCOuntFolder) throws IOException, Exception {
		  String json = DatasourceClient.getDefaultDatasourceClient().readFileAsString(topicCOuntFolder+"/data.txt");
		  TypeToken<Map<String,Map<String,Integer>>> typeToken = new TypeToken<Map<String,Map<String,Integer>>>() {};
		  return JsonConvertor.getObjectFromJson(json, typeToken.getType());
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
