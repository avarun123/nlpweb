package com.sbux.loyalty.nlp.topicservice;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.sbux.loyalty.nlp.config.ConfigBean;
import com.sbux.loyalty.nlp.config.ModelBinding;
import com.sbux.loyalty.nlp.config.RuleBasedModel;
import com.sbux.loyalty.nlp.core.datasources.DatasourceClient;
import com.sbux.loyalty.nlp.core.datasources.DatasourceClient.DatasourceFile;
import com.sbux.loyalty.nlp.databean.CCCDataInputBean;
import com.sbux.loyalty.nlp.grammar.JsonTopicGrammar;
import com.sbux.loyalty.nlp.grammar.TopicAssigner;
import com.sbux.loyalty.nlp.grammar.TopicGrammar;
import com.sbux.loyalty.nlp.grammar.TopicGrammerContainer;
import com.sbux.loyalty.nlp.util.GenericUtil;
import com.sbux.loyalty.nlp.util.JsonConvertor;

/**
 * This REST end point is to do CRUD operations on topic grammar.
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
	
	
	/**
	 * Returns the grammar for a model name and version
	 * @param modelName
	 * @param versionName
	 * @param ui
	 * @return
	 * @throws Exception
	 */
	  @Path("{modelName}/{version}")
	  @GET
	  @Produces("application/text")
	  public Response getModel(@PathParam("modelName") String modelName,@PathParam("versionName") double versionNumber, @Context UriInfo ui) throws Exception {
		try {
			MultivaluedMap<String, String> queryParams = ui.getQueryParameters();
			
			String getRules = queryParams.getFirst("getRules"); 
			if(StringUtils.isBlank(getRules))
				getRules = "false";
			TopicGrammar grammar = TopicGrammerContainer.getTopicGrammar(modelName,versionNumber);
			
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
		 * Returns the grammar for a model name. Returns current version
		 * @param modelName
		 * @param ui
		 * @return
		 * @throws Exception
		 */
		  @Path("{modelName}")
		  @GET
		  @Produces("application/text")
		  public Response getModel(@PathParam("modelName") String modelName, @Context UriInfo ui) throws Exception {
			return getModel(modelName, TopicGrammerContainer.CURRENT_VERSION, ui);
		  }
	  
	  /**
	   * Updates a model and returns the latest version after updating it.
	   * @param modelName
	   * @param ui
	   * @return
	   * @throws Exception
	   */
	  @Path("{modelName}")
	  @PUT
	  @Consumes("application/text")
	  public Response updateModel(@PathParam("modelName") String modelName, @Context UriInfo ui,String json) throws Exception {
		try {
			// validate model
			 ModelValidationResult modelValidationResult= validateModel(json);
			if(modelValidationResult.isValid) {
				// update the model with a new version
				RuleBasedModel model = GenericUtil.getRuleBaseModel(modelName);
				double newVersion = model.getCurrentVersion()+1.0;
				String newVersionFilePath = model.getGrammarFileLocation()+"/"+newVersion+"/"+model.getFileName();
				
				// create new version file
				DatasourceClient.getDefaultDatasourceClient().createFile(newVersionFilePath, json);
				
				// update the current version to new version
				model.setCurrentVersion(newVersion);
				ConfigBean.storeConfig(ConfigBean.getInstance());
				// TODO: store the diff
				return Response.status(201).entity(newVersion).build();
			} else {
				return Response.status(400).entity("Invalid model : Excption is " +modelValidationResult.errorMsg).build();
			}
		} catch(Exception e){
			log.error(e);
			throw e;
		}
	  }
	  
	  
	   /**
	    * Returns the current version number
	    * @param modelName
	    * @param ui
	    * @return
	    * @throws Exception
	    */
	  @Path("currentVersionNo/{modelName}")
	  @GET
	  @Produces("application/text")
	  public Response getCurrentVersionNo(@PathParam("modelName") String modelName, @Context UriInfo ui) throws Exception {
		  double currentVersion = GenericUtil.getRuleBaseModel(modelName).getCurrentVersion();
		  return Response.status(200).entity(currentVersion).build();
	  }
	  
	  /**
	   * validates a model
	   * @param json
	   * @return
	   */
	  public ModelValidationResult validateModel(String json) {
		  // first validate that this is a valid json
		  try {
			  JsonTopicGrammar grammar = new JsonTopicGrammar();
			  grammar.parse(json);
			  boolean result =  !grammar.getGrammar().getTopicNodes().isEmpty() && grammar.getGrammar().getTopicNodes().size() > 0;
			  // do a vanilla topic assignement
			  new TopicAssigner(grammar.getGrammar()).doTopicAssignement(new CCCDataInputBean("Starbucks promotion is great"), true, 5);
			  // ensure that there are 
		  } catch(Exception e){
			  e.printStackTrace();
			  return new ModelValidationResult(false, e.getMessage());
		  }
		  return new ModelValidationResult(true, "");
	  }
	  
	  private static class ModelValidationResult {
		  boolean isValid;
		  String errorMsg;
		public ModelValidationResult(boolean isValid, String errorMsg) {
			super();
			this.isValid = isValid;
			this.errorMsg = errorMsg;
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
