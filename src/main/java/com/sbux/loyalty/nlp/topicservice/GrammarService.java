package com.sbux.loyalty.nlp.topicservice;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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
import org.elasticsearch.common.mvel2.optimizers.impl.refl.nodes.ArrayLength;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.sbux.loyalty.nlp.commands.JsonFileInputParseCommand;
import com.sbux.loyalty.nlp.config.ConfigBean;
import com.sbux.loyalty.nlp.config.ModelBinding;
import com.sbux.loyalty.nlp.config.RuleBasedModel;
import com.sbux.loyalty.nlp.core.datasources.DatasourceClient;
import com.sbux.loyalty.nlp.core.datasources.DatasourceClient.DatasourceFile;
import com.sbux.loyalty.nlp.databean.CCCDataInputBean;
import com.sbux.loyalty.nlp.databean.GrammarDiffRequestBody;
import com.sbux.loyalty.nlp.grammar.GrammarDeltaProcessor;
import com.sbux.loyalty.nlp.grammar.JsonTopicGrammar;
import com.sbux.loyalty.nlp.grammar.TopicAssigner;
import com.sbux.loyalty.nlp.grammar.TopicGrammar;
import com.sbux.loyalty.nlp.grammar.TopicGrammarContainer;
import com.sbux.loyalty.nlp.grammar.GrammarDeltaProcessor.GrammarDelta;
import com.sbux.loyalty.nlp.grammar.TopicGrammar.TopicGrammarNode;
import com.sbux.loyalty.nlp.util.GenericUtil;
import com.sbux.loyalty.nlp.util.JsonConvertor;

/**
 * This REST end point is to do CRUD operations on topic grammar.
 * @author aveettil
 *
 */
@Path("/model")
public class GrammarService  {
	private static final Logger log = Logger.getLogger(GrammarService.class);
	int MAX_COMPARISON_SET_SIZE = 200;
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
	   * Returns all existing models and current version names
	   * @param modelName
	   * @param versionNumber
	   * @param ui
	   * @return
	   * @throws Exception
	   */
	  @Path("models")
	  @GET
	  @Produces("application/text")
	  public Response getAllModels(@PathParam("modelName") String modelName,@PathParam("version") double versionNumber, @Context UriInfo ui) throws Exception {
		 try {
			 ConfigBean.reset(); // force to get the latest from data source instead of cached configuration.
			 String json = JsonConvertor.getJson(ConfigBean.getInstance().getRuleBasedModels());
			 return Response.status(200).entity(json).build();
		 } catch(Exception e){
			 log.error(e);
			 return Response.status(500).entity(e.getMessage()).build();
		 }
	  }
	
	  
	  @Path("/diff/{channel}/{namespace}/{modelName}/{modelVersion}")
	  @POST
	  @Produces("application/text")
	  public Response getDiff(@PathParam("channel") String channel,@PathParam("namespace") String namespace,@PathParam("modelName") String modelName,@PathParam("modelVersion") double modelVersion,@Context UriInfo ui,String requestBodyJson) throws Exception {
		  // get the comparison base
		  ModelBinding modelBinding = GenericUtil.getModelBinding(channel, namespace, modelName);
		  int comparisonBaseSize = 100;
		  MultivaluedMap<String, String> queryParams = ui.getQueryParameters();
		  String size = queryParams.getFirst("limit");
		  if(StringUtils.isNotBlank(size)) {
			  comparisonBaseSize = Integer.parseInt(size);
		  }
		  if(modelBinding == null){
			  return Response.status(404).entity("No model binding for  channel = "+channel+" namespace = "+namespace+" modelName = "+modelName+" found").build();
		  }
		  
		  String dataFolder = GenericUtil.getNamespace(channel, namespace).getDataFolder();
		  
		  GrammarDiffRequestBody grammarDiffRequestBody = JsonConvertor.getObjectFromJson(requestBodyJson, GrammarDiffRequestBody.class);
		  String topicToTextFolder = GenericUtil.getTopicToTextOutputFolder(modelBinding.getTopicToTextFolder(), modelVersion, grammarDiffRequestBody.getTopicPathWihtoutModelName(), null, -1);
		  // we get the topicToTextFolder for the topic path. get all the dates and sequenceNumber associated with the data. That will form as the basis for comparison
		  // why? because input data in those files contains the given topic
		  List<DatasourceFile> dataSourceFiles = DatasourceClient.getDefaultDatasourceClient().getListOfFilesInFolder(topicToTextFolder);
		  String dataFolderContainingTopic  = null;
		  String topicToTextFile = null;
		  for(DatasourceFile df:dataSourceFiles) {
			  String dateAndSequenceNumber = GenericUtil.getDateAndSequenceNumberFromTopicToTextOutputFolder(df.getName());
			   dataFolderContainingTopic = dataFolder+"/"+dateAndSequenceNumber;
			   topicToTextFile = df.getName();
			   break;
		  }
		  if(dataFolderContainingTopic == null) {
			  return Response.status(404).entity("No text data found which belongs to the topic "+grammarDiffRequestBody.getTopicPath()).build();
		  }
		  String[] lines = DatasourceClient.getDefaultDatasourceClient().readFileAsString(dataFolderContainingTopic).split("\n");
		  List<String> comparisonSet = new ArrayList<>();
		  int i=0;
		  for(String line:lines){
			  i++;
			  
			  JsonFileInputParseCommand command = new JsonFileInputParseCommand(channel, namespace);
			  command.setRow(line).parse(null);
			  String text = command.getJsonNlpInputBean().getText();
			  comparisonSet.add(text);
			  if(i==comparisonBaseSize) // limiting the size
				  break;
		  }
		  // add lines from the topic to Text
		   lines = DatasourceClient.getDefaultDatasourceClient().readFileAsString(topicToTextFile).split("\n");
		   i=0;
		  for(String line:lines) {
			  i++;
			  comparisonSet.add(line);
			 
			  if(i==comparisonBaseSize) // limiting the size
				  break;
		  }
		  TopicGrammar grammar = TopicGrammarContainer.getTopicGrammar(modelName, modelVersion);
		  TopicGrammarNode node = grammar.getNodeWithPath(grammarDiffRequestBody.getTopicPath());
		  GrammarDelta delta = new GrammarDeltaProcessor().getDelta(node, grammarDiffRequestBody.getNewConstraints(), comparisonSet , true, 5);
		  // get the topicToReviews
		  String json = JsonConvertor.getJson(delta);
		  return Response.status(200).entity(json).build(); 
	  }
	  
	  private void addToCOmparisonBase(String[] lines,List<String> comparisonSet,String channel,String namespace,int  comparisonBaseSize){
		  
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
	  public Response getModel(@PathParam("modelName") String modelName,@PathParam("version") double versionNumber, @Context UriInfo ui) throws Exception {
		try {
			MultivaluedMap<String, String> queryParams = ui.getQueryParameters();
			
			String getRules = queryParams.getFirst("getRules"); 
			if(StringUtils.isBlank(getRules))
				getRules = "false";
			TopicGrammar grammar = TopicGrammarContainer.getTopicGrammar(modelName,versionNumber);
			
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
			return getModel(modelName, TopicGrammarContainer.CURRENT_VERSION, ui);
		  }
		  
		  /**
		   * Deletes a model name and current version from cache
		   * @param modelName
		   * @param ui
		   * @return
		   * @throws Exception
		   */
		  @Path("cache/{modelName}")
		  @DELETE
		  @Produces("application/text")
		  public Response deleteModelFromcache(@PathParam("modelName") String modelName, @Context UriInfo ui) {
			  try {
				double version = GenericUtil.getRuleBaseModel(modelName).getCurrentVersion();
				TopicGrammarContainer.deleteFromCache(modelName, version);
				return Response.status(200).entity("Successfully deleted model "+modelName+" version "+version+" from cache").build();
			  } catch(Exception e) {
				  log.error(e);
				  return Response.status(500).entity("Error : "+e.getMessage()).build();
			 }
		  }
		  
		  /**
		   * Deletes a model name and version from cache
		   * @param modelName
		   * @param version
		   * @param ui
		   * @return
		   * @throws Exception
		   */
		  @Path("cache/{modelName}/{version}")
		  @DELETE
		  @Produces("application/text")
		  public Response deleteModelFromcache(@PathParam("modelName") String modelName, @PathParam("version") double version,@Context UriInfo ui) throws Exception {
			TopicGrammarContainer.deleteFromCache(modelName, version);
			return Response.status(200).entity("Successfully deleted model "+modelName+" version "+version+" from cache").build();
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
				if(model == null) {
					// TODO - this needs to implement creation of a new model if it is not already existing.
					// JIRA - https://starbucks-analytics.atlassian.net/browse/DS-1056
					throw new InvalidArgumentException("Model is not existing. Feature to create a model through API is not yet implemented");
				}
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
