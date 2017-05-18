package com.sbux.loyalty.nlp.topicservice;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.google.gson.reflect.TypeToken;
import com.sbux.loyalty.nlp.Exception.InvalidGrammarException;
import com.sbux.loyalty.nlp.commands.JsonFileInputParseCommand;
import com.sbux.loyalty.nlp.config.ConfigBean;
import com.sbux.loyalty.nlp.config.ModelBinding;
import com.sbux.loyalty.nlp.config.RuleBasedModel;
import com.sbux.loyalty.nlp.core.datasources.DatasourceClient;
import com.sbux.loyalty.nlp.core.datasources.DatasourceClient.DatasourceFile;
import com.sbux.loyalty.nlp.databean.GrammarDiffRequestBody;
import com.sbux.loyalty.nlp.grammar.GrammarDeltaProcessor;
import com.sbux.loyalty.nlp.grammar.GrammarDeltaProcessor.GrammarDelta;
import com.sbux.loyalty.nlp.grammar.InvalidPreviewRequestException;
import com.sbux.loyalty.nlp.grammar.ModelValidator;
import com.sbux.loyalty.nlp.grammar.OnlineConstraintMatcher;
import com.sbux.loyalty.nlp.grammar.OnlineConstraintMatcher.ConstraintMatchMessage;
import com.sbux.loyalty.nlp.grammar.OnlineConstraintMatcher.Filter;
import com.sbux.loyalty.nlp.grammar.OnlineConstraintMatcher.MatchResponse;
import com.sbux.loyalty.nlp.grammar.ModelValidator.ModelValidationResult;
import com.sbux.loyalty.nlp.grammar.TopicGrammar;
import com.sbux.loyalty.nlp.grammar.TopicGrammar.Constraint;
import com.sbux.loyalty.nlp.grammar.TopicGrammar.TopicGrammarNode;
import com.sbux.loyalty.nlp.grammar.TopicGrammarContainer;
import com.sbux.loyalty.nlp.util.GenericUtil;
import com.sbux.loyalty.nlp.util.JsonConvertor;
import com.sbux.loyalty.nlp.util.TextCache;

/**
 * This REST end point is to do CRUD operations on topic grammar.
 * @author aveettil
 *
 */
@Path("/model")
public class GrammarService  {
	private static final Logger log = Logger.getLogger(GrammarService.class);
	int MAX_COMPARISON_SET_SIZE = 2000;
	public static final Map<String,Map<String,Integer>> topicCountCache = new HashMap<>();;
	//private static Map<String,Map<String,Integer>> topicCountMap= new HashMap<>();
	  @GET
	  @Produces("text/plain")
	  public Response about() throws JSONException {

		JSONObject jsonObject = new JSONObject();
		 
		jsonObject.put("Description", " provides API services related models"); 
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
	  @Produces("text/plain")
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
	  @Produces("text/plain")
	  public Response getDiff(@PathParam("channel") String channel,@PathParam("namespace") String namespace,@PathParam("modelName") String modelName,@PathParam("modelVersion") double modelVersion,@Context UriInfo ui,String requestBodyJson) throws Exception {
		  MultivaluedMap<String, String> queryParams = ui.getQueryParameters();
		  String size = queryParams.getFirst("limit");
		  int comparisonBaseSize = MAX_COMPARISON_SET_SIZE;
			 
		  if(StringUtils.isNotBlank(size)) {
			  comparisonBaseSize = Integer.parseInt(size);
		  }
		  DiffResult diffResult = getDiff(channel, namespace, modelName, modelVersion, requestBodyJson, comparisonBaseSize);
		  return Response.status(diffResult.responseCode).entity(diffResult.json == null?diffResult.errorMEssage:diffResult.json ).build(); 
	  }
	  
	    // Match OPTIONS
	  	@Path("/diff/{channel}/{namespace}/{modelName}/{modelVersion}")
	  	@OPTIONS
		public  Response options_getDiff() {
			return Response.status(Response.Status.NO_CONTENT).build();
		}
	  	
	  
	  @Path("/validate/{modelName}/{modelVersion}")
	  @GET
	  @Produces("application/json")
	  public Response validateModel(@PathParam("modelName") String modelName,@PathParam("modelVersion") double modelVersion,@Context UriInfo ui) throws Exception {
		  TopicGrammar grammar = TopicGrammarContainer.getTopicGrammar(modelName, modelVersion);
		  ModelValidator modelValidator = new ModelValidator();
		  ModelValidationResult result = modelValidator.validateModel(grammar);
		  if(result.isSuccess())
		      return Response.status(200).entity("SUCCESS").build(); 
		  else
			  return Response.status(200).entity(JsonConvertor.getJson(result.getErrorMessages())).build(); 
	  }
	  
	  @Path("/validate/{modelName}")
	  @GET
	  @Produces("application/json")
	  public Response validateModel(@PathParam("modelName") String modelName,@Context UriInfo ui) throws Exception {
		// update the model with a new version
			RuleBasedModel model = GenericUtil.getRuleBaseModel(modelName);
			return validateModel(modelName, model.getCurrentVersion(), ui);
	  }
	  
	  
	  public static class DiffResult {
		  String json;
		  String errorMEssage;
		  int responseCode;
		public DiffResult(String json, String errorMEssage, int errorCode) {
			super();
			this.json = json;
			this.errorMEssage = errorMEssage;
			this.responseCode = errorCode;
		}
		  
	  }
	 /**
	  * Returns a Json String representing the diff between two models
	  * @param channel
	  * @param namespace
	  * @param modelName
	  * @param modelVersion
	  * @param requestBodyJson
	  * @param comparisonSetSize
	  * @return
	 * @throws Exception 
	 * @throws InvalidGrammarException 
	  */
	  public DiffResult getDiff(String channel, String namespace, String modelName, double modelVersion,String requestBodyJson,int comparisonSetSize) throws InvalidGrammarException, Exception {
		  GrammarDiffRequestBody grammarDiffRequestBody = JsonConvertor.getObjectFromJson(requestBodyJson, GrammarDiffRequestBody.class);
		  
		  TopicGrammar grammar = TopicGrammarContainer.getTopicGrammar(modelName, modelVersion);
		  TopicGrammarNode node = grammar.getNodeWithPath(grammarDiffRequestBody.getTopicPath());
		  long start = System.currentTimeMillis();
		  Set<String> comparisonSet = getComparisonSetFromCache(channel, namespace, modelName, modelVersion, comparisonSetSize, grammarDiffRequestBody);
		  Set<String> comparisonSet_short = new HashSet<>();
		  int count = 0;
		  for(String s:comparisonSet) {
			  count++;
			  comparisonSet_short.add(s);
			  if(count > comparisonSetSize)
				  break;
		  }
		  GrammarDelta delta = new GrammarDeltaProcessor().getDelta(node, grammarDiffRequestBody.getNewConstraints(), comparisonSet_short , true, 5);
		  System.out.println("time to compute delta " +(System.currentTimeMillis() - start));
		  // get the topicToReviews
		  String json = JsonConvertor.getJson(delta);
		  return new DiffResult(json,null,200);
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
	  @Produces("text/plain")
	  public Response getModel(@PathParam("modelName") String modelName,@PathParam("version") double versionNumber, @Context UriInfo ui) throws Exception {
		try {
			MultivaluedMap<String, String> queryParams = ui.getQueryParameters();
			
			String getRules = queryParams.getFirst("getRules"); 
			if(StringUtils.isBlank(getRules))
				getRules = "false";
			TopicGrammar grammar = TopicGrammarContainer.getTopicGrammar(modelName,versionNumber);
			//fillNodesWithNameAndPath(grammar);
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
		  @Produces("text/plain")
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
		  @Produces("text/plain")
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
		  @Produces("text/plain")
		  public Response deleteModelFromcache(@PathParam("modelName") String modelName, @PathParam("version") double version,@Context UriInfo ui) throws Exception {
			TopicGrammarContainer.deleteFromCache(modelName, version);
			return Response.status(200).entity("Successfully deleted model "+modelName+" version "+version+" from cache").build();
		  }
		  
		  
		 /**
		  * Returns a preview of the texts matching the constraints
		  * @param channel
		  * @param namespace
		  * @param ui
		  * @param requestBody
		  * @return
		  */
		  @Path("preview/{channel}/{namespace}")
		  @POST
		  @Produces("text/plain")
		  public Response getPreview(@PathParam("channel") String channel, @PathParam("namespace") String namespace,@Context UriInfo ui,String requestBody)  {
			  try {
				  ConstraintMatchMessage msg = JsonConvertor.getObjectFromJson(requestBody, ConstraintMatchMessage.class);
				  List<MatchResponse> response = new OnlineConstraintMatcher().getMatchingTexts(msg,false);
				  String json = JsonConvertor.getJson(response);
				  return Response.status(200).entity(json).build();
			  } catch(InvalidPreviewRequestException e){
				  log.info(e);
				  return Response.status(400).entity(e.getMessage()).build();
			  } catch(Exception e1){
				  log.error(e1);
				  return Response.status(500).entity(e1.getMessage()).build();
			  }
		  }
		  
		   /**
		    * warms up the preview request so that lambda is ready and set to go 
		    * @param ui
		    * @return
		    */
		  @Path("preview/warmup")
		  @GET
		  @Produces("text/plain")
		  public Response warmUpPreview(@Context UriInfo ui)  {
			  try {
				  	List<Constraint> l = new ArrayList<>();
					Constraint c = new Constraint();
					c.setName("c1");
					c.setKeywordsRule("accepted, accepts, accept, ((allow, allowed, allows, able, unable, \"could not\", \"can not\", cannot, \"would not let\", \"did not let\") AND (use, used, using, uses)), acceptance");
					c.setAndWords2Rule("store, location, license?, target, \"barnes and noble\", \"barnes & noble\", kroger, hilton, stores, locations, country, usa, america, \"us cards\", \"card was not accepted\", countries, england, uk, britain, travel, travelling, albertsons, \"barnes n nobles\", barnes, \"the starbucks\", korea, pos, germany, \"puerto rico\", abroad, foreign, safeway, caribbean, franchise, franchises, \"they would not accept\", hospital, \"saudi arabia\", \"b&n\", disney, university, campus, airport, school, kiosk, kiosks, paris, bahamas, motorway, \"new york city\", canada, albertson, nobles, , OWNERSHIP_TYPE:ls, OWNERSHIP_TYPE:fr, OWNERSHIP_TYPE:bn, OWNERSHIP_TYPE:fs");
					l.add(c);
					Filter filter = new Filter();
					filter.setChannel("fsc");
					filter.setNamespace("ccc");
					//filter.setModelName("xLIBStarbucksCardMSRLibrary");
					filter.setStartDt("2016-08-01");
					filter.setEndDt("2016-08-01");
					 
					//filter.setModelVersion(1.0);
					ConstraintMatchMessage msg = new ConstraintMatchMessage(l, filter);
					msg.setMinResponse(5);
				
					new OnlineConstraintMatcher().getMatchingTexts(msg,false);
				  return Response.status(200).entity("SUCCESS").build();
			  } catch(InvalidPreviewRequestException e){
				  log.info(e);
				  return Response.status(400).entity(e.getMessage()).build();
			  } catch(Exception e1){
				  log.error(e1);
				  return Response.status(500).entity(e1.getMessage()).build();
			  }
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
	  @Consumes (MediaType.TEXT_PLAIN)
	  @Produces("text/plain")
	  public Response updateModel(@PathParam("modelName") String modelName, @Context UriInfo ui,String json) throws Exception {
		try {
			// validate model
			ModelValidator.ModelValidationResult modelValidationResult= new ModelValidator().validateModel(json);
			if(modelValidationResult.isSuccess()) {
				// update the model with a new version
				RuleBasedModel model = GenericUtil.getRuleBaseModel(modelName);
				if(model == null) {
					// TODO - this needs to implement creation of a new model if it is not already existing.
					// JIRA - https://starbucks-analytics.atlassian.net/browse/DS-1056
					throw new InvalidArgumentException("Model is not existing. Feature to create a model through API is not yet implemented");
				}
				double newVersion = updateConfigWithNewVersion(model, json);
				// TODO: store the diff
				return Response.status(201).entity(newVersion+"").build();
			} else {
				return Response.status(400).entity("Invalid model : Excption is " +JsonConvertor.getJson(modelValidationResult.getErrorMessages())).build();
			}
		} catch(Exception e){
			log.error(e);
			throw e;
		}
	  }
	  
	  // Match OPTIONS
	  	@Path("{modelName}")
	  	@OPTIONS
		public  Response optionsAll() {
			return Response.status(Response.Status.NO_CONTENT).build();
		}
	  
	  
	  
	  /**
	   * Update the model given the path and the constraints in the path
	   * @param modelName
	   * @param path
	   * @param ui
	   * @param json
	   * @return
	   * @throws Exception
	   */
	  @Path("{modelName}/node")
	  @PUT
	  @Consumes (MediaType.TEXT_PLAIN)
	  @Produces(MediaType.TEXT_PLAIN)
	  public Response updateModel(@PathParam("modelName") String modelName,@QueryParam("path") String path, @Context UriInfo ui,String json) throws Exception {
		try {
			// validate model
			RuleBasedModel model = GenericUtil.getRuleBaseModel(modelName);
			if(model == null) {
					return Response.status(404).entity("model "+modelName+" does not exist").build();
			}
			TopicGrammar topicGrammar = TopicGrammarContainer.getTopicGrammar(modelName, model.getCurrentVersion());
			TopicGrammarNode node = topicGrammar.getNodeWithPath(path);
			if(node == null) {
				return Response.status(404).entity("path "+path+" not found in model "+modelName).build();
			}
			Type collectionType = new TypeToken<List<Constraint>>(){}.getType();
			List<Constraint> constraints = JsonConvertor.getObjectFromJson(json, collectionType);
			node.setConstrainsts(constraints);
			ModelValidator.ModelValidationResult modelValidationResult= new ModelValidator().validateModel(topicGrammar);
			if(modelValidationResult.isSuccess()) {
				// update the model with a new version
				 
				double newVersion = updateConfigWithNewVersion(model, topicGrammar);
				// TODO: store the diff
				return Response.status(201).entity(newVersion+"").build();
			} else {
				return Response.status(400).entity("Invalid model : Exception is " +JsonConvertor.getJson(modelValidationResult.getErrorMessages())).build();
			}
		} catch(Exception e){
			log.error(e);
			throw e;
		}
	  }
	  
	  
	  	// Match OPTIONS
	  	@Path("{modelName}/node")
	  	@OPTIONS
		public  Response options_updateModel() {
			return Response.status(Response.Status.NO_CONTENT).build();
		}
	  /**
	   * Returns a given node specified by path argument
	   * @param modelName
	   * @param path
	   * @param ui
	   * @return
	   * @throws Exception
	   */
	  @Path("{modelName}/node")
	  @GET
	  @Produces(MediaType.TEXT_PLAIN)
	  public Response getModelPath(@PathParam("modelName") String modelName,@QueryParam("path") String path, @Context UriInfo ui) throws Exception {
		try {
			// validate model
			RuleBasedModel model = GenericUtil.getRuleBaseModel(modelName);
			if(model == null) {
				return Response.status(404).entity("Model "+modelName+" not found").build();
			}
			TopicGrammar topicGrammar = TopicGrammarContainer.getTopicGrammar(modelName, model.getCurrentVersion());
			TopicGrammarNode node = topicGrammar.getNodeWithPath(path);
			if(node == null) {
				return Response.status(404).entity("path "+path+" not found in model "+modelName).build();
			}
			else {
				return Response.status(201).entity(JsonConvertor.getJson(node)).build();
			}
			 
		} catch(Exception e){
			log.error(e);
			throw e;
		}
	  }
	  
	
	  /**
	   * 
	   * @param channel
	   * @param namespace
	   * @param modelName
	   * @param modelVersion
	   * @param comparisonSetSize
	   * @param grammarDiffRequestBody
	   * @return
	   * @throws Exception
	   */
	  public Set<String> getComparisonSetFromCache(String channel,String namespace,String modelName, double modelVersion,int comparisonSetSize,GrammarDiffRequestBody grammarDiffRequestBody) throws Exception {
		   
		  return TextCache.getInstance(channel, namespace).getTexts();
	  }
		
	  /**
	   * 
	   * @param channel
	   * @param namespace
	   * @param modelName
	   * @param modelVersion
	   * @param comparisonSetSize
	   * @param grammarDiffRequestBody
	   * @return
	   * @throws Exception
	   */
	  public List<String> getComparisonSet(String channel,String namespace,String modelName, double modelVersion,int comparisonSetSize,GrammarDiffRequestBody grammarDiffRequestBody) throws Exception {
		  List<String> comparisonSet = new ArrayList<>();
		  // get the comparison base
		  ModelBinding modelBinding = GenericUtil.getModelBinding(channel, namespace, modelName);
		  
		  if(modelBinding == null){
			  new DiffResult(null,"No model binding for  channel = "+channel+" namespace = "+namespace+" modelName = "+modelName+" found",404);
		  }
		  
		  String dataFolder = GenericUtil.getNamespace(channel, namespace).getDataFolder();
		  
		 
		  String topicToTextFolder = GenericUtil.getTopicToTextOutputFolder(modelBinding.getTopicToTextFolder(), modelVersion, grammarDiffRequestBody.getTopicPathWihtoutModelName(), null, -1,-1,-1);
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
			  return comparisonSet;
		  }
		  String[] lines = DatasourceClient.getDefaultDatasourceClient().readFileAsString(dataFolderContainingTopic).split("\n");
		 
		  int i=0;
		  for(String line:lines){
			  i++;
			  
			  JsonFileInputParseCommand command = new JsonFileInputParseCommand(channel, namespace);
			  command.setRow(line).parse(null);
			  String text = command.getJsonNlpInputBean().getText();
			  comparisonSet.add(text);
			  if(i==comparisonSetSize) // limiting the size
				  break;
		  }
		  // add lines from the topic to Text
		   lines = DatasourceClient.getDefaultDatasourceClient().readFileAsString(topicToTextFile).split("\n");
		   i=0;
		  for(String line:lines) {
			  i++;
			  comparisonSet.add(line);
			 
			  if(i==comparisonSetSize) // limiting the size
				  break;
		  }
		  return comparisonSet;
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
	  @Produces("text/plain")
	  public Response getCurrentVersionNo(@PathParam("modelName") String modelName, @Context UriInfo ui) throws Exception {
		  double currentVersion = GenericUtil.getRuleBaseModel(modelName).getCurrentVersion();
		  return Response.status(200).entity(currentVersion+"").build();
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
	  
	  /**
	   * 
	   * @param model
	   * @param topicGrammar
	   * @return
	   * @throws IOException
	   * @throws Exception
	   */
	  private synchronized double updateConfigWithNewVersion(RuleBasedModel model,TopicGrammar topicGrammar) throws IOException, Exception {
		  synchronized(model) {  
		    String json = JsonConvertor.getJson(topicGrammar.getTopicNodes());
		  	return updateConfigWithNewVersion(model, json);
		  }
	  }
	  
	  private void fillNodesWithNameAndPath(TopicGrammar topicGrammar) {
             for(TopicGrammarNode node:topicGrammar.getTopicNodes().values()){
				 node.setName(node.getName() );
				 node.setPath(node.getPath() ); // explicitly set Path so that it can be serialized into json
			 }
	  }
	 
	  /**
	   * 
	   * @param model
	   * @param json
	   * @return
	   * @throws IOException
	   * @throws Exception
	   */
	  private  double updateConfigWithNewVersion(RuleBasedModel model,String json) throws IOException, Exception {
		  synchronized(model) {  
		    double newVersion = model.getCurrentVersion()+1.0;
			String newVersionFilePath = model.getGrammarFileLocation()+"/"+newVersion+"/"+model.getFileName().replace(".csv",".json");
			
			// create new version file
			DatasourceClient.getDefaultDatasourceClient().createFile(newVersionFilePath, json);
			
			// update the current version to new version
			model.setCurrentVersion(newVersion);
			ConfigBean.storeConfig(ConfigBean.getInstance());
			GenericUtil.reset(); // reset the configuration
			return newVersion;
		  }
	  }
	  
	 public static void main(String[] args) throws InvalidGrammarException, Exception {
//		 String requestBody = "{\"topicPath\":\"cs all volume|in-store experience|in-store - customer service\",\"newConstraints\":[{\"notWords\":\"\",\"andWords1\":\"\",\"andWords2\":\"\",\"keywords\":\"milk\"}]}";
//		 DiffResult result = new GrammarService().getDiff("ccc", "default", "csAllVolume", 1.0, requestBody, 1000);
//		 System.out.println(result.json);
		
			
		 TopicGrammar grammar = TopicGrammarContainer.getTopicGrammar("csAllVolume", 1.0);
		 TopicGrammarNode node = grammar.getNodeWithPath("cs all volume|digital|digital - in-store wi-fi access");
		 System.out.println(JsonConvertor.getJson(node.getConstrainsts()));
		 String json = JsonConvertor.getJson(node.getConstrainsts());
		 
		 Type collectionType = new TypeToken<List<Constraint>>(){}.getType();
		 
		 List<Constraint> constraints = JsonConvertor.getObjectFromJson(json, collectionType);
			node.setConstrainsts(constraints);
			ModelValidator.ModelValidationResult modelValidationResult= new ModelValidator().validateModel(grammar);
	 }
	  

}
