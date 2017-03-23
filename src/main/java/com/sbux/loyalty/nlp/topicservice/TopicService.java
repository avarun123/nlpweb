package com.sbux.loyalty.nlp.topicservice;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;
import org.elasticsearch.common.UUID;
import org.json.JSONException;
import org.json.JSONObject;

import com.sbux.loyalty.nlp.Exception.DataProcesingException;
import com.sbux.loyalty.nlp.commands.JsonTopicAssignementCommand;
import com.sbux.loyalty.nlp.commands.JsonFileInputParseCommand;
import com.sbux.loyalty.nlp.config.ConfigBean;
import com.sbux.loyalty.nlp.config.ModelBinding;
import com.sbux.loyalty.nlp.config.NameSpace;
import com.sbux.loyalty.nlp.core.datasources.DatasourceClient;
import com.sbux.loyalty.nlp.core.datasources.DatasourceClient.DatasourceFile;
import com.sbux.loyalty.nlp.core.nlpcore.LambdaTopicDetectionProcess;
import com.sbux.loyalty.nlp.core.nlpcore.TopicDetectionProcess;
import com.sbux.loyalty.nlp.databean.NlpBean;
import com.sbux.loyalty.nlp.databean.TopicAssignementOutput;
import com.sbux.loyalty.nlp.databean.TopicAssignmentOutputBean;
import com.sbux.loyalty.nlp.grammar.TopicGrammar;
import com.sbux.loyalty.nlp.grammar.TopicGrammerContainer;
import com.sbux.loyalty.nlp.parsers.InputJsonParser;
import com.sbux.loyalty.nlp.util.GenericUtil;
import com.sbux.loyalty.nlp.util.JsonConvertor;

/**
 * This class does rule based topic detection
 * @author aveettil
 *
 */
@Path("/detecttopics")
public class TopicService  {
	private static final Logger log = Logger.getLogger(TopicService.class);
	public static Map<String,Boolean> taskStatus= new HashMap<>();
	public static Map<String,Integer> numParallelTasks= new HashMap<>();
	
	@GET
	  @Produces("application/text")
	  public Response about() throws JSONException {

		JSONObject jsonObject = new JSONObject();
		 
		jsonObject.put("Description", " topic service "); 
		String result = jsonObject.toString();
		return Response.status(200).entity(result).build();
	  }
	
	/**
	 * Runs topic detection for a model and its current version
	 * @param channel
	 * @param namespace
	 * @param date
	 * @param modelName
	 * @param ui
	 * @return
	 * @throws Exception
	 */
	  @Path("{channel}/{namespace}/{date}/{modelName}")
	  @GET
	  @Produces("application/text")
	  public Response doTopicDetection(@PathParam("channel") String channel,@PathParam("namespace") String namespace,@PathParam("date") String date,@PathParam("modelName") String modelName,@Context UriInfo ui) throws Exception {
			return doTopicDetection(channel, namespace, date, modelName, GenericUtil.getRuleBaseModel(modelName).getCurrentVersion(), ui);
	  }
	
	 /**
	  * Runs topic detection for a model and a model version
	  * @param channel
	  * @param namespace
	  * @param date
	  * @param modelName
	  * @param modelVersion
	  * @param ui
	  * @return
	  * @throws Exception
	  */
	  @Path("{channel}/{namespace}/{date}/{modelName}/{modelVersion}")
	  @GET
	  @Produces("application/text")
	  public Response doTopicDetection(@PathParam("channel") String channel,@PathParam("namespace") String namespace,@PathParam("date") String date,@PathParam("modelName") String modelName,@PathParam("modelVersion") double modelVersion,@Context UriInfo ui) throws Exception {
		try {
			//GenericSnsMsg msgBean = JsonConvertor.getObjectFromJson(json, GenericSnsMsg.class);
			ExecutorService executorService = Executors.newSingleThreadExecutor();

			// generate a task id
			 
			String taskId = UUID.randomUUID().toString().replaceAll("-", "");
			taskStatus.put(taskId, false);
			
			executorService.execute(new Runnable() {
			    public void run() {
			        System.out.println("Topic Detection Job starting");
			        try {
			        	/* Uncomment this to do a sequential non - lambda based topic detection (remember to comment out the lambda topic detection part
			        	 * new TopicDetectionProcess(taskId).doTopicDetection(channel, namespace,modelName,modelVersion,date);
			        	 * taskStatus.put(taskId, true); // update status of task
			        	 */
			        	
			        	/****************************AWS Lambda based topic detection*******************************************/
			        	LambdaTopicDetectionProcess process =new LambdaTopicDetectionProcess(taskId);
			        	process.doTopicDetection(channel, namespace,modelName,modelVersion,date);
						numParallelTasks.put(taskId,process.getNumParallelTasks());
						/****************************END OF AWS Lambda based topic detection ************************************/
						
					} catch (DataProcesingException e) {
					
						e.printStackTrace();
						log.error(e);
					}
			    }
			});

			executorService.shutdown(); // shut down will happen only after the job is completed, even though it is been called now.
			
			return Response.status(200).entity("Topic detection job submitted. job id = "+taskId).build();
		} catch(Exception e){
			log.error(e);
			throw e;
		}
	  }
	  
	  
		  
	  /**
	   * Returns the status of a job completions. (True or false).
	   * @param jobId
	   * @param ui
	   * @return
	   * @throws Exception
	   */
	  @Path("{jobId}")
	  @GET
	  @Produces("application/text")
	  public Response getJobStatus(@PathParam("jobId") String jobId,@Context UriInfo ui) throws Exception {
		try {
			 return Response.status(200).entity(taskStatus.get(jobId)==null?"job not found":taskStatus.get(jobId).toString()).build();
		} catch(Exception e){
			log.error(e);
			throw e;
		}
	  }
}
