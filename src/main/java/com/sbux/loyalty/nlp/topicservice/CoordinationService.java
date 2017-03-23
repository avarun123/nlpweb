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
import javax.ws.rs.POST;
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
 * This class does rule based topic detection. 
 * Note the status progress should be updated in an extrnal space like a data base.
 * @author aveettil
 *
 */
@Path("/coordinate")
public class CoordinationService  {
	private static final Logger log = Logger.getLogger(CoordinationService.class);
 static Map<String,Integer> taskProgress = new HashMap<>();
	
	@GET
	  @Produces("application/text")
	  public Response about() throws JSONException {

		JSONObject jsonObject = new JSONObject();
		 
		jsonObject.put("Description", " coordination service "); 
		String result = jsonObject.toString();
		return Response.status(200).entity(result).build();
	  }
	
	 
	  @Path("{taskId}/{sequenceId}")
	  @POST
	  public Response updateTaskProgress(@PathParam("taskId") String taskId,@PathParam("sequenceId") int sequenceId,@Context UriInfo ui) throws Exception {
			 if(TopicService.taskStatus.get(taskId) == null)  {
				 return Response.status(401).entity("Unknown task = "+taskId).build();
			 } else  {
				 
				 if(TopicService.numParallelTasks.get(taskId) == taskProgress.get(taskId)) {
					 return Response.status(200).entity("All sub tasks for task id "+taskId+" completed").build();
				 }
				// update task progress
				 Integer current = taskProgress.putIfAbsent(taskId, 1);
				 if(current!=null) { // allreadey a value present
					 taskProgress.put(taskId, current+1);
				 }
				 if(TopicService.numParallelTasks.get(taskId) == taskProgress.get(taskId)) {
					 // all tasks completed
					 log.info("All sub tasks for task id "+taskId+" completed. Now running summary stats for the day");
					 //new TopicDetectionProcess().updateTopicCountStats(topicLevels, textId, uniqueIncidentSet);
					 return Response.status(200).entity("All sub tasks for task id "+taskId+" completed").build();
				 } else {
					 return Response.status(200).entity("Num sub tasks completed for tasks Id "+taskId+" = "+taskProgress.get(taskId)).build();
				 }
				
			 }
	  }
}
