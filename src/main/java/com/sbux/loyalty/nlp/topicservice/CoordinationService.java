package com.sbux.loyalty.nlp.topicservice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
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

import com.sbux.loyalty.nlp.config.ConfigBean;
import com.sbux.loyalty.nlp.config.Metadata;
import com.sbux.loyalty.nlp.config.NameSpace;
import com.sbux.loyalty.nlp.config.RuleBasedModel;
import com.sbux.loyalty.nlp.topicservice.GrammarService.DiffResult;
import com.sbux.loyalty.nlp.util.GenericUtil;
import com.sbux.loyalty.nlp.util.JsonConvertor;

/**
 * This class does rule based topic detection. 
 * Note the status progress should be updated in an extrnal space like a data base.
 * @author aveettil
 *
 */
@Path("/service")
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
	
	  @Path("config/refresh")
	  @GET
	  public Response refreshConfig(@Context UriInfo ui)  {
		  try {
			 GenericUtil.reset();
			  return Response.status(200).entity("Successfully refreshed configuration from cache").build();
		  } catch(Exception e) {
			  return Response.status(500).entity("Error occured whilre refreshing cache for configuration : "+e.getMessage()).build(); 
		  }
	  }
	 
	  @Path("config/metadata/{channel}/{namespace}")
	  @GET
	  @Produces("text/plain")
	  public Response getMetaData(@PathParam("channel") String channel,@PathParam("namespace") String namespace) throws Exception {
		  try {
			  ConfigBean.reset(); // force to get the latest from data source instead of cached configuration.
			  NameSpace ns = GenericUtil.getNamespace(channel, namespace);
			  Map<String,Metadata> metadataMapping = ns.getMetaDataMapping();
			  ArrayList<String> metadataNames = new ArrayList<String>();
			  for(Metadata metadata:metadataMapping.values())
			  {
				  metadataNames.add(metadata.getValue());
			  }
			  String json = JsonConvertor.getJson(metadataNames);
			  return Response.status(200).entity(json).build();
		  } catch(Exception e){
			  log.error(e);
			  return Response.status(500).entity(e.getMessage()).build();
		  }
	  }

//	  @Path("progress/{taskId}/{sequenceId}")
//	  @GET
//	  public Response updateTaskProgress(@PathParam("taskId") String taskId,@PathParam("sequenceId") int sequenceId,@Context UriInfo ui) throws Exception {
//			 if(TopicService.taskStatus.get(taskId) == null)  {
//				 return Response.status(401).entity("Unknown task = "+taskId).build();
//			 } else  {
//				 
//				 if(TopicService.numParallelTasks.get(taskId) == taskProgress.get(taskId)) {
//					 return Response.status(200).entity("All sub tasks for task id "+taskId+" completed").build();
//				 }
//				// update task progress
//				 Integer current = taskProgress.putIfAbsent(taskId, 1);
//				 if(current!=null) { // allreadey a value present
//					 taskProgress.put(taskId, current+1);
//				 }
//				 if(TopicService.numParallelTasks.get(taskId) == taskProgress.get(taskId)) {
//					 // all tasks completed
//					 log.info("All sub tasks for task id "+taskId+" completed. Now running summary stats for the day");
//					 //new TopicDetectionProcess().updateTopicCountStats(topicLevels, textId, uniqueIncidentSet);
//					 return Response.status(200).entity("All sub tasks for task id "+taskId+" completed").build();
//				 } else {
//					 return Response.status(200).entity("Num sub tasks completed for tasks Id "+taskId+" = "+taskProgress.get(taskId)).build();
//				 }
//				
//			 }
//	  }
}
