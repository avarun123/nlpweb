package com.sbux.loyalty.nlp.topicservice;

import java.util.List;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PreDestroy;
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
import org.elasticsearch.common.UUID;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.JsonObject;
import com.sbux.loyalty.nlp.aws.LambdaTopicDetectionProcess;
import com.sbux.loyalty.nlp.config.Channel;
import com.sbux.loyalty.nlp.config.ConfigBean;
import com.sbux.loyalty.nlp.config.ModelBinding;
import com.sbux.loyalty.nlp.config.NameSpace;
import com.sbux.loyalty.nlp.config.RuleBasedModel;
import com.sbux.loyalty.nlp.core.TopicDetectionProcess;
import com.sbux.loyalty.nlp.core.datasources.DatasourceClient;
import com.sbux.loyalty.nlp.core.datasources.DatasourceClient.DatasourceFile;
import com.sbux.loyalty.nlp.jobstatus.JobNotFoundException;
import com.sbux.loyalty.nlp.jobstatus.JobStatus;
import com.sbux.loyalty.nlp.jobstatus.JobStatusStore;
import com.sbux.loyalty.nlp.jobstatus.JobStatusStore2;
import com.sbux.loyalty.nlp.util.GenericUtil;
import com.sbux.loyalty.nlp.util.JsonConvertor;
import com.sbux.loyalty.nlp.util.TextCache.TextCacheReloadTask;

/**
 * This class does rule based topic detection
 * @author aveettil
 *
 */
@Path("/topics")
public class TopicService  {
	private static final Logger log = Logger.getLogger(TopicService.class);
	//public static Map<String,Boolean> taskStatus= new HashMap<>();
	//public static Map<String,Integer> numParallelTasks= new HashMap<>();
	Timer textCacheTimer = new Timer();
	 public final int LIMIT_TOPICTOTEXT = 100;
	public TopicService() throws Exception {
		// loads the texts in cache
		//scheduleTextCacheLoad();
		 
	}
	
	@PreDestroy
    public void preDestroy() {
		log.info("cancelling timer task before shutting down");
        textCacheTimer.cancel();
    }
	
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
		 * Runs topic detection for a model and its current version
		 * @param channel
		 * @param namespace
		 * @param date
		 * @param modelName
		 * @param ui
		 * @return
		 * @throws Exception
		 */
		  @Path("/bulk/{channel}/{namespace}/{startDate}/{endDate}/{modelName}")
		  @GET
		  @Produces("application/text")
		  public Response doTopicDetection(@PathParam("channel") String channel,@PathParam("namespace") String namespace,@PathParam("startDate") String startDate,@PathParam("endDate") String endDate,@PathParam("modelName") String modelName,@Context UriInfo ui) throws Exception {
			  String jobid = new TopicDetectionProcess().doBulkTopicDetection(channel, namespace, modelName, GenericUtil.getRuleBaseModel(modelName).getCurrentVersion(), startDate, endDate,null);
			  return Response.status(200).entity (jobid).build();
		  }
		  
	  
	  /**
	   * Returns texts belonging to a particular topic under a model and version for a channel and namespace
	   * @param channel
	   * @param namespace
	   * @param modelName
	   * @param modelVersion
	   * @param topicPath
	   * @param ui
	   * @return
	   * @throws Exception
	   */
	  @Path("texts/{channel}/{namespace}/{modelName}/{modelVersion}/{topicPath}")
	  @GET
	  @Produces("application/text")
	  public Response getTopicTexts(@PathParam("channel") String channel,@PathParam("namespace") String namespace,@PathParam("modelName") String modelName,@PathParam("modelVersion") double modelVersion,@PathParam("topicPath") String topicPath,@Context UriInfo ui) throws Exception {
		try {
			int limitTopicToText = LIMIT_TOPICTOTEXT;
			MultivaluedMap<String, String> queryParams = ui.getQueryParameters();
			String limit = queryParams.getFirst("limit");
			if(StringUtils.isNotBlank(limit))
				limitTopicToText = Integer.parseInt(limit);
			List<String> listOfTexts = GenericUtil.getTextsForTopic(channel, namespace, modelName, modelVersion, topicPath, limitTopicToText);
			return Response.status(200).entity (StringUtils.join(listOfTexts,"\n")).build();
			
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
	   * @param topicPath
	   * @param ui
	   * @return
	   * @throws Exception
	   */
	  @Path("texts/{channel}/{namespace}/{modelName}/{topicPath}")
	  @GET
	  @Produces("application/text")
	  public Response getTopicTexts(@PathParam("channel") String channel,@PathParam("namespace") String namespace,@PathParam("modelName") String modelName,@PathParam("topicPath") String topicPath,@Context UriInfo ui) throws Exception {
		try {
			 
			RuleBasedModel model = GenericUtil.getRuleBaseModel(modelName);
			return getTopicTexts(channel, namespace, modelName, model.getCurrentVersion(), topicPath,ui);
		} catch(Exception e){
			log.error(e);
			throw e;
		}
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
			 
			String jobId = UUID.randomUUID().toString().replaceAll("-", "");
			//taskStatus.put(taskId, false);
			
			executorService.execute(new Runnable() {
			    public void run() {
			        System.out.println("Topic Detection Job starting");
			        try {
			        	/* Uncomment this to do a sequential non - lambda based topic detection (remember to comment out the lambda topic detection part
			        	 * new TopicDetectionProcess(taskId).doTopicDetection(channel, namespace,modelName,modelVersion,date);
			        	 * taskStatus.put(taskId, true); // update status of task
			        	 */
			        	
			        	/****************************AWS Lambda based topic detection*******************************************/
			        	LambdaTopicDetectionProcess process =new LambdaTopicDetectionProcess(channel, namespace,modelName,modelVersion,date,jobId);
			        	process.doTopicDetection();
			        	JobStatusStore.getInstance().createJobStatus(new JobStatus(jobId, process.getNumParallelTasks(), 0, "topic detection for channel = "+channel+" namespace = "+namespace+ " date = "+date+" modelName = "+modelName+" modelVersion = "+modelVersion));
						//numParallelTasks.put(taskId,process.getNumParallelTasks());
						/****************************END OF AWS Lambda based topic detection ************************************/
						
					} catch ( Exception e) {
					
						e.printStackTrace();
						log.error(e);
					}
			    }
			});

			executorService.shutdown(); // shut down will happen only after the job is completed, even though it is been called now.
			
			return Response.status(200).entity(JsonConvertor.getJson(new Job(jobId,"topic detection for channel = "+channel+" namespace = "+namespace+ " date = "+date+" modelName = "+modelName+" modelVersion = "+modelVersion))).build();
		} catch(Exception e){
			log.error(e);
			throw e;
		}
	  }
	  
	  
	  public static class Job {
		  String jobid;
		  String desc;
		public Job(String jobid, String desc) {
			super();
			this.jobid = jobid;
			this.desc = desc;
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
			// return Response.status(200).entity(taskStatus.get(jobId)==null?"job not found":taskStatus.get(jobId).toString()).build();
			JobStatus status = JobStatusStore.getInstance().getJobStatus(jobId);
			status.setPercentCompleted();
			return Response.status(200).entity(JsonConvertor.getJson(status)).build();
		} 
		catch(JobNotFoundException jnf) {
			return Response.status(404).entity("job not found : jobId = "+jobId).build();
		}
		catch(Exception e){
			log.error(e);
			throw e;
		}
	  }
	  
	  public void scheduleTextCacheLoad() throws Exception {
		  List<Channel> channels = ConfigBean.getInstance().getData().getChannels();
			for (Channel chanel:channels) {
				List<NameSpace> nameSpaces = chanel.getNamespaces();
				for(NameSpace ns:nameSpaces) {
					if(ns.getEnableCache().equalsIgnoreCase("true")) {
						textCacheTimer.schedule(new TextCacheReloadTask(chanel.getName(), ns.getName()), 1000);
						log.info("Scheduled TextCacheReloadTask for "+chanel.getName()+"/"+ns.getName());
					}
				}
			}
	  }
	  
	  
	  public static void main(String[] args)   {
	 ConfigBean.propsFile = "sbux-datascience-nlp/config/config.properties";
		     try {
		    	//String jobid = new TopicDetectionProcess().doBulkTopicDetection("fsc", "ccc", "2016-08-01", "2016-08-31",null);
		    	String jobid = new TopicDetectionProcess().doBulkTopicDetection("fsc", "ccc", "cSLoyaltyModel", 1.0, "2016-08-01", "2016-08-31",null);
		    	 //String jobid = new TopicDetectionProcess().doBulkTopicDetection("ccc", "default", "csLoyaltyContacts", 1.0, "2016-08-01", "2016-08-31",null);
		    	 //String jobid= "a4a026e3-c5af-448e-a0f2-7e7dd4a53888";
		    	 while(true) {
		    		 JobStatus status = JobStatusStore2.getInstance().getJobStatus(jobid);
		 			 status.setPercentCompleted();
		 			 System.out.println(JsonConvertor.getJson(status));
		 			 Thread.sleep(10000);
		    	 }
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    // new TopicService().doBulkTopicDetection("appreviews", "appAnnie", "2016-08-01", "2016-08-01");	 
	  }
}
