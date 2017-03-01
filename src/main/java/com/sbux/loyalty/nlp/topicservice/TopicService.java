package com.sbux.loyalty.nlp.topicservice;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.sbux.loyalty.nlp.commands.CCCJsonTopicAssignementCommand;
import com.sbux.loyalty.nlp.commands.CCCSynopsisJsonParseCommand;
import com.sbux.loyalty.nlp.config.ModelBinding;
import com.sbux.loyalty.nlp.config.NameSpace;
import com.sbux.loyalty.nlp.core.datasources.DatasourceClient;
import com.sbux.loyalty.nlp.core.datasources.DatasourceClient.DatasourceFile;
import com.sbux.loyalty.nlp.databean.NlpBean;
import com.sbux.loyalty.nlp.databean.TopicAssignementOutput;
import com.sbux.loyalty.nlp.databean.TopicAssignmentOutputBean;
import com.sbux.loyalty.nlp.grammar.TopicGrammar;
import com.sbux.loyalty.nlp.grammar.TopicGrammerContainer;
import com.sbux.loyalty.nlp.parsers.CCCSynopsisJsonParser;
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
	private static Map<String,Boolean> taskStatus= new HashMap<>();
	@GET
	  @Produces("application/text")
	  public Response about() throws JSONException {

		JSONObject jsonObject = new JSONObject();
		 
		jsonObject.put("Description", " topic service "); 
		String result = jsonObject.toString();
		return Response.status(200).entity(result).build();
	  }
	
	
	  @Path("{channel}/{namespace}/{date}/{modelName}")
	  @GET
	  @Produces("application/text")
	  public Response doTopicDetection(@PathParam("channel") String channel,@PathParam("namespace") String namespace,@PathParam("modelName") String modelName,@PathParam("date") String date,@Context UriInfo ui) throws Exception {
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
						doTopicDetection(channel, namespace,modelName,date);
						taskStatus.put(taskId, true); // update status of task
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
	  
	  
	  private void doTopicDetection(String channelName,String namespace,String modelName,String date) throws DataProcesingException {
		  try {
			   String[] dateparts  = date.split("-");
			   date = dateparts[0]+"/"+dateparts[1]+"/"+dateparts[2];
			   log.info("Getting topic grammar for namespace "+modelName);
			  // retrieve the topic grammar 
			   TopicGrammar grammar = TopicGrammerContainer.getTopicGrammar(modelName);
			   // create parse command
			   CCCSynopsisJsonParseCommand parseCommand = new CCCJsonTopicAssignementCommand(grammar);
			   // create parser to parse data
			//   CCCSynopsisJsonParser parser = new CCCSynopsisJsonParser();
			   
			   // get data location
			   NameSpace ns = GenericUtil.getNamespace(channelName, namespace);
			   String path = ns.getDataFolder()+"/"+date;
			   ModelBinding modelBinding = GenericUtil.getModelBinding(ns, modelName);
			  log.info("Parsing for topic assignement");
			   List<DatasourceFile> dataSourceFiles = DatasourceClient.getDefaultDatasourceClient().getListOfFilesInFolder(path);
			   List<NlpBean> resultSet = new ArrayList<>();
			   for(DatasourceFile df:dataSourceFiles){
				   parsePath( parseCommand, df, resultSet);
			   }
			  // List<NlpBean> resultSet = parser.getResultSet();
			   processResultSet(resultSet, modelBinding.getTopicOutputFolder()+"/"+date,modelBinding.getStatsOutputFolder(),ns.getName(),date);
			   
			} catch(Exception e){
				log.error(e.getMessage(), e);
				throw new DataProcesingException(e.getMessage(), e);
			}
	  }
	  
	  /**
	   * Does a recursive traversal of the directory and processes all the files in the path
	   * @param objectData
	   * @param parseCommand
	   * @param df
	   * @param resultSet
	   * @throws Exception
	   */
	  protected void parsePath(CCCSynopsisJsonParseCommand parseCommand,DatasourceFile df,List<NlpBean> resultSet) throws Exception {
		  CCCSynopsisJsonParser parser = new CCCSynopsisJsonParser();
		  if(df.isDirecttory()) {
			  List<DatasourceFile> dataSourceFiles = DatasourceClient.getDefaultDatasourceClient().getListOfFilesInFolder(df.getName());
			  for(DatasourceFile df_in:dataSourceFiles){
				  parsePath( parseCommand, df_in,resultSet);
			  }
		  } else {
			  InputStream objectData = DatasourceClient.getDefaultDatasourceClient().readFile(df.getName());
			   
			  parser.parseFile(objectData, parseCommand,df.getName());
			  resultSet.addAll(parser.getResultSet());
		  }
	  }
	  /**
	   * 
	   * @param resultSet
	   * @param outputFolder
	   * @param namespace
	   * @param date
	   * @throws Exception
	   */
	  protected void processResultSet(List<NlpBean> resultSet,String outputFolder,String statsOuptuFolder,String namespace,String date) throws Exception {
		   StringBuffer sb = null;
		   // add summary statistics for each topic for the day
		   Map<String,Integer> topicCounts = new HashMap<>();
		  // resultSet.stream().forEach(nlpBean->{((TopicAssignementOutput)nlpBean).getTopicAssignements().stream());
		   for(NlpBean nlpBean:resultSet){
				   TopicAssignementOutput outBean = (TopicAssignementOutput)nlpBean;
				   List<TopicAssignmentOutputBean> topicList = outBean.getTopicAssignements();
				   
				   for(TopicAssignmentOutputBean topic:topicList) {
					  // String level1Topic = topic.getLevels().get(1);
					   StringBuffer topicPath = new StringBuffer();
                       for(int i=1;i<=6;i++){
                    	   String topicName = topic.getLevels().get(i);
                    	   if(topicName==null) {
                    		   break;
                    	   }
                    	   topicPath.append((i==1)?topicName: "/"+topicName);
                    	   Integer currentCount = topicCounts.get(topicPath.toString());
                    	   if( currentCount == null)
                    			   topicCounts.put(topicPath.toString(), 1);
                    	   else
                    		   topicCounts.put(topicPath.toString(), currentCount.intValue() + 1);
                       }
					   String json = JsonConvertor.getJson(topic);
					   if(sb == null){
						   sb = new StringBuffer();
						   sb.append(json);
					   } else {
					        sb.append("\n");
					        sb.append(json);
					   }
				   }
			   }
			   log.info("Uploading topic assignement data to  location "+outputFolder+"/data.txt");
			   try{
				   DatasourceClient.getDefaultDatasourceClient().createFile(outputFolder+"/data.txt", sb.toString());
			   } catch(Exception e){
				   e.printStackTrace( );
			   }
			   log.info("Successfully uploaded "+resultSet.size()+" instances to  location "+outputFolder+"/data.txt");
			   
			   log.info("writing summary statistics");
			   Map<String,Map<String,Integer>> m = new HashMap<>();
			   m.put(date.replace("/", "-"),topicCounts);
			   StatsService.topicCountMap.put(date.replace("/", "-"),topicCounts); // update the cache
			   DatasourceClient.getDefaultDatasourceClient().createFile(statsOuptuFolder+"/"+date+"/data.txt",JsonConvertor.getJson(m));

			   log.info(" successfully uploaded summary statistics to "+statsOuptuFolder+"/"+date);
			   
		}
	  
	  public static void main(String[] args) throws IOException, Exception {
		 
		 List<DatasourceFile> files =  DatasourceClient.getDefaultDatasourceClient().getListOfFilesInFolder("sbux-datascience-nlp/data/ccc/namespaces/csvolumemaster/topic-assignement/2016/08/01/summary");
		 files.stream().forEach(df->depthFirst(df));
	  }
	  
	  private static void depthFirst(DatasourceFile df) {
		  if(df.isDirecttory()) {
			 // path.add(df.get)
			  depthFirst(df);
		  }
		  else
			  System.out.println(df.getName());
	  }


}
