package com.sbux.loyalty.nlp.web;
 

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
 

import com.sbux.loyalty.nlp.commands.CsvTopicGrammerParseCommand;
import com.sbux.loyalty.nlp.commands.CsvInputParseComand;
import com.sbux.loyalty.nlp.config.ConfigBean;
import com.sbux.loyalty.nlp.databean.CCCDataInputBean;
import com.sbux.loyalty.nlp.grammar.TopicGrammar;
import com.sbux.loyalty.nlp.grammar.Rule;
import com.sbux.loyalty.nlp.grammar.RuleEvaluator;
import com.sbux.loyalty.nlp.grammar.TopicGrammerContainer;
import com.sbux.loyalty.nlp.grammar.TopicGrammar.TopicGrammerNode;
import com.sbux.loyalty.nlp.parsers.CsvTopicGrammarParser;
import com.sbux.loyalty.nlp.util.GenericUtil;
 

/**
 * Servlet implementation class NlpWebUat
 */
public class NlpWebUat extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public NlpWebUat() {
        super();
        // TODO Auto-generated constructor stub
    }
    
    
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		String responseString = "";
		try {
			String rule = request.getParameter("rule");
			String verbatim = request.getParameter("verbatim");
			String getRule = request.getParameter("getRule");
			String action = request.getParameter("action");
			if(StringUtils.isNotBlank(verbatim))
				verbatim = GenericUtil.cleanStringFromNonAsciiChars(verbatim);
//			if("getVerbatim".equalsIgnoreCase(action)) {
//				if(StringUtils.isNotBlank(rule)) {
//					List<String> verbatimsForTopic = getVerbatimForRules(rule);
//					responseString=getHtmlForRuleRsponse(request, rule, verbatimsForTopic);
//				}
//			}
			else if("validateRule".equalsIgnoreCase(action) && StringUtils.isNotBlank(rule) && StringUtils.isNotBlank(verbatim)) {
				responseString = validateRule(request, response, rule,verbatim);
			}
			else if(StringUtils.isNotBlank(getRule)){
				responseString = provideRule(request, response);
			} else {
				responseString= getHtml(request, "", "", "");
			}
			
		} catch(Exception e){
			e.printStackTrace( );
			throw new IOException(e.getMessage());
		}
		response.getWriter().append(responseString);
		
	}
	
	 
	
	String validateRule(HttpServletRequest request, HttpServletResponse response,String ruleString,String verbatim) throws Exception {
		List<Rule> ruleList = new ArrayList<>();
		com.sbux.loyalty.nlp.grammar.Rule.getRules(ruleString,ruleList);
		CCCDataInputBean inputBean = new CCCDataInputBean();
		inputBean.setText(verbatim);
		RuleEvaluator ruleEvaluator = new RuleEvaluator(inputBean,6);
		boolean result = ruleEvaluator.evaluateRule(ruleList).isMatching();
		return getHtml(request, ruleString, verbatim, Boolean.toString(result));
	}
	String provideRule(HttpServletRequest request, HttpServletResponse response) {
		try {
			
		    TopicGrammar topicGrammar = TopicGrammerContainer.getTopicGrammar(ConfigBean.getInstance(),"csvolumemaster",TopicGrammerContainer.CURRENT_VERSION);
		    TopicGrammerNode node = topicGrammar.getRoot();
		    return depthFirstTraverse(node);
		} catch(Exception e){
			e.printStackTrace( );
			 
		}
		return "";
	}
	public static String depthFirstTraverse(TopicGrammerNode root) throws Exception {
		TopicGrammerNode node = root;
         if(!root.name.equals("root")) {
			 // there are about 600 rules. generate them uniform
        	 return "";
		 }
 		for(TopicGrammerNode n:root.children){
 			depthFirstTraverse(n);
 		}
	 	return "";
	}
	
	public String getHtml(HttpServletRequest request,String ruleString,String verbatim,String validationResult) throws Exception{
		String url = request.getRequestURL().toString();
//		StringBuffer requestURL = request.getRequestURL();
//		if (request.getQueryString() != null) {
//		    requestURL.append("?").append(URLEncoder.encode(request.getQueryString(),"UTF-8"));
//		}
//		String completeURL = requestURL.toString();
		String retValue = "<html><form action=\""+url+"\"> Rule:<br> <input type=\"text\" style=\"width: 1000px;height: 30px\" name=\"rule\" value=\""+ruleString.replace("\"","&#34;").replace("'","&#39;")+"\">"
				+"<br> verbatim to test:<br>  <input type=\"text\" name=\"verbatim\" style=\"width: 1000px;height: 30px\" value=\""+verbatim.replace("\"","&#34;").replace("'","&#39;")+"\"><br>"
						+ "<br> validation result : "+validationResult;
		retValue+="<br><input type=\"radio\" name=\"action\" value=\"getVerbatim\"> getVerbatim <br> <input type=\"radio\" name=\"action\" value=\"validateRule\" checked=\"checked\"> validateRule<br>";
		retValue+="<br><input type=\"submit\" value=\"Submit\"></form></html>";
	    return retValue;
	}
	public String getHtmlForRuleRsponse(HttpServletRequest request,String ruleString,List<String> verbatims) throws Exception{
		String url = request.getRequestURL().toString();
//		StringBuffer requestURL = request.getRequestURL();
//		if (request.getQueryString() != null) {
//		    requestURL.append("?").append(URLEncoder.encode(request.getQueryString(),"UTF-8"));
//		}
//		String completeURL = requestURL.toString();
		String retValue = "<html><form action=\""+url+"\"> Rule:<br> <input type=\"text\" style=\"width: 1000px;height: 30px\" name=\"rule\" value=\""+ruleString.replace("\"","&#34;").replace("'","&#39;")+"\"><br> verbatims <br> ";
		retValue+="<br> verbatim to test:<br>  <input type=\"text\" name=\"verbatim\" style=\"width: 1000px;height: 30px\" value=\"\"><br>";
		if(verbatims!=null) {
			for(String verbatim:verbatims)
			  retValue+=verbatim.replace("\"","&#34;").replace("'","&#39;")+"<br><br>";
	    
		}
		retValue+="<br><input type=\"radio\" name=\"action\" value=\"getVerbatim\"> getVerbatim <br> <input type=\"radio\" name=\"action\" value=\"validateRule\" checked=\"checked\"> validateRule<br>";
		retValue+="<br><input type=\"submit\" value=\"Submit\"></form></html>";
		return retValue;
	}
	

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
