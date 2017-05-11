package com.sbux.loyalty.nlp.topicservice;

import javax.ws.rs.OPTIONS;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
 

public class OptionsResource {
 

	// Match root-resources
	@OPTIONS
	public Response options() {
	    return Response.status(Response.Status.NO_CONTENT).build();
	}

	// Match sub-resources
	@OPTIONS
	@Path("{path:.*}")
	public  Response optionsAll() {
		return Response.status(Response.Status.NO_CONTENT).build();
	}
}
