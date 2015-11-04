package spongecell.guardian.application;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.PreDestroy;
import javax.servlet.http.HttpServletRequest;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import spongecell.guardian.agent.yarn.Agent;
import spongecell.guardian.agent.yarn.ResourceManagerAppMonitorScheduler;

import com.fasterxml.jackson.core.util.ByteArrayBuilder;

@Slf4j
@RestController
@RequestMapping("/v1/guardian")
public class GuardianResource {
	private @Autowired ResourceManagerAppMonitorScheduler scheduler;
	@Autowired private ApplicationContext appContext;
	
	@PreDestroy 
	public void shutdown () throws InterruptedException {
		scheduler.shutdown();
	}
	
	@RequestMapping("/ping")
	public ResponseEntity<?> icmpEcho(HttpServletRequest request) throws Exception {
		InputStream is = request.getInputStream();
		String content = getContent(is); 
		log.info("Returning : {} ", content);
		ResponseEntity<String> response = new ResponseEntity<String>(content, HttpStatus.OK);
		return response; 
	}
	
	@RequestMapping(method = RequestMethod.POST)
	public ResponseEntity<?> agentScheduler(HttpServletRequest request,
			@RequestParam(value="agentId") String agentId) throws Exception {
		
		// TODO What about the fluent API.
		// agent.addGroupId().addArtifactId()
		//      .addVersion().addOther().build();
		//***************************************************
		Agent agent = (Agent)appContext.getBean(agentId);
		scheduler.setAgent(agent);
		scheduler.run();	
		String content = "Agent " + agentId  + " has been scheduled to run."; 
		log.info("Returning : {} ", content);
		ResponseEntity<String> response = new ResponseEntity<String>(content, HttpStatus.OK);
		return response; 
	}	
	
	@RequestMapping(method = RequestMethod.PUT)
	public ResponseEntity<?> putRequestParamEndpoint(HttpServletRequest request,
			@RequestParam(value = "id") String id) throws Exception {
		String content = "Greetings " + id  + " from the postRequestParamEndpoint"; 
		log.info("Returning : {} ", content);
		ResponseEntity<String> response = new ResponseEntity<String>(content, HttpStatus.OK);
		return response; 
	}	
	
	@RequestMapping(method = RequestMethod.GET)
	public ResponseEntity<?> getRequestParamEndpoint(HttpServletRequest request,
			@RequestParam(value = "id") String id) throws Exception {
		String content =  id + ":" + "testValue";
		log.info("Returning {} for id {}", content, id);
		ResponseEntity<String> response = new ResponseEntity<String>(content, HttpStatus.OK);
		return response; 
	}	
	
	@RequestMapping(method = RequestMethod.DELETE)
	public ResponseEntity<?> deleteRequestParamEndpoint(HttpServletRequest request,
			@RequestParam(value = "id") String id) throws Exception {
		String content =  "Deleted " + id + ":" + "testValue";
		log.info("Returning {} for id {}", content, id);
		ResponseEntity<String> response = new ResponseEntity<String>(content, HttpStatus.OK);
		return response; 
	}	
	
	@RequestMapping("/monitor")
	public ResponseEntity<?> monitor(HttpServletRequest request,
			@RequestParam(value = "op") String op, 
			@RequestParam (value="duration") String duration) throws Exception {
		String content = "";
		if (op.equals("start")) {
			content = "Starting the monitor"; 
		}
		log.info("Returning : {} ", content);
		ResponseEntity<String> response = new ResponseEntity<String>(content, HttpStatus.OK);
		return response; 
	}	
	
	private String getContent (InputStream is) throws IOException {
		ByteArrayBuilder bab = new ByteArrayBuilder();
		int value;
		while ((value = is.read()) != -1) {
			bab.append(value);
		}
		String content = new String(bab.toByteArray());
		bab.close();
		return content; 
	}
}
