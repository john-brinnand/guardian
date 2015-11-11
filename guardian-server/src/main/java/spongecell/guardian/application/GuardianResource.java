package spongecell.guardian.application;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import javax.annotation.PreDestroy;
import javax.servlet.http.HttpServletRequest;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import spongecell.guardian.agent.exception.GuardianWorkFlowException;
import spongecell.guardian.agent.scheduler.GuardianWorkFlowScheduler;
import spongecell.guardian.agent.workflow.IAgentWorkFlow;
import spongecell.guardian.agent.yarn.Agent;
import spongecell.guardian.agent.yarn.ResourceManagerAppMonitorScheduler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
@RestController
@RequestMapping("/v1/guardian")
@EnableConfigurationProperties({GuardianWorkFlowScheduler.class})
public class GuardianResource {
	@Autowired private GuardianWorkFlowScheduler scheduler;
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
	public ResponseEntity<?> agentSchedulerBuilder(
		HttpServletRequest request,
		@RequestParam(value="agentId") String agentId) throws Exception {
		
		// TODO What about the fluent API.
		// agent.addGroupId().addArtifactId()
		//      .addVersion().addOther().build();
		//****************************************
		String body = getContent(request.getInputStream());
		IAgentWorkFlow workFlow = buildWorkFlow(body);
		scheduler.run(workFlow);
		
		String content = "WorkFlow " + workFlow.getId()  + " has been scheduled to run."; 
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
		Scanner s = new Scanner(is);
		StringBuffer buf = new StringBuffer();
		while (s.hasNext()) {
			buf.append(s.next());
		}
		s.close();
		log.info(buf.toString());
		return buf.toString(); 
	}
	
	private IAgentWorkFlow buildWorkFlow(String body) {
		ObjectMapper om = new ObjectMapper();
		IAgentWorkFlow workFlow = null; 
		try {
			JsonNode node = om.readTree(body);
			String workFlowId = node.get("workFlow").get("workFlowId").asText(); 
			workFlow = (IAgentWorkFlow) appContext.getBean(workFlowId);
			workFlow.setId(workFlowId);
			
			String[] agentIds = node.get("workFlow").get("agentIds").asText().split(","); 
			int count = 0;
			String step = "step";
			for (String agentId : agentIds) {
				Agent agent = (Agent) appContext.getBean(agentId);
				step = step + count;
				workFlow.addEntry(step, agent);
				count++;
			}	
		} catch (IOException e) {
			throw new GuardianWorkFlowException("ERROR building workflow: ", e);
		} 
		return workFlow;
	}
}
