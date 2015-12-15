package spongecell.guardian.application;

import static spongecell.guardian.agent.workflow.GuardianAgentWorkFlowKeys.CREATE;
import static spongecell.guardian.agent.workflow.GuardianAgentWorkFlowKeys.OP;
import static spongecell.guardian.agent.workflow.GuardianAgentWorkFlowKeys.REGISTRY_CLAZZ_NAME;
import static spongecell.guardian.agent.workflow.GuardianAgentWorkFlowKeys.WORKFLOW;
import static spongecell.guardian.agent.workflow.GuardianAgentWorkFlowKeys.WORKFLOW_CLAZZ_NAME;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Scanner;

import javax.annotation.PreDestroy;
import javax.servlet.http.HttpServletRequest;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.BeansException;
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
import spongecell.guardian.agent.workflow.GuardianAgentWorkFlowKeys.STATUS;
import spongecell.guardian.agent.workflow.IAgentWorkFlow;
import spongecell.guardian.agent.yarn.Agent;
import spongecell.guardian.configuration.repository.IGenericConfigurationRepository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
@RestController
@RequestMapping("/v1/guardian")
@EnableConfigurationProperties({ 
	GuardianWorkFlowScheduler.class,
})
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
		@RequestParam(value=OP) String op) throws Exception {
		
		// TODO What about the fluent API.
		// agent.addGroupId().addArtifactId()
		//      .addVersion().addOther().build();
		//****************************************	
		String content = ""; 
		if (op.equals(CREATE)) {
			String body = getContent(request.getInputStream());
			IAgentWorkFlow workFlow = buildWorkFlow(body);
			scheduler.run(workFlow);
			content = "WorkFlow " + workFlow.getId()  + " " + STATUS.STARTED.name(); 
			log.info("Returning : {} ", content);
		}
		ResponseEntity<String> response = new ResponseEntity<String>(
			content, HttpStatus.OK);
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
			@RequestParam(value = OP) String op, 
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
	
	/**
	 * Build a workflow.
	 * 
	 * @param body
	 * @return
	 */
	private IAgentWorkFlow buildWorkFlow(String body) {
		IGenericConfigurationRepository repo =  null;
		IAgentWorkFlow workFlow = null;
		ObjectMapper om = new ObjectMapper();
		JsonNode node;
		try {
			node = om.readTree(body);
		} catch (IOException e1) {
			throw new GuardianWorkFlowException("ERROR - workflow was not built: ", e1);
		}
		String registryClazz = node.get(WORKFLOW).get(REGISTRY_CLAZZ_NAME).asText(); 
		String workFlowClazz = node.get(WORKFLOW).get(WORKFLOW_CLAZZ_NAME).asText(); 

		try {
			repo = (IGenericConfigurationRepository) appContext.getBean(
					Class.forName(registryClazz).newInstance().getClass());
			workFlow = (IAgentWorkFlow) appContext.getBean(workFlowClazz);
			
			Iterator<Entry<String, ArrayList<String>>> entries = repo.agentIterator();
			int stepCount = 1;
			while (entries.hasNext()) {
				Entry<String, ArrayList<String>> entry = entries.next();
				log.info("Entry - name {}, value {}", 
						entry.getKey(), entry.getValue());
				Agent agent = (Agent) appContext.getBean(entry.getKey());
				workFlow.addEntry("step" + stepCount, agent);
				stepCount++;
			}	
		} catch (BeansException | InstantiationException
				| IllegalAccessException | ClassNotFoundException e) {
			log.info("ERROR - creating workflow");
			throw new GuardianWorkFlowException("ERROR - workflow not created: ", e);
		}
		return workFlow;		
	}
}
