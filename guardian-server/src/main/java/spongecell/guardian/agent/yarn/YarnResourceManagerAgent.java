package spongecell.guardian.agent.yarn;

import static spongecell.guardian.agent.yarn.ResourceManagerAppMonitorConfiguration.APP;
import static spongecell.guardian.agent.yarn.ResourceManagerAppMonitorConfiguration.FINAL_STATUS;
import static spongecell.guardian.agent.yarn.ResourceManagerAppMonitorConfiguration.STATE;

import java.io.IOException;
import java.time.LocalDateTime;

import javax.annotation.PostConstruct;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.kie.api.runtime.KieSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import spongecell.guardian.agent.yarn.ResourceManagerAppMonitorConfiguration.RunStates;
import spongecell.guardian.agent.yarn.exception.YarnResourceManagerException;
import spongecell.guardian.agent.yarn.model.ResourceManagerAppStatus;
import spongecell.guardian.agent.yarn.model.ResourceManagerEvent;
import spongecell.guardian.handler.KieMemoryFileSystemSessionHandler;
import spongecell.guardian.notification.SlackGuardianWebHook;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author jbrinnand
 */
@Slf4j
@Getter @Setter
@EnableConfigurationProperties({
	ResourceManagerAppMonitor.class, 
	KieMemoryFileSystemSessionHandler.class
})
public class YarnResourceManagerAgent implements Agent {
	private @Autowired ResourceManagerAppMonitor resourceManagerAppMonitor;
	private @Autowired KieMemoryFileSystemSessionHandler kieMFSessionHandler;	
	private String groupId;
	private String artifactId;
	private String version;
	private String sessionId;
	private String moduleId;
	private String [] users;
	
	@PostConstruct 
	public void init () {
		groupId = resourceManagerAppMonitor.getConfig().getGroupId();
		artifactId = resourceManagerAppMonitor.getConfig().getArtifactId();
		version = resourceManagerAppMonitor.getConfig().getVersion();
		moduleId = resourceManagerAppMonitor.getConfig().getModuleId();
		sessionId = resourceManagerAppMonitor.getConfig().getSessionId();
		users = resourceManagerAppMonitor.getConfig().getUsers();
		
		kieMFSessionHandler.newBuilder()
			.addGroupId(groupId)
			.addArtifactId(artifactId)
			.addVersion(version)
			.addModelId(moduleId)
			.addSessionId(sessionId)
			.build();
	}
	
	/**
	 * Get the status of a Yarn application. 
	 */
	@Override
	public void getStatus () {
		JsonNode appStatus = null;
		String runState = ""; 
		String finalStatus = ""; 
		int interval = 5;
		int count = 0;
		
		do {
			log.info("********** Getting the applications' status.**********");
			try {
				appStatus = resourceManagerAppMonitor
						.getResourceManagerAppStatusUser(users);
				
				runState = appStatus.get(APP).get(STATE).asText();
				finalStatus = appStatus.get(APP).get(FINAL_STATUS).asText();
				int mod = count % interval;
				
				if (runState.equals(RunStates.RUNNING.name()) && 
					finalStatus.equals(RunStates.UNDEFINED.name()) && mod == 0) {
					validateYarnMonitorRules(appStatus);
				}
				count++;
			} catch (IllegalStateException | IOException | InterruptedException e) {
				if (e instanceof InterruptedException) {
					log.info("App monitor interrupted. Exiting...");
					break;
				}
				throw new YarnResourceManagerException(
					"Failed to get the application's satus", e);
			}
		} while (runState.equals(RunStates.UNKNOWN.name()) && 
				finalStatus.equals(RunStates.UNKNOWN.name()) || 
				((runState.equals(RunStates.RUNNING.name()) && 
				finalStatus.equals(RunStates.UNDEFINED.name()))));
		try {
			validateYarnMonitorRules(appStatus);
		} catch (JsonProcessingException e) {
			log.info("ERROR - validation failure: {} ", e);
		}
	}
	
	/**
	 * Add the Yarn application's status to the 
	 * ResourceManagerAppStatus. Send it, as a Fact to the 
	 * Rules Engine.
	 *  
	 * @param appStatus
	 * @throws JsonProcessingException
	 */
	private void validateYarnMonitorRules(JsonNode appStatus)
			throws JsonProcessingException {
		log.info("appStatus is: {} ", new ObjectMapper()
			.writerWithDefaultPrettyPrinter()
			.writeValueAsString(appStatus));
		
		//***************************************************
		// Add this fact to a KieSession and fire it up.
		//***************************************************
		ResourceManagerAppStatus resourceManagerAppStatus = 
				ResourceManagerAppStatus.instance();
		resourceManagerAppStatus.setAppStatus(appStatus);
		resourceManagerAppStatus.setActive(true);
		
		ResourceManagerEvent event = new ResourceManagerEvent() ;
		event.dateTime = LocalDateTime.now( ).toString();
		event.setEventSeverity(ResourceManagerEvent.severity.INFORMATIONAL.name());
		
		SlackGuardianWebHook slackClient = new SlackGuardianWebHook();
		
		KieSession kieSession = kieMFSessionHandler.getRepositorySession(
				groupId, artifactId, version, sessionId);
		
		Object[] facts = { resourceManagerAppStatus, event, slackClient };
		for (Object fact : facts) {
			kieSession.insert(fact);
		}
		kieSession.fireAllRules();
		kieSession.dispose();
	}	
	
	@Bean(name="yarnResourceManagerAgent")
	public Agent buildAgent() {
		return new YarnResourceManagerAgent();
	}
}
