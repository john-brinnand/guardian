package spongecell.guardian.test;

import static spongecell.guardian.agent.yarn.ResourceManagerAppMonitorConfiguration.APP;
import static spongecell.guardian.agent.yarn.ResourceManagerAppMonitorConfiguration.FINAL_STATUS;
import static spongecell.guardian.agent.yarn.ResourceManagerAppMonitorConfiguration.STATE;

import java.io.IOException;
import java.time.LocalDateTime;

import lombok.extern.slf4j.Slf4j;

import org.kie.api.runtime.KieSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import spongecell.guardian.agent.yarn.ResourceManagerAppMonitor;
import spongecell.guardian.agent.yarn.ResourceManagerAppMonitorConfiguration.RunStates;
import spongecell.guardian.agent.yarn.YarnResourceManagerAgent;
import spongecell.guardian.agent.yarn.model.ResourceManagerAppStatus;
import spongecell.guardian.agent.yarn.model.ResourceManagerEvent;
import spongecell.guardian.handler.KieMemoryFileSystemSessionHandler;
import spongecell.guardian.notification.SlackGuardianWebHook;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
@ContextConfiguration(classes = { 
	ResourceManagerAppMonitorTest.class,
	ResourceManagerAppMonitor.class, 
	KieMemoryFileSystemSessionHandler.class	
})
@EnableConfigurationProperties({})
public class ResourceManagerAppMonitorTest extends AbstractTestNGSpringContextTests {
	private @Autowired ResourceManagerAppMonitor resourceManagerAppMonitor;
	private @Autowired KieMemoryFileSystemSessionHandler kieMFSessionHandler;	
	private KieSession kieSession;	
	private static final String WORDCOUNT = "word count";
	private static final String groupId = "spongecell";
	private static final String artifactId = "yarn-monitor";
	private static final String version = "0.0.1-SNAPSHOT";
	private static final String moduleId = "yarn-monitor-module-v1";
	private static final String sessionId = "yarn-session-v1";
	private static final String [] USERS = { "spongecell", "heston", "root"} ;
	
	@BeforeClass
	public void initKieMFSessionHandler() {
		kieMFSessionHandler.newBuilder()
			.addGroupId(groupId)
			.addArtifactId(artifactId)
			.addVersion(version)
			.addModelId(moduleId)
			.addSessionId(sessionId)
			.build();
	}

	@Test
	public void validateAppMonitorConfiguration() {
		final String CLUSTER = "ws/v1/cluster";
		final String ENDPOINT = "apps";
		final String SCHEME = "http";
		final String HOST = "dockerhadoop";
		final int PORT = 8088;

		String cluster = resourceManagerAppMonitor.getConfig().getCluster();
		Assert.assertNotNull(cluster);
		Assert.assertEquals(cluster, CLUSTER);

		String endPoint = resourceManagerAppMonitor.getConfig().getEndpoint();
		Assert.assertNotNull(endPoint);
		Assert.assertEquals(endPoint, ENDPOINT);

		String scheme = resourceManagerAppMonitor.getConfig().getScheme();
		Assert.assertNotNull(scheme);
		Assert.assertEquals(scheme, SCHEME);

		String host = resourceManagerAppMonitor.getConfig().getHost();
		Assert.assertNotNull(host);
		Assert.assertEquals(host, HOST);

		int port = resourceManagerAppMonitor.getConfig().getPort();
		Assert.assertNotNull(port);
		Assert.assertEquals(port, PORT);
	}

	/**
	 * Note: this test requires the wordcount test to be runnning in the local
	 * hadoop cluster within the docker container.
	 * 
	 * @throws IllegalStateException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void validateResourceManagerApps() throws IllegalStateException,
			IOException, InterruptedException {
		JsonNode appStatus = null;
		String runState = ""; 
		String finalStatus = ""; 
		boolean validation = false; 
		do {
			log.info("*************** Getting the applications' status.***************");
			
			appStatus = resourceManagerAppMonitor
					.getResourceManagerAppStatus(WORDCOUNT);
			Assert.assertNotNull(appStatus);
			
			runState = appStatus.get(APP).get(STATE).asText();
			finalStatus = appStatus.get(APP).get(FINAL_STATUS).asText();
			
			if (runState.equals(RunStates.RUNNING.name()) && 
				finalStatus.equals(RunStates.UNDEFINED.name()) && 
				validation == false) {
				validateYarnMonitorRules(appStatus);
				validation = true;
			}
		} while (runState.equals(RunStates.UNKNOWN.name()) && 
				finalStatus.equals(RunStates.UNKNOWN.name()) || 
				((runState.equals(RunStates.RUNNING.name()) && 
				finalStatus.equals(RunStates.UNDEFINED.name()))));

		Assert.assertEquals(appStatus.get(APP).get(STATE).asText(),
				RunStates.FINISHED.name());
		Assert.assertEquals(appStatus.get(APP).get(FINAL_STATUS).asText(),
				RunStates.SUCCEEDED.name());
		
		validateYarnMonitorRules(appStatus);
	}
	
	
	/**
	 * Note: this test requires the wordcount test to be runnning in the local
	 * hadoop cluster within the docker container.
	 * 
	 * @throws IllegalStateException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void validateResourceManagerAppMonitorUsers() throws IllegalStateException,
			IOException, InterruptedException {
		JsonNode appStatus = null;
		String runState = ""; 
		String finalStatus = ""; 
		boolean validation = false; 
		do {
			log.info("*************** Getting the applications' status.***************");
			
			appStatus = resourceManagerAppMonitor
					.getResourceManagerAppStatusUser(USERS);
			Assert.assertNotNull(appStatus);
			
			runState = appStatus.get(APP).get(STATE).asText();
			finalStatus = appStatus.get(APP).get(FINAL_STATUS).asText();
			
			if (runState.equals(RunStates.RUNNING.name()) && 
				finalStatus.equals(RunStates.UNDEFINED.name()) && 
				validation == false) {
				validateYarnMonitorRules(appStatus);
				validation = true;
			}
		} while (runState.equals(RunStates.UNKNOWN.name()) && 
				finalStatus.equals(RunStates.UNKNOWN.name()) || 
				((runState.equals(RunStates.RUNNING.name()) && 
				finalStatus.equals(RunStates.UNDEFINED.name()))));

		Assert.assertEquals(appStatus.get(APP).get(STATE).asText(),
				RunStates.FINISHED.name());
		Assert.assertEquals(appStatus.get(APP).get(FINAL_STATUS).asText(),
				RunStates.SUCCEEDED.name());
		
		validateYarnMonitorRules(appStatus);
	}

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
		
		kieSession = kieMFSessionHandler.getRepositorySession(
				groupId, artifactId, version, sessionId);
		
		Object[] facts = { resourceManagerAppStatus, event, slackClient };
		for (Object fact : facts) {
			kieSession.insert(fact);
		}
		kieSession.fireAllRules();
	}
}