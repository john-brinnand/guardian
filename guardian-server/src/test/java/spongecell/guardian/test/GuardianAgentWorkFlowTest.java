package spongecell.guardian.test;

import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import lombok.extern.slf4j.Slf4j;

import org.codehaus.jackson.node.JsonNodeFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import spongecell.guardian.agent.exception.GuardianWorkFlowException;
import spongecell.guardian.agent.hdfs.HDFSFileListAgent;
import spongecell.guardian.agent.scheduler.GuardianWorkFlowScheduler;
import spongecell.guardian.agent.workflow.GuardianAgentWorkFlow;
import spongecell.guardian.agent.yarn.Agent;
import spongecell.guardian.agent.yarn.YarnResourceManagerAgent;

/**
 * @author jbrinnand
 */
@Slf4j
@ContextConfiguration(classes = { 
	GuardianAgentWorkFlowTest.class, 
	GuardianAgentWorkFlow.class,
	YarnResourceManagerAgent.class,
	GuardianWorkFlowScheduler.class,
	HDFSFileListAgent.class
})
public class GuardianAgentWorkFlowTest extends AbstractTestNGSpringContextTests{
	private @Autowired GuardianAgentWorkFlow workFlow;
	private @Autowired GuardianWorkFlowScheduler scheduler;
	private static final String YARN_MONITOR_DRL = "yarn-monitor.drl";
	private static final String BASE_PATH = "src/main/resources";
	private static final String RULES_PATH = "spongecell/guardian/rules/core/yarn";
	private static final String GROUP_ID = "spongecell";
	private static final String ARTIFACT_ID = "heston-alpha-agent";
	private static final String VERSION_ID = "0.0.1-SNAPSHOT";
	private static final String MODULE_ID = "heston-alpha-module";
	private static final String SESSION_ID = "heston-alpha-session";
	private final String agentId = "yarnResourceManagerAgent";
	private @Autowired ApplicationContext ctx;
	
	@BeforeClass
	public void init () {
		Assert.assertNotNull(workFlow.getKieMFSessionHandler());
		log.info("Building the session.");
		
		// Simulate how the guardian resource
		// will build rules.
		//************************************
		workFlow.newSessionBuilder()
			.groupId(GROUP_ID)
			.artifactId(ARTIFACT_ID)
			.versionId(VERSION_ID)
			.moduleId(MODULE_ID)
			.sessionId(SESSION_ID)
			.build();		
	}

	@Test
	public void validateGuardianAgentWorkFlowConfig() {
		log.info("Here.");
		Assert.assertEquals(workFlow.getKieMFSessionHandler()
			.getRules()[0], YARN_MONITOR_DRL);
		Assert.assertEquals(workFlow.getKieMFSessionHandler()
			.getBasePath(), BASE_PATH);
		Assert.assertEquals(workFlow.getKieMFSessionHandler()
			.getRulesPath(), RULES_PATH);
		Assert.assertEquals(workFlow.getKieMFSessionHandler()
			.getSessionId(), SESSION_ID);
		Assert.assertEquals(workFlow.getKieMFSessionHandler()
			.getArtifactId(), ARTIFACT_ID);
		Assert.assertEquals(workFlow.getKieMFSessionHandler()
			.getModuleId(), MODULE_ID);
	}

	@Test
	public void validateGuardianAgentWorkFlowRules() throws URISyntaxException {
		log.info("Validating the agent work flow's rules.");
		final String stepOne = "MonitorYarnApp";
		Agent agent = (Agent) ctx.getBean(agentId);
		workFlow.addEntry(stepOne, agent);
		workFlow.execute();
	}

	@Test
	public void validateGuardianAgentWorkFlowScheduler()
			throws URISyntaxException, InterruptedException {
		final String stepOne = "MonitorYarnApp";
		Agent agent = (Agent) ctx.getBean(agentId);
		workFlow.addEntry(stepOne, agent);
		try {
			scheduler.run(workFlow);
		} catch (TimeoutException | InterruptedException | ExecutionException e) {
			throw new GuardianWorkFlowException("Agent WorkFlow failure", e);
		}
		Thread.sleep(90000);
	}

	@Test
	public void validateGuardianAgentWorkFlowSchedulerMultiAgent()
			throws URISyntaxException, InterruptedException {
		final String[] agentIds = { "yarnResourceManagerAgent",
				"hdfsListDirectoryAgent" };
		int count = 1;
		String step = "step";
		for (String agentId : agentIds) {
			Agent agent = (Agent) ctx.getBean(agentId);
			step = step + count;
			workFlow.addEntry(step, agent);
			count++;
		}
		try {
			scheduler.run(workFlow);
		} catch (TimeoutException | InterruptedException | ExecutionException e) {
			throw new GuardianWorkFlowException("Agent WorkFlow failure", e);
		}
		Thread.sleep(90000);
	}

	@Test
	public void validateGuardianAgentWorkFlowSchedulerMultiAgentProperties()
			throws URISyntaxException, InterruptedException, JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();
		node.with("workFlow").put("name", "heston-workflow");
		node.with("workFlow").with("Step-1").with("yarnResourceManagerAgent");
		node.with("workFlow").with("Step-2").with("hdfsListDirectoryAgent")
			.put("outputPath", "/data/raw/analytics");
		node.with("workFlow").with("Step-3").with("hdfsListDirectoryAgent")
			.put("inputPath", "/data/extract")
			.put("outputPath", "/data/extract");
		log.info("WorkFlow config is: {} ", mapper
			.writerWithDefaultPrettyPrinter()
			.writeValueAsString(node)); 
		
		printNodes(node, mapper);
	}
	
	private void printNodes(JsonNode node, ObjectMapper mapper) throws JsonProcessingException {
		Iterator<JsonNode> iter = node.iterator();
		while (iter.hasNext()) {
			JsonNode next = iter.next();
			log.info(" ------------------------------------- ");
			log.info("Configuration node is: {} ", 
				mapper.writerWithDefaultPrettyPrinter()
				.writeValueAsString(next));
			if (!next.isNull()) {
				printNodes(next, mapper);
			}
		}
	}
}	