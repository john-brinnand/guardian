package spongecell.guardian.test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import spongecell.guardian.agent.yarn.ResourceManagerAppMonitorScheduler;
import spongecell.guardian.agent.yarn.YarnResourceManagerAgent;
import spongecell.guardian.agent.yarn.exception.YarnResourceManagerException;

/**
 * @author jbrinnand
 */
@Slf4j
@ContextConfiguration(classes = { ResourceManagerSchedulerYarnAgentTest.class})
@EnableConfigurationProperties ({ 
	ResourceManagerAppMonitorScheduler.class,
	YarnResourceManagerAgent.class
})
public class ResourceManagerSchedulerYarnAgentTest extends AbstractTestNGSpringContextTests{
	private @Autowired ResourceManagerAppMonitorScheduler scheduler;
	private @Autowired YarnResourceManagerAgent agent;


	@Test
	public void validateScheduler() throws InterruptedException,
			TimeoutException, ExecutionException {
		scheduler.setAgent(agent);
		scheduler.run();
		Thread.sleep(10000);
		scheduler.shutdown();
		log.info("Executor service shutdown status is: {}", scheduler.getPool().isShutdown());
	}	
	
	/**
	 * Note: this test requires that the wordcount map-reduce
	 * job is running on the local cluster. And this in turn
	 * requires data for the map-reduce job to consume. 
	 */
	@Test
	public void validateYarnResourceManagerAgent() {
		try {
			scheduler.setAgent(agent);
			scheduler.run();
			Thread.sleep(90000);
			scheduler.shutdown();
			log.info("Executor service shutdown status is: {}", 
					scheduler.getPool().isShutdown());		
		} catch (TimeoutException | InterruptedException | ExecutionException e) {
			throw new YarnResourceManagerException("Scheduler error", e);
		}
	}	
}	