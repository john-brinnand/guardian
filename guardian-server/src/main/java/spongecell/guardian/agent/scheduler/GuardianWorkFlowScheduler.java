package spongecell.guardian.agent.scheduler;

import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.util.Assert;

import spongecell.guardian.agent.workflow.IAgentWorkFlow;

@Slf4j
@Getter @Setter
@EnableConfigurationProperties({ GuardianWorkFlowSchedulerConfiguration.class })
public class GuardianWorkFlowScheduler {
	@Autowired GuardianWorkFlowSchedulerConfiguration config;
	private ExecutorService pool;
	private Future<?> future; 
	
	public GuardianWorkFlowScheduler () {}
	
	@PostConstruct
	public void init () {
		pool = Executors.newFixedThreadPool(config.getNumThreads());
	}

	/**
	 * Start the data load.
	 * 
	 * @return
	 * @throws TimeoutException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public Future<?> run(IAgentWorkFlow agentWorkFlow) throws TimeoutException, 
		InterruptedException, ExecutionException {
		
		future = pool.submit(new Runnable() {
			@Override
			public void run() {
				while (true) {
					final long endTime;
					final long startTime = System.currentTimeMillis();
					try {
						agentWorkFlow.execute();
					} catch (URISyntaxException e1) {
						log.error("ERROR - workflow failed to complete: {} ", e1);
					}
					endTime = System.currentTimeMillis();
					log.info("------------------  Agent action completed  in {} {} ", 
							endTime - startTime, TimeUnit.MILLISECONDS.toString().toLowerCase());
					try {
						Thread.sleep(config.getWaitTime());
					} catch (InterruptedException e) {
						log.info("Error - thread interrupted: {} ", e.toString());
					}
				}
			}
		});
		return future;
	}

	/**
	 * Shutdown the executor.
	 * 
	 * @throws InterruptedException
	 */
	public void shutdown() throws InterruptedException {
		future.cancel(true);
		pool.shutdown();
		pool.awaitTermination(5000, TimeUnit.MILLISECONDS);
		if (!pool.isShutdown()) {
			log.info("Pool is not shutdown.");
			pool.shutdownNow();
		}
		Assert.isTrue(pool.isShutdown());
		log.info("Pool shutdown status : {}", pool.isShutdown());
	}
}
