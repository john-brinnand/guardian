package spongecell.guardian.agent.yarn;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.util.Assert;

@Slf4j
@Getter @Setter
@EnableConfigurationProperties({ ResourceManagerAppMonitorSchedulerConfiguration.class })
public class ResourceManagerAppMonitorScheduler {
	@Autowired ResourceManagerAppMonitorSchedulerConfiguration config;
	private final ExecutorService pool = Executors.newScheduledThreadPool(1);
	private Future<?> future; 
	private Agent agent;

	/**
	 * Start the data load.
	 * 
	 * @return
	 * @throws TimeoutException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public Future<?> run() throws TimeoutException, 
		InterruptedException, ExecutionException {
		
		// Note: the topic and groupId must be set in 
		// the environment. Otherwise default properties
		// will be in effect and data may not be received
		// from Kafka.
		//************************************************
		future = pool.submit(new Runnable() {
			@Override
			public void run() {
				while (true) {
					final long endTime;
					final long startTime = System.currentTimeMillis();
					agent.getStatus();
					endTime = System.currentTimeMillis();
					log.info("------------------  Agent action completed  in {} {} ", 
							endTime - startTime, TimeUnit.MILLISECONDS.toString().toLowerCase());
					try {
						Thread.sleep(5000);
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
