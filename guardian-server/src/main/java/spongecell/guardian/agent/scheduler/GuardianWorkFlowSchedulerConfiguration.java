package spongecell.guardian.agent.scheduler;

import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.Setter;

import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter @Setter
@ConfigurationProperties(prefix ="guardian.scheduler")
public class GuardianWorkFlowSchedulerConfiguration {
	public Integer waitTime = 5000;
	public Integer numThreads = 20;
	public TimeUnit timeUnit = TimeUnit.MILLISECONDS;
}
