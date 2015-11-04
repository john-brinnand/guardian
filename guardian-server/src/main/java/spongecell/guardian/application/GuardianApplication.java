package spongecell.guardian.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import spongecell.guardian.agent.yarn.ResourceManagerAppMonitor;
import spongecell.guardian.agent.yarn.ResourceManagerAppMonitorScheduler;
import spongecell.guardian.agent.yarn.YarnResourceManagerAgent;
import spongecell.guardian.handler.KieMemoryFileSystemSessionHandler;


@SpringBootApplication
@EnableAutoConfiguration
@EnableConfigurationProperties({
	GuardianResourceConfiguration.class, 
	ResourceManagerAppMonitorScheduler.class,
	YarnResourceManagerAgent.class,
	ResourceManagerAppMonitor.class, 
	KieMemoryFileSystemSessionHandler.class
})
@EnableWebMvc
public class GuardianApplication {
    public static void main(String[] args) {
        SpringApplication.run(GuardianApplication.class, args);
    }	
}
