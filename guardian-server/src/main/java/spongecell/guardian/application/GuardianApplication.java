package spongecell.guardian.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import spongecell.guardian.agent.workflow.GuardianAgentWorkFlow;
import spongecell.guardian.agent.yarn.YarnAgentConfigurationRegistry;


@SpringBootApplication
@EnableAutoConfiguration
@EnableConfigurationProperties({
	YarnAgentConfigurationRegistry.class,
	GuardianResourceConfiguration.class,
})
@EnableWebMvc
public class GuardianApplication {
    public static void main(String[] args) {
        SpringApplication.run(GuardianApplication.class, args);
    }	
}
