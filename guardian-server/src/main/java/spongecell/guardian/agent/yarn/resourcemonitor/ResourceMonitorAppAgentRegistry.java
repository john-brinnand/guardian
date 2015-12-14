package spongecell.guardian.agent.yarn.resourcemonitor;

import javax.annotation.PostConstruct;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;

import spongecell.guardian.configuration.repository.GenericConfigurationRepository;
import spongecell.workflow.config.framework.BeanConfigurations;

@Slf4j
@Getter @Setter
@EnableConfigurationProperties(GenericConfigurationRepository.class)
public class ResourceMonitorAppAgentRegistry {
	private @Autowired GenericConfigurationRepository configRepo;

	public ResourceMonitorAppAgentRegistry() { }

	public ResourceMonitorAppAgentRegistry (
			GenericConfigurationRepository configRepo1) {
	}
	
	@PostConstruct
	public void init() {
		log.info("Initializing the registry.");
		configRepo.addRegistryBeans(getClass());
	}

	//*************************************************************
	// Resource Manager Application Monitor Agent.
	//*************************************************************
	@Bean(name=ResourceManagerAppMonitorAgent.BEAN_NAME)
	@DependsOn(value={ 
		ResourceManagerAppMonitorAgent.BEAN_CONFIG_NAME
	})
	public ResourceManagerAppMonitorAgent buildResourceManagerAppAgent () {
		return new ResourceManagerAppMonitorAgent(configRepo);
	}	
	
	@Bean(name=ResourceManagerAppMonitorAgent.BEAN_CONFIG_NAME)
	@ConfigurationProperties(prefix=
		ResourceManagerAppMonitorAgent.BEAN_CONFIG_PREFIX)
	@BeanConfigurations(parent=ResourceManagerAppMonitorAgent.BEAN_NAME)
	public ResourceManagerAppMonitorConfiguration buildResourceManagerAppAgentConfiguration() {
		return new ResourceManagerAppMonitorConfiguration();
	}			
}
