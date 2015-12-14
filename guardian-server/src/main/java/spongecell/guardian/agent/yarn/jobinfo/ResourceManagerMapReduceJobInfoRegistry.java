package spongecell.guardian.agent.yarn.jobinfo;

import javax.annotation.PostConstruct;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;

import spongecell.guardian.agent.yarn.resourcemonitor.ResourceManagerAppMonitorConfiguration;
import spongecell.guardian.configuration.repository.GenericConfigurationRepository;
import spongecell.webhdfs.WebHdfsConfiguration;
import spongecell.webhdfs.WebHdfsWorkFlow;
import spongecell.webhdfs.WebHdfsWorkFlow.Builder;
import spongecell.workflow.config.framework.BeanConfigurations;

@Slf4j
@Getter @Setter
@EnableConfigurationProperties(GenericConfigurationRepository.class)
public class ResourceManagerMapReduceJobInfoRegistry {
	private @Autowired GenericConfigurationRepository configRepo;

	public ResourceManagerMapReduceJobInfoRegistry() { }

	public ResourceManagerMapReduceJobInfoRegistry (
			GenericConfigurationRepository configRepo1) {
	}
	
	@PostConstruct
	public void init() {
		log.info("Initializing the registry.");
		configRepo.addRegistryBeans(getClass());
	}

	//*************************************************************
	// ResourceManager Job Info Agent 
	//*************************************************************
	@Bean(name=ResourceManagerMapReduceJobInfoAgent.BEAN_NAME)
	@DependsOn(value={ 
		ResourceManagerMapReduceJobInfoAgent.BEAN_CONFIG_NAME,
		ResourceManagerMapReduceJobInfoAgent.BEAN_WORKFLOW_BUILDER_NAME,
		ResourceManagerMapReduceJobInfoAgent.BEAN_WEBHDFS_CONFIG_NAME,
	})
	public ResourceManagerMapReduceJobInfoAgent buildResourceManagerJobInfoAgent () {
		return new ResourceManagerMapReduceJobInfoAgent(configRepo);
	}	
	
	@Bean(name=ResourceManagerMapReduceJobInfoAgent.BEAN_CONFIG_NAME)
	@ConfigurationProperties(prefix=
		ResourceManagerMapReduceJobInfoAgent.BEAN_CONFIG_PREFIX)
	@BeanConfigurations(parent=ResourceManagerMapReduceJobInfoAgent.BEAN_NAME)
	public ResourceManagerAppMonitorConfiguration buildResourceManagerJobInfoAppConfig() {
		return new ResourceManagerAppMonitorConfiguration();
	}		
	
	@Bean(name=ResourceManagerMapReduceJobInfoAgent.BEAN_WORKFLOW_BUILDER_NAME)
	@ConfigurationProperties(prefix=
		ResourceManagerMapReduceJobInfoAgent.BEAN_WORKFLOW_BUILDER_CONFIG_PREFIX)
	@BeanConfigurations(parent=ResourceManagerMapReduceJobInfoAgent.BEAN_NAME)
	public Builder buildResourceManagerWorkFlowBuilder() {
		return new WebHdfsWorkFlow.Builder();
	}	
	
	@Bean(name=ResourceManagerMapReduceJobInfoAgent.BEAN_WEBHDFS_CONFIG_NAME)
	@ConfigurationProperties(prefix=
		ResourceManagerMapReduceJobInfoAgent.BEAN_WEBHDFS_CONFIG_PREFIX)
	@BeanConfigurations(parent=ResourceManagerMapReduceJobInfoAgent.BEAN_NAME)
	public WebHdfsConfiguration buildResourceManagerJobInfoConfig() {
		return new WebHdfsConfiguration();
	}	
}
