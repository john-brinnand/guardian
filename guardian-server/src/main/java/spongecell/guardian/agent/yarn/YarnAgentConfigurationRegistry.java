package spongecell.guardian.agent.yarn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;

import lombok.Getter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;

import spongecell.datasource.airstream.framework.BeanConfigurations;
import spongecell.guardian.agent.hdfs.HDFSOutputDataValidator;
import spongecell.webhdfs.WebHdfsConfiguration;
import spongecell.webhdfs.WebHdfsWorkFlow;
import spongecell.webhdfs.WebHdfsWorkFlow.Builder;
import spongecell.workflow.config.repository.GenericConfigurationRepository;
import spongecell.workflow.config.repository.IGenericConfigurationRepository;


@Getter
@EnableConfigurationProperties({ GenericConfigurationRepository.class })
public class YarnAgentConfigurationRegistry implements IGenericConfigurationRepository {
	@Autowired private GenericConfigurationRepository configRepo;

	public YarnAgentConfigurationRegistry() { }
	
	@PostConstruct
	public void init() {
		configRepo.addBeans(getClass());
	}

	@Override
	public Iterator<Object> iterator() {
		return configRepo.iterator();
	}
	
	public Iterator<Entry<String, String>> mapIterator() {
		return configRepo.mapIterator();
	}	
	
	public Entry<String, String> getEntry(String name) {
		return configRepo.getEntry(name);
	}
	
	@Override
	public Iterator<Entry<String, ArrayList<String>>> agentIterator() {
		return configRepo.agentIterator();
	}	

	//*************************************************************
	// Resource Manager Application Monitor Agent.
	//*************************************************************
	@Bean(name=ResourceManagerAppMonitorAgent.BEAN_NAME)
	@DependsOn(value={ ResourceManagerAppMonitorAgent.BEAN_CONFIG_NAME })
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
		
	//*************************************************************
	// HDFS Data Validator Agent.
	//*************************************************************
	@Bean(name=HDFSOutputDataValidator.BEAN_NAME)
	@DependsOn(value={ 
		HDFSOutputDataValidator.WEBHDFS_BEAN_NAME, 
		HDFSOutputDataValidator.WEBHDFS_WORKFLOW_BEAN_NAME
	})
	@ConfigurationProperties(prefix=HDFSOutputDataValidator.BEAN_CONFIG_PROPS_PREFIX)
	public HDFSOutputDataValidator buildHdfsOutputDataValidator () {
		return new HDFSOutputDataValidator(configRepo);
	}	
	
	@Bean(name=HDFSOutputDataValidator.WEBHDFS_WORKFLOW_BEAN_NAME)
	@ConfigurationProperties(prefix=
		HDFSOutputDataValidator.WEBHDFS_WORKFLOW_CONFIG_PREFIX)
	@BeanConfigurations(include=false)
	public WebHdfsWorkFlow.Builder buildWebHdfsWorkFlow() {
		return new WebHdfsWorkFlow.Builder();
	}
	
	@Bean(name=HDFSOutputDataValidator.WEBHDFS_BEAN_NAME)
	@ConfigurationProperties(prefix=
		HDFSOutputDataValidator.WEBHDFS_BEAN_CONFIG_PROPS_PREFIX)
	@BeanConfigurations(parent=HDFSOutputDataValidator.BEAN_NAME)
	public WebHdfsConfiguration buildWebHdfsConfig() {
		return new WebHdfsConfiguration();
	}
	
	@Override
	public <T> T getAgent(String agentId) {
		return configRepo.getAgent(agentId);
	}

	@Override
	public ApplicationContext getApplicationContext() {
		return configRepo.getApplicationContext();
	}	
}
