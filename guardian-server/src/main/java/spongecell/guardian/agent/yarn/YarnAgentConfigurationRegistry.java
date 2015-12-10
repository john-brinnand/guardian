package spongecell.guardian.agent.yarn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import spongecell.guardian.agent.hdfs.HDFSOutputDataValidator;
import spongecell.guardian.agent.hdfs.HDFSOutputDataValidatorRegistry;
import spongecell.guardian.agent.yarn.jobinfo.ResourceManagerMapReduceJobInfoAgent;
import spongecell.guardian.agent.yarn.jobinfo.ResourceManagerMapReduceJobInfoRegistry;
import spongecell.guardian.agent.yarn.resourcemonitor.ResourceManagerAppMonitorAgent;
import spongecell.guardian.agent.yarn.resourcemonitor.ResourceMonitorAppAgentRegistry;
import spongecell.workflow.config.framework.BeanConfigurations;
import spongecell.workflow.config.repository.BetaGenericConfigurationRepository;
import spongecell.workflow.config.repository.GenericConfigurationRepository;
import spongecell.workflow.config.repository.IGenericConfigurationRepository;


@Slf4j
@Getter
@EnableConfigurationProperties({ 
	GenericConfigurationRepository.class,
	BetaGenericConfigurationRepository.class,
	HDFSOutputDataValidatorRegistry.class,
	ResourceMonitorAppAgentRegistry.class,
	ResourceManagerMapReduceJobInfoRegistry.class
})
public class YarnAgentConfigurationRegistry implements IGenericConfigurationRepository {
	@Autowired private GenericConfigurationRepository configRepo;
	private static final String RESOURCE_MANAGER_APP_AGENT_BUILDER = "A:resourceManagerAppAgentBuilder";
	private static final String RESOURCE_MANAGER_JOBINFO_AGENT_BUILDER = "B:resourceManagerJobInfoAgentBuilder";
	private static final String HDFS_OUTPUT_VALIDATOR_BUILDER = "C:hdfsOutputDataValidatorBuilder";

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
	@Bean(name=RESOURCE_MANAGER_APP_AGENT_BUILDER)
	@BeanConfigurations(include=false)
	public ResourceManagerAppMonitorAgent buildResourceManagerAppAgent () {
		configRepo.getApplicationContext()
			.getBean(ResourceMonitorAppAgentRegistry.class);
		return new ResourceManagerAppMonitorAgent();
	}
	
	//*************************************************************
	// ResourceManager Job Info Agent 
	//*************************************************************
	@Bean(name=RESOURCE_MANAGER_JOBINFO_AGENT_BUILDER)
	@BeanConfigurations(include=false)
	public ResourceManagerMapReduceJobInfoAgent buildResourceManagerJobInfoAgent () {
		configRepo.getApplicationContext()
			.getBean(ResourceManagerMapReduceJobInfoRegistry.class);	
		return new ResourceManagerMapReduceJobInfoAgent();
	}	
		
	//*************************************************************
	// HDFS Data Validator Agent.
	//*************************************************************
	@Bean(name=HDFS_OUTPUT_VALIDATOR_BUILDER)
	@BeanConfigurations(include=false)
	public HDFSOutputDataValidator buildHdfsOutputDataValidator () {
		configRepo.getApplicationContext()
			.getBean(HDFSOutputDataValidatorRegistry.class);
		return new HDFSOutputDataValidator();
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
