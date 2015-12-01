package spongecell.guardian.agent.yarn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import lombok.extern.slf4j.Slf4j;

import org.springframework.context.ApplicationContext;

import spongecell.workflow.config.repository.IGenericConfigurationRepository;

@Slf4j
public class ResourceManagerMapReduceJobInfoAgent implements Agent {
	private ResourceManagerAppMonitorConfiguration config;
	public static final String BEAN_NAME = "resourceManagerMapReduceJobInfoAgent";
	public static final String BEAN_CONFIG_NAME = "resourceManagerMapReduceJobInfoConfig";
	public static final String BEAN_CONFIG_PREFIX = ResourceManagerAppMonitorAgent.BEAN_CONFIG_PREFIX; 

	public ResourceManagerMapReduceJobInfoAgent() { }
	
	public ResourceManagerMapReduceJobInfoAgent (IGenericConfigurationRepository repo) {
		Iterator<Entry<String, ArrayList<String>>> entries = repo.agentIterator();
		while (entries.hasNext()) {
			Entry<String, ArrayList<String>> entry = entries.next();
			if (entry.getKey().equals(BEAN_NAME)) {
				log.info("Building agent: {} ", entry.getKey());
				buildAgent(entry, repo.getApplicationContext());
			}
		}		
	}
	
	public void buildAgent(Entry<String, ArrayList<String>> agentEntry,
			ApplicationContext ctx) { 
		ArrayList<String> configIds = agentEntry.getValue();
		for (String configId : configIds) {
			if (configId.equals(BEAN_CONFIG_PREFIX)) {
				config = (ResourceManagerAppMonitorConfiguration) ctx.getBean(BEAN_CONFIG_NAME);
				log.info(config.toString());
			}
		} 
	}	
	
	@Override
	public Object[] getStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object[] getStatus(Object[] args) {
		// TODO Auto-generated method stub
		return new Object[1];
	}

	@Override
	public Agent buildAgent() {
		// TODO Auto-generated method stub
		return null;
	}

}
