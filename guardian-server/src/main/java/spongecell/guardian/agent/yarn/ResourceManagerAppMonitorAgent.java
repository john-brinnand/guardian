package spongecell.guardian.agent.yarn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;

import spongecell.workflow.config.repository.IGenericConfigurationRepository;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@ConfigurationProperties(prefix="")
@EnableConfigurationProperties({ 
	ResourceManagerAppMonitorConfiguration.class ,
	ResourceManagerMapReduceJobInfo.class
})
public class ResourceManagerAppMonitorAgent implements Agent {
	private ResourceManagerAppMonitorConfiguration config;
	public static final String BEAN_NAME = "resourceManagerAppMonitorAgent";
	public static final String BEAN_CONFIG_PREFIX = "app.monitor";
	public static final String BEAN_CONFIG_NAME = "resourceManagerAppMonitorConfiguration";
	
	public ResourceManagerAppMonitorAgent() { }
	
	public ResourceManagerAppMonitorAgent(IGenericConfigurationRepository repo) {
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
		log.info("Getting status");
		return new Object[1];
	}

	@Override
	public Agent buildAgent() {
		// TODO Auto-generated method stub
		return null;
	}

}
