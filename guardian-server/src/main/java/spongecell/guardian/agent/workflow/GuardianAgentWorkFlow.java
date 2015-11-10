package spongecell.guardian.agent.workflow;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lombok.Getter;

import org.kie.api.runtime.KieSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import spongecell.guardian.agent.yarn.Agent;
import spongecell.guardian.handler.KieMemoryFileSystemSessionHandler;

/**
 * A simple workflow that uses a sequence
 * of agents to discover facts about one or 
 * more infrastructure components. The findings
 * of these agents (called facts) are then sent to 
 * the rules engine for evaluation.
 *  
 * @author jbrinnand
 *
 */
@Getter
@EnableConfigurationProperties({
	KieMemoryFileSystemSessionHandler.class
})
public class GuardianAgentWorkFlow implements IAgentWorkFlow {
	private Map<String, Agent> workFlow;
	private @Autowired KieMemoryFileSystemSessionHandler kieMFSessionHandler;	
	
	@Bean(name="guardianAgentWorkFlow")
	public IAgentWorkFlow guardianAgentWorkFlow() {
		return new GuardianAgentWorkFlow();
	}
	
	public GuardianAgentWorkFlow() {
		this.workFlow = new LinkedHashMap<String, Agent>();
	}
	
	public KieSessionBuilder newSessionBuilder() {
		return new KieSessionBuilder();
	}

	public class KieSessionBuilder {
		private KieMemoryFileSystemSessionHandler.Builder builder;
		
		public KieSessionBuilder () {
			builder = kieMFSessionHandler.newBuilder();
		}
		
		public KieSessionBuilder groupId (String groupId) {
			builder.addGroupId(groupId);
			return this;
		}

		public KieSessionBuilder artifactId (String artifactId) {
			builder.addArtifactId(artifactId);
			return this;
		}	
		
		public KieSessionBuilder versionId (String versionId) {
			builder.addVersion(versionId);
			return this;
		}		
		public KieSessionBuilder moduleId (String moduleId) {
			builder.addModelId(moduleId);
			return this;
		}
		
		public KieSessionBuilder sessionId (String sessionId) {
			builder.addSessionId(sessionId);
			return this;
		}	
		
		public void build() {
			builder.build();	
		}
	}
	
	public IAgentWorkFlow addEntry (String step, Agent agent) {
		this.workFlow.put(step, agent);
		return this;
	}
	
	/**
	 * Use the agents to discover facts about the infrastructure
	 * components. Evaluate those findings using the rules engine.
	 */
	@Override
	public void execute() throws URISyntaxException {
		Set<Entry<String, Agent>>entries = workFlow.entrySet();
		ArrayList<Object[]> findings = new ArrayList<Object[]>(); 
		
		for (Entry<String, Agent>entry : entries) {
			Agent agent = entry.getValue();
			Object[] facts = agent.getStatus();
			findings.add(facts);
			
		}
		validateFindings(findings.toArray());
	}	


	/**
	 * Validate the facts / findings discovered by the agents.
	 * 
	 * @param facts
	 */
	private void validateFindings(Object[] facts) {
		final String groupId = kieMFSessionHandler.getGroupId();
		final String artifactId = kieMFSessionHandler.getArtifactId(); 
		final String version = kieMFSessionHandler.getVersion(); 
		final String sessionId = kieMFSessionHandler.getSessionId(); 
		
		KieSession kieSession = kieMFSessionHandler.getRepositorySession(
				groupId, 
				artifactId, 
				version, 
				sessionId);
		
		for (Object fact : facts) {
			kieSession.insert(fact);
		}
		kieSession.fireAllRules();
		kieSession.dispose();
	}
}
