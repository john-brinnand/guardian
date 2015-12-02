package spongecell.guardian.agent.workflow;

import java.net.URISyntaxException;

import spongecell.guardian.agent.yarn.Agent;

public interface IAgentWorkFlow {
	/**
	 * Get the findings from all the agents and 
	 * validate them.
	 * 
	 * @throws URISyntaxException
	 */
	public abstract void execute() throws URISyntaxException;
	
	/**
	 * Add an entry to the workflow.
	 * 
	 * @param step
	 * @param agent
	 * @return
	 */
	public IAgentWorkFlow addEntry (String step, Agent agent);
	
	public String getId();
	
	public void setId(String id);
}