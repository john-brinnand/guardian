package spongecell.guardian.agent.workflow;

import java.net.URISyntaxException;

import lombok.Getter;
import lombok.Setter;
import spongecell.guardian.agent.yarn.Agent;

public interface IAgentWorkFlow {
	@Getter @Setter
	public String id = "workFlow";
	
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
	

}