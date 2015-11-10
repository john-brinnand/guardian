package spongecell.guardian.agent.workflow;

import java.net.URISyntaxException;

public interface IAgentWorkFlow {

	/**
	 * Get the findings from all the agents and 
	 * validate them.
	 * 
	 * @throws URISyntaxException
	 */
	public abstract void execute() throws URISyntaxException;

}