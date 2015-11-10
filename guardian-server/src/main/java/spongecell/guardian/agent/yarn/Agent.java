package spongecell.guardian.agent.yarn;

import org.springframework.context.annotation.Bean;


/**
 * @author jbrinnand
 */
public interface Agent {
	/**
	 * Get the status of  managed object
	 * or component in an Infrastructure.
	 */
	public abstract Object[] getStatus();
	
	/**
	 * This method must contain a name for the 
	 * bean. Without it, the agent will not be 
	 * built, loaded or run dynamically. 
	 * 
	 * @return
	 */
	@Bean(name="")
	public abstract Agent buildAgent(); 

}