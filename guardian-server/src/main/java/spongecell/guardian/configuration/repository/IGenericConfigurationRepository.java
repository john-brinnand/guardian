package spongecell.guardian.configuration.repository;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.springframework.context.ApplicationContext;

public interface IGenericConfigurationRepository {
	
	/**
	 * Get the iterator.
	 * 
	 * @return
	 */
	public Iterator<Object> iterator();

	/**
	 * Get an individual agent.
	 * 
	 * @param agentId
	 * @return
	 */
	public <T> T getAgent (String agentId);
	
	/**
	 * Get the agents and their configuration.
	 * 
	 * @return
	 */
	public Iterator<Entry<String, ArrayList<String>>> agentIterator();
	
	/**
	 * Get the application context from the repository.
	 * 
	 * @return
	 */
	public ApplicationContext getApplicationContext();
}
