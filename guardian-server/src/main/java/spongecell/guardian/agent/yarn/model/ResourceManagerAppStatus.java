package spongecell.guardian.agent.yarn.model;

import static spongecell.guardian.agent.yarn.model.ResourceManagerAppKeys.*;

import lombok.Getter;
import lombok.Setter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author jbrinnand
 */
@Getter @Setter
public class ResourceManagerAppStatus {
	private JsonNode appStatus;
	private boolean active = false;
	
	public static ResourceManagerAppStatus instance() {
		return new ResourceManagerAppStatus();
	}
	
	public String getState () {
		return appStatus.get(APP).get(STATE).asText();
	}
	
	public String getFinalStatus () {
		return appStatus.get(APP).get(FINAL_STATUS).asText();
	}	
	
	public String getManagedObject() {
		return appStatus.get(APP).get("name").asText();
	} 
	
	public JsonNode getBody() {
		// TODO - use a schema to generate the body.
		//******************************************
		ObjectNode node =  JsonNodeFactory.instance.objectNode();
		
		node.set(APP, JsonNodeFactory.instance.objectNode());
		((ObjectNode)node.get(APP)).put(USER, appStatus.get(APP).get(USER).asText());
		((ObjectNode)node.get(APP)).put(NAME, appStatus.get(APP).get(NAME).asText());
		((ObjectNode)node.get(APP)).put(STATE, appStatus.get(APP).get(STATE).asText());
		((ObjectNode)node.get(APP)).put(QUEUE, appStatus.get(APP).get(QUEUE).asText());
		((ObjectNode)node.get(APP)).put(FINAL_STATUS, 
				appStatus.get(APP).get(FINAL_STATUS).asText());	
		((ObjectNode)node.get(APP)).put(PROGRESS, 
				appStatus.get(APP).get(PROGRESS).asText());	
		((ObjectNode)node.get(APP)).put(APPLICATION_TYPE, 
				appStatus.get(APP).get(APPLICATION_TYPE).asText());	
		((ObjectNode)node.get(APP)).put(STARTED_TIME, 
				appStatus.get(APP).get(STARTED_TIME).asText());	
		((ObjectNode)node.get(APP)).put(FINISHED_TIME, 
				appStatus.get(APP).get(FINISHED_TIME).asText());	
		((ObjectNode)node.get(APP)).put(ELAPSED_TIME, 
				appStatus.get(APP).get(ELAPSED_TIME).asText());	
		
		return node; 
	}
}
