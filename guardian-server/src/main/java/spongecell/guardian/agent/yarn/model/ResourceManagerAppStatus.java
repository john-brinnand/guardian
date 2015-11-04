package spongecell.guardian.agent.yarn.model;

import static spongecell.guardian.agent.yarn.ResourceManagerAppMonitorConfiguration.APP;
import static spongecell.guardian.agent.yarn.ResourceManagerAppMonitorConfiguration.STATE;
import static spongecell.guardian.agent.yarn.ResourceManagerAppMonitorConfiguration.FINAL_STATUS;
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
		ObjectNode node =  JsonNodeFactory.instance.objectNode();
		node.set("app", JsonNodeFactory.instance.objectNode());
		((ObjectNode)node.get("app")).put("user", appStatus.get("app").get("user").asText());
		((ObjectNode)node.get("app")).put("name", appStatus.get("app").get("name").asText());
		((ObjectNode)node.get("app")).put("state", appStatus.get("app").get("state").asText());
		((ObjectNode)node.get("app")).put("queue", appStatus.get("app").get("queue").asText());
		((ObjectNode)node.get("app")).put("finalStatus", 
				appStatus.get("app").get("finalStatus").asText());	
		((ObjectNode)node.get("app")).put("progress", 
				appStatus.get("app").get("progress").asText());	
		((ObjectNode)node.get("app")).put("applicationType", 
				appStatus.get("app").get("applicationType").asText());	
		((ObjectNode)node.get("app")).put("startedTime", 
				appStatus.get("app").get("startedTime").asText());	
		((ObjectNode)node.get("app")).put("finishedTime", 
				appStatus.get("app").get("finishedTime").asText());	
		((ObjectNode)node.get("app")).put("elapsedTime", 
				appStatus.get("app").get("elapsedTime").asText());	
		
		return node; 
	}
}
