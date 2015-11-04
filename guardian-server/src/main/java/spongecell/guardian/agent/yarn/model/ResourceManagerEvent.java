package spongecell.guardian.agent.yarn.model;

import java.time.LocalDateTime;
import java.util.Iterator;

import lombok.Getter;
import lombok.Setter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Getter @Setter
public class ResourceManagerEvent {
	public String source;
	public String dateTime ;
	public String managedObject;
	public String expected;
	
	public static enum severity {
		INFORMATIONAL,
		MINOR,
		MAJOR,
		CRITICAL
	};
	public String absolutePath;
	public String message = new String();
	public Object body = new String();
	public String eventSeverity;
	private boolean valid = true;
	
	public ResourceManagerEvent () {
		dateTime = LocalDateTime.now().toString();
	}
	
	public String getMessage() {
		message = String.format("%s : %s,\n%s : %s\n%s : %s\n%s\n%s\n%s", 
			"ManagedObject", managedObject, 
			"Source", source, 
			"Severity", eventSeverity, 
			"*****************************************************************", 
			 body, 
			"*****************************************************************"); 
		return message;
	}
	// TODO: there should data formatters for JSON, Excel, CSV, 
	// or whatever is needed by the client. They should either
	// be injected into the GuardianEvent by a builder or a dispatcher 
	// should delegate the job of data formatting to these methods / objects.
	//***********************************************************************
	public String getJsonEventMessage() throws JsonProcessingException {
		ObjectNode event = new ObjectMapper().createObjectNode();
		event.put ("source", source);
		event.put ("timestamp", dateTime);
		event.put("severity", eventSeverity);
		event.put("target", managedObject);
		
		int i = 1;
		if (body instanceof ArrayNode) {
			event.put("Number of files found", ((ArrayNode)body).size());
			Iterator<JsonNode> nodes = ((ArrayNode)body).iterator();
			while (nodes.hasNext()) {
				JsonNode node = nodes.next();
				event.put("Row " + i,  new ObjectMapper()
					.writerWithDefaultPrettyPrinter()
					.writeValueAsString(node).replace("\n", "").replace("\"", ""));
				i++;
			}
		}
		else if (body instanceof JsonNode) {
			event.put("Row-" + i,   new ObjectMapper()
				.writerWithDefaultPrettyPrinter()
				.writeValueAsString((JsonNode)body)
				.replace("\n", "").replace("\"", ""));			
		}
		return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(event);
	}
}
