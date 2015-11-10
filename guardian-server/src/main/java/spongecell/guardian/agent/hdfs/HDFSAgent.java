package spongecell.guardian.agent.hdfs;

import static spongecell.webhdfs.WebHdfsParams.DEFAULT_PERMISSIONS;
import static spongecell.webhdfs.WebHdfsParams.FILE;
import static spongecell.webhdfs.WebHdfsParams.FILE_STATUS;
import static spongecell.webhdfs.WebHdfsParams.FILE_STATUSES;
import static spongecell.webhdfs.WebHdfsParams.PERMISSION;
import static spongecell.webhdfs.WebHdfsParams.TYPE;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.LocalDateTime;

import javax.annotation.PostConstruct;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import spongecell.guardian.agent.exception.GuardianWorkFlowException;
import spongecell.guardian.agent.yarn.Agent;
import spongecell.guardian.model.HDFSDirectory;
import spongecell.guardian.notification.GuardianEvent;
import spongecell.guardian.notification.SlackGuardianWebHook;
import spongecell.webhdfs.WebHdfsConfiguration;
import spongecell.webhdfs.WebHdfsOps;
import spongecell.webhdfs.WebHdfsWorkFlow;

/**
 * @author jbrinnand
 */
@Slf4j
@Getter @Setter
@EnableConfigurationProperties({ WebHdfsConfiguration.class   })
public class HDFSAgent implements Agent {
	private @Autowired WebHdfsConfiguration webHdfsConfig;	
	private @Autowired WebHdfsWorkFlow.Builder webHdfsWorkFlowBuilder;
	
	@PostConstruct 
	public void init () {}
	
	@Bean(name="hdfsAgent")
	public Agent buildAgent() {
		return new HDFSAgent();
	}
	
	/**
	 * Get the status of a Yarn application. 
	 */
	@Override
	public Object[] getStatus () {
		log.info("********** Getting HDFS status.**********");
		
		Object[] facts =  null;
		
		// TODO the output path must be configurable.
		WebHdfsWorkFlow workFlow = webHdfsWorkFlowBuilder
			.path("/data/test-output1")
			.addEntry("ListDirectoryStatus", 
					WebHdfsOps.LISTSTATUS, 
					HttpStatus.OK, 
					webHdfsConfig.getBaseDir())
			.build();

		try {
			CloseableHttpResponse response = workFlow.execute();
			int responseCode = HttpStatus.OK.value();  
			Assert.isTrue(response.getStatusLine().getStatusCode() == responseCode, 
					"Response code indicates a failed write: " + 
					response.getStatusLine().getStatusCode());
			
			ArrayNode fileStatus = getFileStatus(response);
			
			facts = createFacts(fileStatus, "/data/test-output1");
			
		} catch (URISyntaxException | ParseException | IOException e) {
			throw new GuardianWorkFlowException("ERROR - HDFS Agent failure", e);
		} 
		return facts;
	}
	
	private ArrayNode getFileStatus(CloseableHttpResponse response) 
			throws JsonParseException, JsonMappingException, ParseException, IOException {
		ObjectNode dirStatus = new ObjectMapper().readValue(
			EntityUtils.toString(response.getEntity()), 
			new TypeReference<ObjectNode>() {
		});
		log.info("Directory status is: {} ", new ObjectMapper()
			.writerWithDefaultPrettyPrinter()
			.writeValueAsString(dirStatus));
		
		ArrayNode fileStatus  = new ObjectMapper().readValue(dirStatus
			.get(FILE_STATUSES)
			.get(FILE_STATUS).toString(),
			new TypeReference<ArrayNode>() { 
		});
		for (int i = 0; i < fileStatus.size(); i++) {
			JsonNode fileStatusNode = fileStatus.get(i);
			Assert.isTrue(fileStatusNode.get(TYPE).asText().equals(FILE), 
				"ERROR - cannot read the Node. It is not a file: " 
				+ fileStatusNode.get(TYPE).asText());
		}		
		return fileStatus;
	}
	
	private Object[] createFacts (ArrayNode fileStatus, String path) {
		HDFSDirectory hdfsDir = new HDFSDirectory();
		hdfsDir.setNumChildren(fileStatus.size());
		hdfsDir.setOwner("root");
		hdfsDir.setFileStatus(fileStatus);
		hdfsDir.setTargetDir(path);
		
		GuardianEvent event = new GuardianEvent();
		event.dateTime = LocalDateTime.now().toString();
		event.absolutePath = webHdfsConfig.getBaseDir();
		event.setEventSeverity(GuardianEvent.severity.INFORMATIONAL.name());
		
		SlackGuardianWebHook slackClient = new SlackGuardianWebHook();
		
		Object[] facts = { hdfsDir, event, slackClient };
		
		return facts;
	}
}
