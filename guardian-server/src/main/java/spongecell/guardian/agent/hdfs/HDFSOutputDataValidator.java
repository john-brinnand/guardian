package spongecell.guardian.agent.hdfs;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.codehaus.plexus.component.annotations.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
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
import spongecell.webhdfs.WebHdfsWorkFlow.Builder;
import spongecell.workflow.config.repository.IGenericConfigurationRepository;
import static spongecell.webhdfs.WebHdfsParams.FILE;
import static spongecell.webhdfs.WebHdfsParams.FILE_STATUS;
import static spongecell.webhdfs.WebHdfsParams.FILE_STATUSES;
import static spongecell.webhdfs.WebHdfsParams.TYPE;

@Slf4j
@EnableConfigurationProperties({ 
	WebHdfsConfiguration.class, 
	WebHdfsWorkFlow.Builder.class 
})
public class HDFSOutputDataValidator implements Agent {
	private WebHdfsWorkFlow.Builder builder;
	private WebHdfsWorkFlow workFlow;
	public static final String BEAN_NAME = "hdfsOutputDataValidator";
	public static final String BEAN_CONFIG_PROPS_PREFIX = "hdfs.output.data.validator";
	public static final String WEBHDFS_BEAN_NAME = "hdfsOutputDataValidatorWebhdfsConfigBean";
	public static final String WEBHDFS_BEAN_CONFIG_PROPS_PREFIX = "hdfs.output.webhdfs";
	public static final String WEBHDFS_WORKFLOW_BEAN_NAME = "hdfsWorkFlowBeanName";
	public static final String WEBHDFS_WORKFLOW_CONFIG_PREFIX = "hdfs.output.workflow.webhdfs";
	
	public HDFSOutputDataValidator () { } 
		
	public HDFSOutputDataValidator (IGenericConfigurationRepository repo) { 
		Iterator<Entry<String, ArrayList<String>>> entries = repo.agentIterator();
		while (entries.hasNext()) {
			Entry<String, ArrayList<String>> entry = entries.next();
			if (entry.getKey().equals(BEAN_NAME)) {
				log.info("Building agent: {} ", entry.getKey());
				buildAgent(entry, repo.getApplicationContext());
			}
		}
	} 
	
	public void buildAgent(Entry<String, ArrayList<String>> agentEntry,
			ApplicationContext ctx) { 
		ArrayList<String> configIds = agentEntry.getValue();
		for (String configId : configIds) {
			if (configId.equals(WEBHDFS_BEAN_CONFIG_PROPS_PREFIX)) {
				builder = (Builder) ctx.getBean(WEBHDFS_WORKFLOW_BEAN_NAME);
				workFlow = builder
					.context(ctx)
					.repoId(WEBHDFS_BEAN_NAME)
					.build();
			}
		} 
	} 
	
	@Override
	public Object[] getStatus(Object[] args) {
		log.info("********** Getting HDFS status.**********");
		
		WebHdfsConfiguration webHdfsConfig = workFlow.getConfig();
		Object[] facts =  null;
		String path = webHdfsConfig.getBaseDir() + "/" + webHdfsConfig.getFileName();
		log.info(path);
		
		WebHdfsWorkFlow workFlow = builder
			.path(path)
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
		event.absolutePath = workFlow.getConfig().getBaseDir();
		event.setEventSeverity(GuardianEvent.severity.INFORMATIONAL.name());
		
		SlackGuardianWebHook slackClient = new SlackGuardianWebHook();
		
		Object[] facts = { hdfsDir, event, slackClient };
		
		return facts;
	}
	
	@Override
	public Object[] getStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Agent buildAgent() {
		// TODO Auto-generated method stub
		return null;
	}
}
