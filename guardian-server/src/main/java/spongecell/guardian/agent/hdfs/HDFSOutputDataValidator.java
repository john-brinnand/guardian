package spongecell.guardian.agent.hdfs;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static spongecell.webhdfs.WebHdfsParams.FILE;
import static spongecell.webhdfs.WebHdfsParams.FILE_STATUS;
import static spongecell.webhdfs.WebHdfsParams.FILE_STATUSES;
import static spongecell.webhdfs.WebHdfsParams.TYPE;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.util.Assert;

import spongecell.guardian.agent.exception.GuardianWorkFlowException;
import spongecell.guardian.agent.util.Args;
import spongecell.guardian.agent.workflow.GuardianAgentWorkFlowKeys;
import spongecell.guardian.agent.yarn.Agent;
import spongecell.guardian.model.HDFSDirectory;
import spongecell.guardian.notification.GuardianEvent;
import spongecell.guardian.notification.SlackGuardianWebHook;
import spongecell.webhdfs.FilePath;
import spongecell.webhdfs.WebHdfsConfiguration;
import spongecell.webhdfs.WebHdfsOps;
import spongecell.webhdfs.WebHdfsWorkFlow;
import spongecell.webhdfs.WebHdfsWorkFlow.Builder;
import spongecell.workflow.config.repository.IGenericConfigurationRepository;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
		if (log.isDebugEnabled()) {
			String[] beanNames = repo.getApplicationContext().getBeanDefinitionNames();
			for (String beanName : beanNames) {
				log.info(beanName);
			}		
		}
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
	public Args getStatus(Args args) {
		log.info("********** Getting HDFS status.**********");
		WebHdfsConfiguration webHdfsConfig = workFlow.getConfig();
		String jobStatusFileName = (String) args.getMap().get(
				GuardianAgentWorkFlowKeys.JOB_STATUS_FILE);
		if (jobStatusFileName == null) {
			return args;
		}
		log.info("Job status location: {} ", webHdfsConfig.getBaseDir() + "/" +   
			args.getMap().get(GuardianAgentWorkFlowKeys.JOB_STATUS_FILE));
		
		// TODO - DateTimeFormaater and FilePath should be in a utility class
		DateTimeFormatter customDTF = new DateTimeFormatterBuilder()
        	.appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
        	.appendValue(MONTH_OF_YEAR, 2)
        	.appendValue(DAY_OF_MONTH, 2)
        	.toFormatter();	
		
		FilePath filePath = new FilePath.Builder()
			.addPathSegment(workFlow.getConfig().getBaseDir())
			.addPathSegment(customDTF.format(LocalDate.now()))
			.addPathSegment(jobStatusFileName)
			.build();
			
		log.info("Path parent is: {}", filePath.getFile().getParent());
		
		
//		WebHdfsWorkFlow workFlow = builder
//			.path(filePath.getFile().getParent())
//			.addEntry("GetFileStatus", 
//					WebHdfsOps.GETFILESTATUS, 
//					HttpStatus.OK, 
//					jobStatusFileName)
//			.build();

			try {
				JsonNode jobStatus = readJobInfoStatusFile(filePath);
				getJobOutputDir(jobStatus);
			} catch (IllegalStateException | IOException | URISyntaxException e) {
				log.error("Failed to read job status: {} ", e);
				throw new GuardianWorkFlowException("ERROR - HDFS Agent failure", e);
			}
			
//			CloseableHttpResponse response = workFlow.execute();
//			int responseCode = HttpStatus.OK.value();  
//			Assert.isTrue(response.getStatusLine().getStatusCode() == responseCode, 
//					"Response code indicates a failed read : " + 
//					response.getStatusLine().getStatusCode());
			
			// TODO get the output directory from the jobInfoStatusFile and
			// use it to access the output directory and the files within it.
//			JsonNode fileStatus = getFileStatus(response);
			
			
//			createFacts(fileStatus, "/data/test-output1", args);
			
//		} catch (URISyntaxException | ParseException | IOException e) {
		return args;		
	}	
	
	public Object[] getStatus(Object[] args) {
//		log.info("********** Getting HDFS status.**********");
//		
//		WebHdfsConfiguration webHdfsConfig = workFlow.getConfig();
//		Object[] facts =  null;
//		String path = webHdfsConfig.getBaseDir() + "/" + webHdfsConfig.getFileName();
//		log.info(path);
//		
//		WebHdfsWorkFlow workFlow = builder
//			.path(path)
//			.addEntry("ListDirectoryStatus", 
//					WebHdfsOps.LISTSTATUS, 
//					HttpStatus.OK, 
//					webHdfsConfig.getBaseDir())
//			.build();
//
//		try {
//			CloseableHttpResponse response = workFlow.execute();
//			int responseCode = HttpStatus.OK.value();  
//			Assert.isTrue(response.getStatusLine().getStatusCode() == responseCode, 
//					"Response code indicates a failed write: " + 
//					response.getStatusLine().getStatusCode());
//			
//			JsonNode fileStatus = getFileStatus(response);
//			log.info("Job Info File Status is: {} ",
//				new ObjectMapper().writerWithDefaultPrettyPrinter()
//					.writeValueAsString(fileStatus));
//			
//			readJobInfoStatusFile(fileStatus);
//			
//			// TODO get the output directory from the jobInfoStatusFile and
//			// use it to access the output directory and the files within it.
////			facts = createFacts(fileStatus, "/data/test-output1", new Args());
//			
//		} catch (URISyntaxException | ParseException | IOException e) {
//			throw new GuardianWorkFlowException("ERROR - HDFS Agent failure", e);
//		} 
		return null;	
	}

//	private ArrayNode getFileStatus(CloseableHttpResponse response) 
//			throws JsonParseException, JsonMappingException, ParseException, IOException {
//		ObjectNode dirStatus = new ObjectMapper().readValue(
//			EntityUtils.toString(response.getEntity()), 
//			new TypeReference<ObjectNode>() {
//		});
//		log.info("Directory status is: {} ", new ObjectMapper()
//			.writerWithDefaultPrettyPrinter()
//			.writeValueAsString(dirStatus));
//		
//		ArrayNode fileStatus  = new ObjectMapper().readValue(dirStatus
//			.get(FILE_STATUSES)
//			.get(FILE_STATUS).toString(),
//			new TypeReference<ArrayNode>() { 
//		});
//		for (int i = 0; i < fileStatus.size(); i++) {
//			JsonNode fileStatusNode = fileStatus.get(i);
//			Assert.isTrue(fileStatusNode.get(TYPE).asText().equals(FILE), 
//				"ERROR - cannot read the Node. It is not a file: " 
//				+ fileStatusNode.get(TYPE).asText());
//		}		
//		return fileStatus;
//	}
	/**
	 * curl -i -L "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN
                    [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]"
	 * @return
	 * @throws URISyntaxException 
	 * @throws IOException 
	 * @throws IllegalStateException 
	 */
	private JsonNode readJobInfoStatusFile(FilePath jobInfoFileStatusPath)
			throws URISyntaxException, IllegalStateException, IOException {
		WebHdfsWorkFlow workFlow = builder
				.path(jobInfoFileStatusPath.getFile().getParent())
				.addEntry("OpenReadFile", 
						WebHdfsOps.OPENANDREAD, 
						HttpStatus.OK, 
						jobInfoFileStatusPath.getFileName())
				.build();
		CloseableHttpResponse response = workFlow.execute();
		String content = getContent(response.getEntity().getContent());
		JsonNode jobStatus = new ObjectMapper() .readTree(content);
		log.info("Job status file content is: {} ", new ObjectMapper()
			.writerWithDefaultPrettyPrinter()
			.writeValueAsString(jobStatus));	
		return jobStatus;
	}
	
	private JsonNode getJobOutputDir (JsonNode jobStatus) {
		Iterator<JsonNode> properties = jobStatus.get("conf").get("property").iterator();
		while (properties.hasNext()) {
			log.info(properties.next().toString());
		}
		return null;
	}
	
	private JsonNode getFileStatus(CloseableHttpResponse response) 
			throws JsonParseException, JsonMappingException, ParseException, IOException {
		ObjectNode jsonFileStatus = new ObjectMapper().readValue(
			EntityUtils.toString(response.getEntity()), 
			new TypeReference<ObjectNode>() {
		});
		log.info("File status is: {} ", new ObjectMapper()
			.writerWithDefaultPrettyPrinter()
			.writeValueAsString(jsonFileStatus));
		
		JsonNode fileStatus = jsonFileStatus.get("FileStatus");
		Assert.isTrue(fileStatus.get(TYPE).asText().equals(FILE), 
				"ERROR - cannot read the Node. It is not a file: " 
				+ fileStatus.get(TYPE).asText());
		return fileStatus;
	}	
		
	private Object[] createFacts (ArrayNode fileStatus, String path, Args args) {
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
		args.addArg("hdfsDataValidatorHdfsFact", hdfsDir);
		args.addArg("hdfsDataValidatorEventFact", event);
		args.addArg("hdfsDataValidatorSlackClient", slackClient);
		
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
	

	/**
	 * Utility: getContent from a stream.
	 * 
	 * @param is
	 * @return
	 * @throws IOException
	 */
	private String getContent(InputStream is) throws IOException {
		ByteArrayBuilder bab = new ByteArrayBuilder();
		int value;
		while ((value = is.read()) != -1) {
			bab.append(value);
		}
		String content = new String(bab.toByteArray());
		bab.close();
		return content;
	}	
}
