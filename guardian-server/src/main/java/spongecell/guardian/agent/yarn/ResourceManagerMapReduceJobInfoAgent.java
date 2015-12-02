package spongecell.guardian.agent.yarn;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import spongecell.webhdfs.FilePath;
import spongecell.webhdfs.WebHdfsConfiguration;
import spongecell.webhdfs.WebHdfsOps;
import spongecell.webhdfs.WebHdfsWorkFlow;
import spongecell.webhdfs.WebHdfsWorkFlow.Builder;
import spongecell.webhdfs.exception.WebHdfsException;
import spongecell.workflow.config.repository.IGenericConfigurationRepository;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

@Slf4j
public class ResourceManagerMapReduceJobInfoAgent implements Agent {
	public static final String BEAN_NAME = "resourceManagerMapReduceJobInfoAgent";
	public static final String BEAN_CONFIG_NAME = "resourceManagerAppMonitorConfiguration";
	public static final String BEAN_CONFIG_PREFIX = ResourceManagerAppMonitorAgent.BEAN_CONFIG_PREFIX; 
	public static final String BEAN_WEBHDFS_CONFIG_NAME  = "resourceManagerMapReduceJobInfoWebHdfsConfig";
	public static final String BEAN_WEBHDFS_CONFIG_PREFIX  = "mapreduce.jobinfo.webhdfs";
	public static final String BEAN_WORKFLOW_BUILDER_NAME = "workFlowBuilder";
	public static final String BEAN_WORKFLOW_BUILDER_CONFIG_PREFIX = "mapreduce.jobinfo.workflow.webhdfs";
	
	private ResourceManagerAppMonitorConfiguration config;
	private RequestConfig requestConfig;	
	private WebHdfsWorkFlow.Builder builder;	
	private WebHdfsWorkFlow workFlow;	

	public ResourceManagerMapReduceJobInfoAgent() { 
		requestConfig = RequestConfig.custom()
		  .setConnectTimeout(3 * 1000)
		  .setConnectionRequestTimeout(1 * 1000)
		  .setSocketTimeout(3 * 1000)
		  .build();	
	}
	
	public ResourceManagerMapReduceJobInfoAgent (IGenericConfigurationRepository repo) {
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
			if (configId.equals(BEAN_CONFIG_PREFIX)) {
				config = (ResourceManagerAppMonitorConfiguration) ctx.getBean(BEAN_CONFIG_NAME);
				log.info(config.toString());
				continue;
			}
			if (configId.equals(BEAN_WEBHDFS_CONFIG_PREFIX)) {
				log.info(BEAN_WEBHDFS_CONFIG_PREFIX);
				builder = (Builder) ctx.getBean(BEAN_WORKFLOW_BUILDER_NAME);
				workFlow = builder
					.context(ctx)
					.repoId(BEAN_WEBHDFS_CONFIG_NAME)
					.build();
			}	
		} 
	}	

	@Override
	public Object[] getStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object[] getStatus(Object[] args) {
		// TODO Auto-generated method stub
		log.info("Args are: {}", args);
		JsonNode jsonAppStatus = (JsonNode)args[0];
		if (jsonAppStatus.get("app").get("state").asText().equals("UNKNOWN")) {
			return args;
		}
		String appId = jsonAppStatus.get("app").get("id").asText();
		
		try {
			log.info(new ObjectMapper().writerWithDefaultPrettyPrinter()
					.writeValueAsString(jsonAppStatus));
			getMapReduceJobInfo(appId, jsonAppStatus.toString());
		} catch (JsonProcessingException e) {
			log.info("ERROR - AppStatus arg is invalid. ", e);
		}
		return args; 
	}

	@Override
	public Agent buildAgent() {
		// TODO Auto-generated method stub
		return null;
	}

	public void getMapReduceJobInfo(String appId, String appStatus) {
		try {
			CloseableHttpResponse response = requestAppMapReduceJobs(appId);
			String jobContent = getContent(response.getEntity().getContent());
			String jobId = getJobId(jobContent, appStatus);
			if (jobId == null) {
				return;
			}
			response = requestAppMapReduceJobInfo(appId, jobId);
			if (response == null) {
				return;
			}
			String jobInfoContent = getContent(response.getEntity().getContent());
			JsonNode jsonJobInfoStatus = new ObjectMapper()
				.readTree(jobInfoContent);
			log.info(new ObjectMapper()
				.writerWithDefaultPrettyPrinter()
				.writeValueAsString(jsonJobInfoStatus));	
			
			StringEntity entity = new StringEntity(jobInfoContent);
			WebHdfsWorkFlow workFlow = buildWorkFlow(entity, appId);
			workFlow.execute();
		} catch (WebHdfsException | URISyntaxException | 
				IllegalStateException | IOException e) {
			log.info("ERROR getting MapReduce Job Info: {} ", e);
		}
	}
	
	/**
	 * Source: https://hadoop.apache.org/docs/r2.7.1/hadoop-mapreduce-client/
	 * hadoop-mapreduce-client-core/MapredAppMasterRest.html
	 * 
	 * GET http://<proxy http address:port>/proxy/
	 * application_1326232085508_0004/ws/v1/mapreduce/jobs
	 * 
	 * GET http://<proxy http address:port>/proxy/
	 * application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/conf
	 * @param appId
	 * @return
	 */
	public CloseableHttpResponse requestAppMapReduceJobs(String appId) 
		throws WebHdfsException{
		CloseableHttpClient httpClient = HttpClients.createDefault();
		URI uri = null;
		try {
			uri = new URIBuilder()
					.setScheme(config.getScheme())
					.setHost(config.getHost())
					.setPort(config.getPort())
					.setPath( "/" + "proxy" 
						+ "/" + appId + 
						"/" + "ws/v1/mapreduce/jobs")
					.build();
		} catch (URISyntaxException e) {
			throw new WebHdfsException(
					"ERROR - failure to create URI. Cause is:  ", e);
		}
		HttpGet get = new HttpGet(uri);
		get.setConfig(requestConfig);
		log.info("URI is : {} ", get.getURI().toString());

		CloseableHttpResponse response = null;
		try {
			response = httpClient.execute(get);
			Assert.notNull(response);
			log.info("Response status code {} ", response.getStatusLine()
					.getStatusCode());
			Assert.isTrue(response.getStatusLine().getStatusCode() == 200,
					"Response code indicates a failed GET operation");
		} catch (IOException e) {
			log.error("IOException timed out {} ", e);
			if (e instanceof ConnectionPoolTimeoutException) {
				log.info("Connection timed out {} ", e.getCause());
			}
			else {
				log.info("ERROR {} ", e.getCause());
			}			
		} finally {
			get.completed();
		}
		return response;
	}	
	
	/**
	 * Source: https://hadoop.apache.org/docs/r2.7.1/hadoop-mapreduce-client/
	 * hadoop-mapreduce-client-core/MapredAppMasterRest.html
	 * 
	 * GET http://<proxy http address:port>/proxy/
	 * application_1326232085508_0004/ws/v1/mapreduce/jobs/job_1326232085508_4_4/conf
	  
	 * @param appId
	 * @param jobId
	 * @return
	 */
	public CloseableHttpResponse requestAppMapReduceJobInfo(String appId, String jobId) {
		CloseableHttpClient httpClient = HttpClients.createDefault();
		URI uri = null;
		try {
			uri = new URIBuilder()
					.setScheme(config.getScheme())
					.setHost(config.getHost())
					.setPort(config.getPort())
					.setPath( "/" 
						+ "proxy" + "/" + appId 
						+ "/" + "ws/v1/mapreduce/jobs" 
						+ "/" + jobId + "/" + "conf")
					.build();
		} catch (URISyntaxException e) {
			throw new WebHdfsException(
					"ERROR - failure to create URI. Cause is:  ", e);
		}
		HttpGet get = new HttpGet(uri);
		log.info("URI is : {} ", get.getURI().toString());

		CloseableHttpResponse response = null;
		try {
			response = httpClient.execute(get);
			Assert.notNull(response);
			log.info("Response status code {} ", response.getStatusLine()
					.getStatusCode());
			Assert.isTrue(response.getStatusLine().getStatusCode() == 200,
					"Response code indicates a failed GET operation");
		} catch (IOException e) {
			log.info("ERROR {}", e);
		} finally {
			get.completed();
		}
		return response;
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

	/**
	 * {
	 *   "jobs": {
	 *       "job": [
	 *           {
	 *               "startTime": 1447449404328,
	 *               "finishTime": 0,
	 *               "elapsedTime": 7434,
	 *               "id": "job_1447351326751_0045",
	 *               "name": "word count",
	 *               "user": "root",
	 *               "state": "RUNNING",
	 *               "mapsTotal": 1,
	 *               "mapsCompleted": 1,
	 *               "reducesTotal": 1,
	 *               "reducesCompleted": 0,
	 *               "mapProgress": 100,
	 *               "reduceProgress": 0,
	 *               "mapsPending": 0,
	 *               "mapsRunning": 0,
	 *               "reducesPending": 1,
	 *               "reducesRunning": 0,
	 *               "uberized": false,
	 *               "diagnostics": "",
	 *               "newReduceAttempts": 1,
	 *               "runningReduceAttempts": 0,
	 *               "failedReduceAttempts": 0,
	 *               "killedReduceAttempts": 0,
	 *               "successfulReduceAttempts": 0,
	 *               "newMapAttempts": 0,
	 *               "runningMapAttempts": 0,
	 *               "failedMapAttempts": 0,
	 *               "killedMapAttempts": 0,
	 *               "successfulMapAttempts": 1
	 *           }
	 *       ]
	 *    }
	 * }
	 * @param content
	 * @param name
	 * @return
	 */
	public String getJobId(String content, String appStatus) {
		String id = null;
		try {
			String name = new ObjectMapper().readTree(appStatus)
				.get("app")
				.get("name")
				.asText();	
			
			Iterator<JsonNode> appsIter = new ObjectMapper().readTree(content)
					.elements();
			while (appsIter.hasNext()) {
				JsonNode app = appsIter.next();
				Iterator<JsonNode> elements = app.elements();
				while (elements.hasNext()) {
					JsonNode element = elements.next();
					Iterator<JsonNode> properties = element.iterator();
					while (properties.hasNext()) {
						JsonNode property = properties.next();
						if (property.get("name").asText().equals(name)) {
							id = property.get("id").asText();
							break;
						}
					}
				}
			}
		} catch (IOException e) {
			log.info("ERROR - failed to read the jobStatus: {} ", e);
		}
		return id;
	}		
	
	private WebHdfsWorkFlow buildWorkFlow(StringEntity entity, String appId) {
		DateTimeFormatter customDTF = new DateTimeFormatterBuilder()
	        .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
	        .appendValue(MONTH_OF_YEAR, 2)
	        .appendValue(DAY_OF_MONTH, 2)
	        .toFormatter();	

		// Basedir: /data/guardian/<dateTimeFormat>
		//*******************************************
		FilePath path = new FilePath.Builder()
			.addPathSegment(workFlow.getConfig().getBaseDir())
			.addPathSegment(customDTF.format(LocalDate.now()))
			.build();
		
		WebHdfsConfiguration config = workFlow.getConfig();
		String filePrefix = appId + "_"; 
		String relativePathFileName = filePrefix + workFlow.getConfig().getFileName();		
		String fileName = path.getFile().getPath() + File.separator + 
			relativePathFileName;	
		
		WebHdfsWorkFlow workFlow = builder
			.path(path.getFile().getPath())
			.fileName(relativePathFileName)
			.config(config)
			.addEntry("CreateBaseDir", 
				WebHdfsOps.MKDIRS, 
				HttpStatus.OK, 
				path.getFile().getPath())
			.addEntry("SetBaseDirOwner", 
				WebHdfsOps.SETOWNER, 
				HttpStatus.OK, 
				config.getBaseDir(), 
				config.getOwner(), 
				config.getGroup())
			.addEntry("CreateAndWriteToFile", 
				WebHdfsOps.CREATE, 
				HttpStatus.CREATED, 
				entity)
			.addEntry("SetFileOwner", WebHdfsOps.SETOWNER, 
				HttpStatus.OK, 
				fileName,
				config.getOwner(), 
				config.getGroup())
			.build();		
		return workFlow;
	}	
}
