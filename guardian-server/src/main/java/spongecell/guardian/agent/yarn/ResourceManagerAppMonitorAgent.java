package spongecell.guardian.agent.yarn;

import static spongecell.guardian.agent.yarn.model.ResourceManagerAppKeys.APP;
import static spongecell.guardian.agent.yarn.model.ResourceManagerAppKeys.FINAL_STATUS;
import static spongecell.guardian.agent.yarn.model.ResourceManagerAppKeys.STATE;
import static spongecell.guardian.agent.yarn.model.ResourceManagerAppKeys.TRACKING_URL;
import static spongecell.guardian.agent.yarn.model.ResourceManagerAppKeys.USER;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.util.Assert;

import spongecell.guardian.agent.exception.GuardianWorkFlowException;
import spongecell.guardian.agent.yarn.resourcemonitor.ResourceMonitorAppAgentRegistry;
import spongecell.webhdfs.exception.WebHdfsException;
import spongecell.workflow.config.repository.IGenericConfigurationRepository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
@Getter
@EnableConfigurationProperties({ 
	ResourceManagerAppMonitorConfiguration.class ,
	ResourceManagerMapReduceJobInfo.class
})
public class ResourceManagerAppMonitorAgent implements Agent {
	private ResourceManagerAppMonitorConfiguration config;
	private RequestConfig requestConfig;
	public static final String BEAN_NAME = "resourceManagerAppMonitorAgent";
	public static final String BEAN_CONFIG_PREFIX = "app.monitor";
	public static final String BEAN_CONFIG_NAME = "resourceManagerAppMonitorConfiguration";
	
	public ResourceManagerAppMonitorAgent() { 
		requestConfig = RequestConfig.custom()
			  .setConnectTimeout(3 * 1000)
			  .setConnectionRequestTimeout(1 * 1000)
			  .setSocketTimeout(3 * 1000)
			  .build();
	}
	
	public ResourceManagerAppMonitorAgent(IGenericConfigurationRepository repo) {
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
			}
		} 
	}	
	
	@Override
	public Object[] getStatus() {
		// TODO Auto-generated method stub
		log.info("Getting status");
		return null;
	}

	@Override
	public Object[] getStatus(Object[] args) {
		String[] users = config.getUsers();
		String appId = null; 
		CloseableHttpResponse response = null; 
		int retryCount = 5;
		do {
			response = requestResourceManagerAppsStatus();
			response.getStatusLine().getStatusCode();

			// Get the application's id.
			// **************************
			try {
				InputStream is = response.getEntity().getContent();
				appId = getUserAppId(users, is);
				Thread.sleep(3000);
				log.info("AppId is: {} ", appId);
				response.close();
				retryCount--;
			} catch (IllegalStateException | IOException | InterruptedException e) {
				throw new GuardianWorkFlowException("ERROR retrieving the App's status.", e);
			}
		} while (appId == null && retryCount > 0);
		
		if (appId == null) {
			ObjectNode node = JsonNodeFactory.instance.objectNode();
			node.set(APP, JsonNodeFactory.instance.objectNode());
			((ObjectNode)node.get(APP)).put(STATE, "UNKNOWN");
			((ObjectNode)node.get(APP)).put(FINAL_STATUS, "UNKNOWN");
			args[0] = node;
			return args;
		}
		
		// Extract the appId, return it as a fact.
		//*****************************************
		response = requestAppStatus(appId);
		
		JsonNode jsonAppStatus = null;
		String appStatus;
		try {
			appStatus = getContent(response.getEntity().getContent());
			jsonAppStatus = new ObjectMapper().readTree(appStatus);
			log.debug(new ObjectMapper().writerWithDefaultPrettyPrinter()
				.writeValueAsString(jsonAppStatus));
			response.close();
		} catch (IllegalStateException | IOException e) {
			throw new GuardianWorkFlowException("ERROR retrieving the App's status.", e);
		}
		Object[] obj = new Object[1];
		obj[0] = jsonAppStatus;
		return obj;
	}
	
	/**
	 * http://hadoop-production-resourcemanager.spongecell.net:8088/
	 * ws/v1/cluster/apps?states=running"
	 */
	public CloseableHttpResponse requestResourceManagerAppsStatus() 
		throws WebHdfsException{
		CloseableHttpClient httpClient = HttpClients.createDefault();
		URI uri = null;
		String states = ResourceManagerAppMonitorConfiguration.STATES;
		String runState = ResourceManagerAppMonitorConfiguration.RunStates.RUNNING
				.name();
		try {
			uri = new URIBuilder()
					.setScheme(config.getScheme())
					.setHost(config.getHost())
					.setPort(config.getPort())
					.setPath( "/" + config.getCluster() + "/"
							+ config.getEndpoint())
					.setParameter(states, runState).build();
		} catch (URISyntaxException e) {
			throw new WebHdfsException(
					"ERROR - failure to create URI. Cause is:  ", e);
		}
		HttpGet get = new HttpGet(uri);
		get.setConfig(requestConfig);
		log.debug("URI is : {} ", get.getURI().toString());

		CloseableHttpResponse response = null;
		try {
			response = httpClient.execute(get);
			Assert.notNull(response);
			log.info("Response status code {} ", response.getStatusLine()
					.getStatusCode());
			Assert.isTrue(response.getStatusLine().getStatusCode() == 200,
					"Response code indicates a failed request.");
		} catch (IOException e) {
			if (e instanceof ConnectionPoolTimeoutException) {
				log.info("Connection timed out {} ", e.getMessage());
			}
			else {
				log.info("ERROR {} ", e.getMessage());
			}
		} finally {
			 get.completed();
		}
		return response;
	}
	
	/**
	 * http://hadoop-production-resourcemanager.spongecell.net:8088
	 * /ws/v1/cluster/apps/application_1437061842430_87698"
	 * 
	 * @param appId
	 * @return
	 */
	public CloseableHttpResponse requestAppStatus(String appId) 
		throws WebHdfsException {
		CloseableHttpClient httpClient = HttpClients.createDefault();
		URI uri = null;
		try {
			uri = new URIBuilder()
					.setScheme(config.getScheme())
					.setHost(config.getHost())
					.setPort(config.getPort())
					.setPath( "/" + config.getCluster() + "/"
						+ config.getEndpoint() + "/" + appId)
					.setParameter("states", "running,finished")
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
					"Response code indicates a failed write");
		} catch (IOException e) {
			if (e instanceof ConnectionPoolTimeoutException) {
				log.info("Connection timed out {} ", e.getMessage());
			}
			else {
				log.info("ERROR {} ", e.getMessage());
			}	
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
	
	public String getUserAppId(String[] users, InputStream is)
			throws JsonProcessingException, IOException {
		String appId = null;
		String appStatus = getContent(is);

		Iterator<JsonNode> appsIter = new ObjectMapper().readTree(appStatus)
				.elements();
		while (appsIter.hasNext()) {
			JsonNode app = appsIter.next();
			Iterator<JsonNode> elements = app.elements();
			while (elements.hasNext()) {
				JsonNode element = elements.next();
				Iterator<JsonNode> properties = element.iterator();
				while (properties.hasNext()) {
					JsonNode property = properties.next();
					for (String user : users) {
						if (property.get(USER).asText().equals(user)) {
							String trackingUrl = property.get(TRACKING_URL)
									.toString();
							String[] urlElements = trackingUrl.split("/");
							appId = urlElements[urlElements.length - 2];
							break;
						}
					}
				}
			}
		}
		return appId;
	}
	
	@Override
	public Agent buildAgent() {
		// TODO Auto-generated method stub
		return null;
	}
}
