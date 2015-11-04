package spongecell.guardian.agent.yarn;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.util.Assert;

import spongecell.webhdfs.exception.WebHdfsException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author jbrinnand
 */
@Slf4j
@Getter
@EnableConfigurationProperties({ ResourceManagerAppMonitorConfiguration.class })
public class ResourceManagerAppMonitor {
	@Autowired
	private ResourceManagerAppMonitorConfiguration config;
	private CloseableHttpClient httpClient;

	public ResourceManagerAppMonitor() {
		httpClient = HttpClients.createDefault();
	}

	public JsonNode getResourceManagerAppStatus(String appName)
			throws IllegalStateException, IOException, InterruptedException {

		String appId = null; 
		CloseableHttpResponse response = null; 
		int retryCount = 5;
		do {
			response = requestResourceManagerAppsStatus();
			response.getStatusLine().getStatusCode();

			// Get the application's id.
			// **************************
			InputStream is = response.getEntity().getContent();
			appId = getAppId(appName, is);
			Thread.sleep(1000);
			log.info("AppId is: {} ", appId);
			response.close();
			retryCount--;
		} while (appId == null && retryCount > 0);
		
		if (appId == null) {
			ObjectNode node = JsonNodeFactory.instance.objectNode();
			node.set("app", JsonNodeFactory.instance.objectNode());
			((ObjectNode)node.get("app")).put("state", "UNKNOWN");
			((ObjectNode)node.get("app")).put("finalStatus", "UNKNOWN");
			return node;
		}
		
		// Extract the appId, return it as a fact.
		//*****************************************
		response = requestAppStatus(appId);
		String appStatus = getContent(response.getEntity().getContent());
		JsonNode jsonAppStatus = new ObjectMapper().readTree(appStatus);
		log.debug(new ObjectMapper().writerWithDefaultPrettyPrinter()
			.writeValueAsString(jsonAppStatus));
		response.close();
		return jsonAppStatus;
	}
	
	public JsonNode getResourceManagerAppStatusUser(String[] users)
			throws IllegalStateException, IOException, InterruptedException {

		String appId = null; 
		CloseableHttpResponse response = null; 
		int retryCount = 5;
		do {
			response = requestResourceManagerAppsStatus();
			response.getStatusLine().getStatusCode();

			// Get the application's id.
			// **************************
			InputStream is = response.getEntity().getContent();
			appId = getUserAppId(users, is);
			Thread.sleep(3000);
			log.info("AppId is: {} ", appId);
			response.close();
			retryCount--;
		} while (appId == null && retryCount > 0);
		
		if (appId == null) {
			ObjectNode node = JsonNodeFactory.instance.objectNode();
			node.set("app", JsonNodeFactory.instance.objectNode());
			((ObjectNode)node.get("app")).put("state", "UNKNOWN");
			((ObjectNode)node.get("app")).put("finalStatus", "UNKNOWN");
			return node;
		}
		
		// Extract the appId, return it as a fact.
		//*****************************************
		response = requestAppStatus(appId);
		String appStatus = getContent(response.getEntity().getContent());
		JsonNode jsonAppStatus = new ObjectMapper().readTree(appStatus);
		log.debug(new ObjectMapper().writerWithDefaultPrettyPrinter()
			.writeValueAsString(jsonAppStatus));
		response.close();
		return jsonAppStatus;
	}
	

	/**
	 * http://hadoop-production-resourcemanager.spongecell.net:8088/
	 * ws/v1/cluster/apps?states=running"
	 */
	public CloseableHttpResponse requestResourceManagerAppsStatus() {
		URI uri = null;
		String states = ResourceManagerAppMonitorConfiguration.STATES;
		String runState = ResourceManagerAppMonitorConfiguration.RunStates.RUNNING
				.name();
		try {
			uri = new URIBuilder()
					.setScheme(config.getScheme())
					.setHost(config.getHost())
					.setPort(config.getPort())
					.setPath(
							"/" + config.getCluster() + "/"
									+ config.getEndpoint())
					.setParameter(states, runState).build();
		} catch (URISyntaxException e) {
			throw new WebHdfsException(
					"ERROR - failure to create URI. Cause is:  ", e);
		}
		HttpGet get = new HttpGet(uri);
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
			throw new WebHdfsException("ERROR - failure to create file: "
					+ uri.toString(), e);
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
	public CloseableHttpResponse requestAppStatus(String appId) {
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
		log.info("URI is : {} ", get.getURI().toString());

		CloseableHttpResponse response = null;
		try {
			response = httpClient.execute(get);
			Assert.notNull(response);
			log.info("Response status code {} ", response.getStatusLine()
					.getStatusCode());
			Assert.isTrue(response.getStatusLine().getStatusCode() == 200,
					"Response code indicates a failed write");
			// response.close();

		} catch (IOException e) {
			throw new WebHdfsException("ERROR - failure to create file: "
					+ uri.toString(), e);
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
	 * Utility: get the appName from the ResourceManagers Applications' Status.
	 * 
	 * { "apps": { "app": [ { "id": "application_1437061842430_87723", "user":
	 * "heston", "name": "[ETL] Elastic Search Load Mappings", "queue":
	 * "heston", "state": "RUNNING", "finalStatus": "UNDEFINED", "progress": 5,
	 * "trackingUI": "ApplicationMaster", "trackingUrl":
	 * "http://hadoop-production-resourcemanager.spongecell.net:8081
	 * /proxy/application_1437061842430_87723/", "diagnostics": "", "clusterId":
	 * 1437061842430, "applicationType": "MAPREDUCE", "startedTime":
	 * 1445975779066, "finishedTime": 0, "elapsedTime": 11490,
	 * "amContainerLogs": "http://hadoop-production-worker44.spongecell.net:8042
	 * /node/containerlogs/container_1437061842430_87723_01_000001/heston",
	 * "amHostHttpAddress": "hadoop-production-worker44.spongecell.net:8042",
	 * "allocatedMB": 3072, "allocatedVCores": 2, "runningContainers": 2 } ] } }
	 * 
	 * @param appName
	 * @param appStatus
	 * @return
	 * @throws JsonProcessingException
	 * @throws IOException
	 */
	public String getAppId(String appName, InputStream is)
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
					if (property.get("name").asText().equals(appName)) {
						String trackingUrl = property.get("trackingUrl")
								.toString();
						String[] urlElements = trackingUrl.split("/");
						appId = urlElements[urlElements.length - 2];
						break;
					}
				}
			}
		}
		return appId;
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
						if (property.get("user").asText().equals(user)) {
							String trackingUrl = property.get("trackingUrl")
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
}
