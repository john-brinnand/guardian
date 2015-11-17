package spongecell.guardian.agent.yarn;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import spongecell.webhdfs.exception.WebHdfsException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@EnableConfigurationProperties({ 
	ResourceManagerAppMonitorConfiguration.class 
})
public class ResourceManagerMapReduceJobInfo {
	@Autowired
	private ResourceManagerAppMonitorConfiguration config;	
	private RequestConfig requestConfig;
	
	public ResourceManagerMapReduceJobInfo () {
		requestConfig = RequestConfig.custom()
			  .setConnectTimeout(3 * 1000)
			  .setConnectionRequestTimeout(1 * 1000)
			  .setSocketTimeout(3 * 1000)
			  .build();
	}
	
	public void getMapReduceJobInfo(String appId, String appStatus)
			throws IllegalStateException, IOException {
		CloseableHttpResponse response = requestAppMapReduceJobs(appId);
		
		String jobContent = getContent(response.getEntity().getContent());
		
		String jobId = getJobId(jobContent, appStatus);
		if (jobId != null) {
			response = requestAppMapReduceJobInfo(appId, jobId);
			String jobInfoContent = getContent(response.getEntity().getContent());
			JsonNode jsonJobInfoStatus = new ObjectMapper()
				.readTree(jobInfoContent);
			log.info(new ObjectMapper()
				.writerWithDefaultPrettyPrinter()
				.writeValueAsString(jsonJobInfoStatus));	
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
//			String content = getContent(response.getEntity().getContent());
//			log.info("******** MapReduce Jobs: {} ", content); 					
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
//			throw new GuardianWorkFlowException("ERROR - failed to read the jobStatus", e);
		}
		return id;
	}		
}
