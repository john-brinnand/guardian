package spongecell.guardian.notification;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import spongecell.webhdfs.exception.WebHdfsException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
@Getter @Setter
//@EnableConfigurationProperties (SlackGuardianWebHookConfiguration.class)
public class SlackGuardianWebHook {
	private SlackGuardianWebHookConfiguration slackWebHookConfig;
	private CloseableHttpClient httpClient;
	private boolean sendMsg;
	private boolean messageSent;
	private String message;
	public static final String EMOJI_GHOST = ":ghost:";
	
	public SlackGuardianWebHook () {
		httpClient = HttpClients.createDefault();
		sendMsg = false;
		messageSent = false;
		message = new String();
		slackWebHookConfig = new SlackGuardianWebHookConfiguration();
	}
	
	public CloseableHttpResponse create(final AbstractHttpEntity entity)
			throws ClientProtocolException, IOException {
		/**
		 * curl -X POST --data-urlencode 'payload={
		 * "text": "This is posted to #guardian and comes from a bot named guardian.", 
		 * "icon_emoji": ":ghost:"}' 
		 * https://hooks.slack.com/services/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
		 */
		ObjectNode node = new ObjectMapper().createObjectNode();
		node.put("text", "Greetings from the guardian resource from SlackGuardianWebHook.\n Test");
		node.put("icon_emoji", ":ghost:");
		log.info (node.toString());
		
		URI uri = null;
		try {
			uri = new URIBuilder()
			.setScheme(slackWebHookConfig.getScheme())
			.setHost(slackWebHookConfig.getHost())
			.setPath("/" + slackWebHookConfig.getResource() + 
				"/" + slackWebHookConfig.getGuardianCertificate().trim())
			.build();
		} catch (URISyntaxException e) {
			throw new WebHdfsException("ERROR - failure to create URI. Cause is:  ", e);	
		}
		HttpPost post = new HttpPost(uri);
		log.info ("URI is : {} ", post.getURI().toString());
		
		post.addHeader("content-type", "application/json");
		log.info ("URI is : {} ", post.getURI().toString());
		post.setEntity(new StringEntity(node.toString().trim()));
		
		log.info("Entity is: {}",
				EntityUtils.toString(new StringEntity(node.toString())));
		
		CloseableHttpResponse response = httpClient.execute(post);
		log.info("Response is: {} ", response.getStatusLine().getStatusCode());
		log.info(EntityUtils.toString(response.getEntity()));
		
		return response;
	}

	public CloseableHttpResponse send(String message, String emoji)
			throws ClientProtocolException, IOException {
		
		ObjectNode node = new ObjectMapper().createObjectNode();
		node.put("text", message);
		node.put("icon_emoji", emoji);
		log.info (node.toString());
		
		URI uri = null;
		try {
			uri = new URIBuilder()
				.setScheme(slackWebHookConfig.getScheme())
				.setHost(slackWebHookConfig.getHost())
				.setPath("/" + slackWebHookConfig.getResource() + 
					"/" + slackWebHookConfig.getGuardianCertificate().trim())
				.build();
		} catch (URISyntaxException e) {
			throw new WebHdfsException("ERROR - failure to create URI. Cause is:  ", e);	
		}
		HttpPost post = new HttpPost(uri);
		log.info ("URI is : {} ", post.getURI().toString());
		
		post.addHeader("content-type", "application/json");
		log.info ("URI is : {} ", post.getURI().toString());
		post.setEntity(new StringEntity(new ObjectMapper()
			.writerWithDefaultPrettyPrinter()
			.writeValueAsString(node)));
		
		log.info("Entity is: {}",
				EntityUtils.toString(new StringEntity(node.toString())));
		
		CloseableHttpResponse response = httpClient.execute(post);
		log.info("Response is: {} ", response.getStatusLine().getStatusCode());
		log.info(EntityUtils.toString(response.getEntity()));
		
		return response;
	}	
}