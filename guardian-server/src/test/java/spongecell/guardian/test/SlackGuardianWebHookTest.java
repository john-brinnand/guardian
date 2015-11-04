package spongecell.guardian.test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;

import javax.annotation.PostConstruct;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import spongecell.guardian.model.HDFSDirectory;
import spongecell.guardian.notification.SlackGuardianWebHook;

/**
 * @author jbrinnand
 */
@Slf4j
@ContextConfiguration(classes = { SlackGuardianWebHookTest.class})
@EnableConfigurationProperties ({ SlackGuardianWebHook.class })
public class SlackGuardianWebHookTest extends AbstractTestNGSpringContextTests{
	private @Autowired SlackGuardianWebHook slackGuardianWebHook;
	private final static String MESSAGE = "This is posted to #guardian and comes from a bot named guardian.";
	private final static String EMOJI_GHOST = ":ghost:";

	@PostConstruct
	public void postConstruct() throws URISyntaxException, UnsupportedEncodingException {
	}

	@Test
	public void slackGuardianWebHookSend() throws ClientProtocolException, IOException {
		CloseableHttpResponse response = slackGuardianWebHook.send(MESSAGE, EMOJI_GHOST);
		log.info("Response status code {} ", response.getStatusLine().getStatusCode());
		Assert.assertEquals(HttpStatus.OK.value(), response.getStatusLine().getStatusCode());
		response.close();
	}	
}	