package spongecell.guardian.test;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
@ContextConfiguration(classes = { AnnotationTests.class })
@EnableConfigurationProperties (AnnotationTests.TemplateProperties.class)
public class AnnotationTests extends AbstractTestNGSpringContextTests{
	@Autowired TemplateProperties properties;
	private final static String TEMPLATE_ID = "123";
	private final static String ZOOKEEPER_CONNECT_VALUE = "192.168.33.10:2181";
	private final static String GROUP_ID_VALUE = "testGroup";		
	private final static String KAFKA_BROKERS = "192.168.33.10:9092";

	/**
	 * For this test to run successfully, the following properties must be
	 * set in the environment:
	 * 
	 * 		template.properties.groupId : testGroup
	 * 		template.properties.zookeeperConnect:192.168.33.10:2181
	 * 		template.properties.kafkaBrokers: 192.168.33.10:9092
	 * 
	 * Also - the default values are not the same as the environment values. 
	 * The test will therefore fail, if the environment is not configured
	 * correctly.
	 */
	@Test
	public void testConfigProperties() {
		log.info("Template id {}: ", properties.getTemplateId());
		log.info("Template zookeeperConnect {}: ", properties.getZookeeperConnect());
		log.info("Template kafkaBrokers {}: ", properties.getKafkaBrokers());
	
		 // Note that groupId, by default is set to "defaultGroup". However
		 // its final value - taken from the environment - is "testGroup". This
		 // validate that Spring will override default values with values from 
		 // the environment, if they exist. 
		 //**********************************************************************
		log.info("Template id {}: ", properties.getGroupId());
		
		Assert.assertEquals(properties.getTemplateId(), TEMPLATE_ID);
		Assert.assertEquals(properties.getGroupId(), GROUP_ID_VALUE);
		Assert.assertEquals(properties.getZookeeperConnect(), ZOOKEEPER_CONNECT_VALUE);
		Assert.assertEquals(properties.getKafkaBrokers(), KAFKA_BROKERS);
	}
	
	@Getter @Setter
	@ConfigurationProperties("template.properties")
	public static class TemplateProperties {
		private String templateId = "123";
		private String zookeeperConnect = "192.168.33.1.2182";		
		private String kafkaBrokers = "192.168.33.1:9092"; 
		private String groupId = "defaultGroup";
		
		public TemplateProperties() {}
	}
}
