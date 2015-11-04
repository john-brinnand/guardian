package spongecell.guardian.test;

import java.io.InputStream;

import lombok.extern.slf4j.Slf4j;

import org.junit.Assert;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.builder.Message.Level;
import org.kie.api.io.KieResources;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import spongecell.guardian.model.Person;

@Slf4j
@ContextConfiguration(classes = { BusinessRulesTest.class })
public class BusinessRulesTest extends AbstractTestNGSpringContextTests {
	private final static String BASE_PATH = "src/main/resources";
	private KieSession kieSession;

	@BeforeTest
	public void init() {
		String[] rules = { "./" + BASE_PATH
				+ "/spongecell/guardian/rules/core/person.drl" };

		KieServices kieServices = KieServices.Factory.get();
		KieResources kieResources = kieServices.getResources();
		KieFileSystem kieFileSystem = kieServices.newKieFileSystem();
		KieRepository kieRepository = kieServices.getRepository();

		for (String rule : rules) {
			InputStream ruleIn = getClass().getResourceAsStream(
					"/spongecell/guardian/rules/core/person.drl");
			Assert.assertNotNull(ruleIn);
			String path = "src/main/resources/spongecell/"
					+ "guardian/rules/core/" + rule;
			kieFileSystem.write(path,
					kieResources.newInputStreamResource(ruleIn, "UTF-8"));
		}
		KieBuilder kb = kieServices.newKieBuilder(kieFileSystem);
		kb.buildAll();

		if (kb.getResults().hasMessages(Level.ERROR)) {
			throw new RuntimeException("Build Errors:\n"
					+ kb.getResults().toString());
		}
		KieContainer kieContainer = kieServices.newKieContainer(
			kieRepository.getDefaultReleaseId());

		kieSession = kieContainer.newKieSession();
	}

	/**
	 * For this test to run successfully, the following properties must be set
	 * in the environment:
	 * 
	 * template.properties.groupId : testGroup
	 * template.properties.zookeeperConnect:192.168.33.10:2181
	 * template.properties.kafkaBrokers: 192.168.33.10:9092
	 * 
	 * Also - the default values are not the same as the environment values. The
	 * test will therefore fail, if the environment is not configured correctly.
	 */
	@Test
	public void testPersonAgeRule() {
		Object[] facts = { new Person("John Doe", 21) };
		log.info ("Running the testPersonRule");
		
		for (Object fact : facts) {
			kieSession.insert(fact);
		}
		int numRules = kieSession.fireAllRules();
		Assert.assertEquals(1, numRules);
	}
	
	@Test
	public void testPersonNameRule() {
		Object[] facts = { new Person("John Doe", 21) };
		log.info ("Running the testPersonRule");
		
		for (Object fact : facts) {
			kieSession.insert(fact);
		}
		int numRules = kieSession.fireAllRules();
		Assert.assertEquals(1, numRules);
	}	
}
