package spongecell.guardian.test;

import java.io.InputStream;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.junit.Assert;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.builder.KieScanner;
import org.kie.api.builder.Message.Level;
import org.kie.api.builder.ReleaseId;
import org.kie.api.builder.model.KieBaseModel;
import org.kie.api.builder.model.KieModuleModel;
import org.kie.api.builder.model.KieSessionModel;
import org.kie.api.conf.EqualityBehaviorOption;
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.event.rule.DebugRuleRuntimeEventListener;
import org.kie.api.io.KieResources;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.conf.ClockTypeOption;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

import spongecell.guardian.listener.RuleEventListener;


@Slf4j
@Getter @Setter
@ConfigurationProperties(prefix="kiesession.handler")
public class KieSessionHandler {
	private String path;
	private KieSession kieSession;
	private KieRepository kieRepository;
	private String[] rules;
	private String basePath = "src/main/resources";
	private String rulesPath = "myPath";

	public KieSessionHandler() {}
	
	/**
	 * Validate from: http://docs.jboss.org/drools/release/6.0.1.Final
	 *  /drools-docs/html/KIEChapter.html#KIEExamplesSection
	 *  
	 *  Example 4.60. Utilize and Run - Java
	 * 
	 * @return
	 */
	@Bean
	@ConfigurationProperties(prefix="kiesession")
	public KieSession buildKieSession() {
		KieServices kieServices = KieServices.Factory.get();
		KieResources kieResources = kieServices.getResources();
		KieFileSystem kieFileSystem = kieServices.newKieFileSystem();
		kieRepository = kieServices.getRepository();

		for (String rule : rules) {
			InputStream ruleIn = getClass().getResourceAsStream("/" + rulesPath + "/" + rule);
			Assert.assertNotNull(ruleIn);
			String path = basePath + "/"  + rulesPath + "/" + rule;
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
		kieSession.addEventListener(new RuleEventListener());
		kieSession.addEventListener(new DebugRuleRuntimeEventListener());		
		return kieSession;
	}
	
	public KieSession buildKIESessionKModuleFile() {
		KieServices kieServices = KieServices.Factory.get();
		
		//************************************************************
		// Use kmodule.xml in the class path.
		// Note the name "ksession1" must match the session name
		// in the kmodule.xml. 
		//************************************************************
		KieContainer kContainer = kieServices.getKieClasspathContainer();
		KieSession kSession = kContainer.newKieSession("ksession1");		
		log.info(kSession.getKieBase().getKiePackages().toString());
		
		return kSession;
	}
	
	public KieSession buildKIESessionKModuleMemoryFileSystem() {
		KieServices kieServices = KieServices.Factory.get();
		KieResources kieResources = kieServices.getResources();
		
		// Create a release identifier, based on maven's 
		// artifact identification: group, artifactId, version.
		//*****************************************************
		ReleaseId releaseId = kieServices.newReleaseId(
			"spongecell", "core", "0.0.1-SNAPSHOT");
		
		//************************************************************
		// Generate the Module, Model and Session. 
		//************************************************************
		KieModuleModel kieModuleModel = kieServices.newKieModuleModel();
	    KieBaseModel kieBaseModel1 = kieModuleModel.newKieBaseModel("KBase1")
	        .setDefault(true)
	        .setEqualsBehavior(EqualityBehaviorOption.EQUALITY)
	        .setEventProcessingMode(EventProcessingOption.CLOUD);

	    kieBaseModel1.newKieSessionModel("KSession1")
	    	.setDefault(true)
	        .setType(KieSessionModel.KieSessionType.STATEFUL)
	        .setClockType(ClockTypeOption.get("realtime"));
	    
	    // Write the rules to the MFS - Memory File System.
	    //*********************************************************
		KieFileSystem kieFileSystem = kieServices.newKieFileSystem();		
		
		for (String rule : rules) {
			InputStream ruleIn = getClass().getResourceAsStream(
					"/" + rulesPath + "/" + rule);
			Assert.assertNotNull(ruleIn);
			String path = basePath + "/"  + rulesPath + "/" + rule;
			kieFileSystem.write(path,
					kieResources.newInputStreamResource(ruleIn, "UTF-8"));
		}	
	    // Write the Module to the MFS - Memory File System.
	    //*********************************************************
	    kieFileSystem.writeKModuleXML(kieModuleModel.toXML());
	    
	    // Write the pom to the MFS - Memory File System.
	    // Note: without this step, the Module cannot be accessed.
	    //*********************************************************
	    kieFileSystem.generateAndWritePomXML(releaseId);
	    log.info(kieModuleModel.toXML());

	    // Build the rules for this module. Note that the 
	    // module is contained within the file system.
	    //*****************************************************
	    KieBuilder kb = kieServices.newKieBuilder(kieFileSystem);
		kb.buildAll();
		
		if (kb.getResults().hasMessages(Level.ERROR)) {
			throw new RuntimeException("Build Errors:\n"
					+ kb.getResults().toString());
		}	   
		KieContainer kieContainer = kieServices.newKieContainer(releaseId); 
	    
		// Create the session. 
	    KieSession kieSession = kieContainer.newKieSession("KSession1");
	
		return kieSession;
	}	
	
	public void scanKieRepository (String groupId, String artifactId,
			String version, String sessionName) {
		KieServices kieServices = KieServices.Factory.get();
		ReleaseId releaseId = kieServices.newReleaseId(groupId, artifactId, version);
		KieContainer kContainer = kieServices.newKieContainer( releaseId );
		KieScanner kScanner = kieServices.newKieScanner( kContainer );
		// Start the KieScanner polling the Maven repository every 10 seconds
		kScanner.scanNow();
		log.debug("Here");
	}
	
	public KieSession getRepositorySession(String groupId, String artifactId,
			String version, String sessionName) {
		KieServices kieServices = KieServices.Factory.get();
		ReleaseId releaseId = kieServices.newReleaseId(groupId, artifactId, version);
		KieContainer kieContainer = kieServices.newKieContainer( releaseId );
	    KieSession kieSession = kieContainer.newKieSession(sessionName);
		return kieSession;
	}
	
	public KieSession buildKieSessionReleaseId(String groupId,
			String artifactId, String version) {
		KieServices kieServices = KieServices.Factory.get();
		KieResources kieResources = kieServices.getResources();
		KieFileSystem kieFileSystem = kieServices.newKieFileSystem();
		kieRepository = kieServices.getRepository();
		ReleaseId releaseId = kieServices.newReleaseId(
			groupId, artifactId, version);
		
		for (String rule : rules) {
			InputStream ruleIn = getClass().getResourceAsStream("/" + rulesPath + "/" + rule);
			Assert.assertNotNull(ruleIn);
			String path = basePath + "/"  + rulesPath + "/" + rule;
			kieFileSystem.write(path,
					kieResources.newInputStreamResource(ruleIn, "UTF-8"));
		}	
		// Write the Module to the MFS - Memory File System.
	    //*********************************************************
	    kieFileSystem.generateAndWritePomXML(releaseId);
	    
		KieBuilder kb = kieServices.newKieBuilder(kieFileSystem);
		kb.buildAll();

		if (kb.getResults().hasMessages(Level.ERROR)) {
			throw new RuntimeException("Build Errors:\n"
					+ kb.getResults().toString());
		}
		KieContainer kieContainer = kieServices.newKieContainer(releaseId);

		kieSession = kieContainer.newKieSession("KSession2");
		kieSession.addEventListener(new RuleEventListener());
		kieSession.addEventListener(new DebugRuleRuntimeEventListener());		
		return kieSession;
	}	
	
	public KieSession buildKIESessionKModuleMemoryFileSystem(
			String groupId, 
			String artifactId, 
			String version, 
			String modelId, 
			String sessionId) {
		KieServices kieServices = KieServices.Factory.get();
		KieResources kieResources = kieServices.getResources();
		
		// Create a release identifier, based on maven's 
		// artifact identification: group, artifactId, version.
		//*****************************************************
		ReleaseId releaseId = kieServices.newReleaseId(groupId, artifactId, version);
		
		// Build the module which contains the knowledge-base 
		// and the session.
		//****************************************************
		KieModuleModel kieModuleModel = buildKieModule(kieServices, modelId, sessionId);
	
		// Store the kieModule and the rules in the memory files system.
		//**************************************************************
		KieFileSystem kieFileSystem = buildKieFileSystem(kieServices,
				kieResources, kieModuleModel, releaseId);
	    
		// Build the container with the compiled knowledge-base
		// and the releaseId.
		//*****************************************************
		KieContainer kieContainer = buildKieContainer(kieServices,
				kieFileSystem, releaseId);
	    
	    KieSession kieSession = kieContainer.newKieSession(sessionId);
	
		return kieSession;
	}	
	
	private KieModuleModel buildKieModule (KieServices kieServices, String modelId, String sessionId) {
		//************************************************************
		// Generate the Module, Model and Session. 
		//************************************************************
		KieModuleModel kieModuleModel = kieServices.newKieModuleModel();
		KieBaseModel kieBaseModel1 = kieModuleModel.newKieBaseModel(modelId)
				.setDefault(true)
				.setEqualsBehavior(EqualityBehaviorOption.EQUALITY)
				.setEventProcessingMode(EventProcessingOption.CLOUD);

		kieBaseModel1.newKieSessionModel(sessionId)
			.setDefault(true)
			.setType(KieSessionModel.KieSessionType.STATEFUL)
			.setClockType(ClockTypeOption.get("realtime"));
		
		return kieModuleModel;
	}

	private KieFileSystem buildKieFileSystem (
		KieServices kieServices, 
		KieResources kieResources, 
		KieModuleModel kieModuleModel,
		ReleaseId releaseId) {
		
	    // Write the rules to the MFS - Memory File System.
	    //*********************************************************
		KieFileSystem kieFileSystem = kieServices.newKieFileSystem();		
		
		for (String rule : rules) {
			InputStream ruleIn = getClass().getResourceAsStream(
					"/" + rulesPath + "/" + rule);
			Assert.assertNotNull(ruleIn);
			String path = basePath + "/"  + rulesPath + "/" + rule;
			kieFileSystem.write(path,
					kieResources.newInputStreamResource(ruleIn, "UTF-8"));
		}	
	    // Write the Module to the MFS - Memory File System.
	    //*********************************************************
	    kieFileSystem.writeKModuleXML(kieModuleModel.toXML());
	    
	    // Write the pom to the MFS - Memory File System.
	    // Note: without this step, the Module cannot be accessed.
	    //*********************************************************
	    kieFileSystem.generateAndWritePomXML(releaseId);
	    log.info(kieModuleModel.toXML());
	    
	    return kieFileSystem;
	}
	
	private KieContainer buildKieContainer(KieServices kieServices,
			KieFileSystem kieFileSystem, ReleaseId releaseId) {
		
		 // Build the rules for this module. Note that the 
	    // module is contained within the file system.
	    //*****************************************************
	    KieBuilder kb = kieServices.newKieBuilder(kieFileSystem);
		kb.buildAll();
		
		if (kb.getResults().hasMessages(Level.ERROR)) {
			throw new RuntimeException("Build Errors:\n"
					+ kb.getResults().toString());
		}	   
		KieContainer kieContainer = kieServices.newKieContainer(releaseId); 
		
	    return kieContainer;
	}	
}