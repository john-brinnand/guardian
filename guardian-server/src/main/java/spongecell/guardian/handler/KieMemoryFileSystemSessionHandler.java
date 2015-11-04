package spongecell.guardian.handler;

import java.io.InputStream;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieModule;
import org.kie.api.builder.KieRepository;
import org.kie.api.builder.KieScanner;
import org.kie.api.builder.Message.Level;
import org.kie.api.builder.ReleaseId;
import org.kie.api.builder.model.KieBaseModel;
import org.kie.api.builder.model.KieModuleModel;
import org.kie.api.builder.model.KieSessionModel;
import org.kie.api.conf.EqualityBehaviorOption;
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.io.KieResources;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.conf.ClockTypeOption;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;

/**
 * @author jbrinnand
 */
@Slf4j
@Getter @Setter
@ConfigurationProperties(prefix="kiesession.handler")
public class KieMemoryFileSystemSessionHandler {
	private String path;
	private KieScanner kieScanner;
	private KieRepository kieRepository = null;
	private String[] rules;
	private String basePath = "src/main/resources";
	private String rulesPath = "myPath";

	private KieMemoryFileSystemSessionHandler() {}

	public Builder newBuilder () {
		return new Builder();
	}
	/**
	 * Builder which accepts the properties
	 * provided by a user to register their
	 * session.
	 * 
	 * @author jbrinnand
	 *
	 */
	public class Builder {
		private String groupId;
		private String artifactId;
		private String version;
		private String modelId;
		private String sessionId;
		
		public Builder addGroupId (String groupId) {
			this.groupId = groupId;
			return this;
		}
		
		public Builder addArtifactId (String artifactId) {
			this.artifactId = artifactId;
			return this;
		}	
		
		public Builder addVersion (String version) {
			this.version = version;
			return this;
		}	
		
		public Builder addModelId (String modelId) {
			this.modelId = modelId;
			return this;
		}	
		
		public Builder addSessionId (String sessionId ) {
			this.sessionId = sessionId;
			return this;
		}		
		
		public Builder addRules (String rules ) {
			return this;
		}
		
		public KieSession build () {
			KieServices kieServices = KieServices.Factory.get();
			KieResources kieResources = kieServices.getResources();
			if (kieRepository == null) {
				kieRepository = kieServices.getRepository();
			}
			// Create a release identifier, based on maven's 
			// artifact identification: group, artifactId, version.
			//*****************************************************
			ReleaseId releaseId = kieServices.newReleaseId(
					groupId, artifactId, version);

			// Build the module which contains the knowledge-base 
			// and the session.
			//****************************************************
			KieModuleModel kieModuleModel = buildKieModule(
					kieServices, modelId, sessionId);

			// Store the kieModule and the rules in the memory files system.
			//**************************************************************
			KieFileSystem kieFileSystem = buildKieFileSystem(kieServices,
					kieResources, kieModuleModel, releaseId);

			// Build the container with the compiled knowledge-base
			// and the releaseId.
			//*****************************************************
			KieContainer kieContainer = buildKieContainer(kieServices,
					kieFileSystem, releaseId);
			
			// Start polling the Maven repository. 
//			if (kieScanner == null) {
//				kieScanner = kieServices.newKieScanner( kieContainer );
//				kieScanner.start( 10000L );
//			}
			KieSession kieSession = kieContainer.newKieSession(sessionId);

			return kieSession;
		}	
	}
	
	/**
	 * Build the KieModule.
	 * 
	 * "13.2.1. KieModule
 	 * The <kie:kmodule> defines a collection of KieBase and associated KieSession's. 
 	 * The kmodule tag has one MANDATORY parameter 'id'.
	 * A kmodule tag can contain only the following tags as children. kie:kbase "
	 *
	 * @param kieServices
	 * @param modelId
	 * @param sessionId
	 * @return
	 */
	private KieModuleModel buildKieModule(KieServices kieServices,
			String modelId, String sessionId) {
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

	/**
	 * Build the KieFileSystem. It contains, the rules and the module
	 * which contains the session and its packages.
	 * 
	 * @param kieServices
	 * @param kieResources
	 * @param kieModuleModel
	 * @param releaseId
	 * @return
	 */
	private KieFileSystem buildKieFileSystem (
		KieServices kieServices, 
		KieResources kieResources, 
		KieModuleModel kieModuleModel,
		ReleaseId releaseId) {
		
		log.info("Rules path is: {} ", rulesPath);
		
	    // Write the rules to the MFS - Memory File System.
	    //*********************************************************
		KieFileSystem kieFileSystem = kieServices.newKieFileSystem();		
		
		for (String rule : rules) {
			InputStream ruleIn = getClass().getResourceAsStream(
					"/" + rulesPath + "/" + rule);
			log.info("Rules path is: {} ", rulesPath);
			Assert.isTrue(ruleIn != null, 
				"InputStream is null. Probable cause: Rules Resource "
				+ "Path is invalid: " + rulesPath);
			String path = basePath + "/"  + rulesPath + "/" + rule;
			kieFileSystem.write(path,
					kieResources.newInputStreamResource(ruleIn, "UTF-8"));
		}	
	    // Write the Module to the Memory File System (MFS).
	    //****************************************************
	    kieFileSystem.writeKModuleXML(kieModuleModel.toXML());
	    
	    // Write the pom to the MFS - Memory File System.
	    // Note: without this step, the Module cannot be accessed.
	    //********************************************************
	    kieFileSystem.generateAndWritePomXML(releaseId);
	    log.info(kieModuleModel.toXML());
	    
	    return kieFileSystem;
	}
	
	/**
	 * Build the container using the stored releaseId.
	 * 
	 * @param kieServices
	 * @param kieFileSystem
	 * @param releaseId
	 * @return
	 */
	private KieContainer buildKieContainer(KieServices kieServices,
		KieFileSystem kieFileSystem, ReleaseId releaseId) {
		
		// Build the rules for this module. Note that the 
	    // module is contained within the file system.
		// And the 'kieModule is automatically deployed to the
		// KieRepository if it is successfully built'.
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
	
	/**
	 * Get the session from the repository. 
	 * 
	 * @param groupId
	 * @param artifactId
	 * @param version
	 * @param sessionName
	 * @return
	 */
	public KieSession getRepositorySession(String groupId, 
		String artifactId, String version, String sessionName) {
		KieServices kieServices = KieServices.Factory.get();
		ReleaseId releaseId = kieServices.newReleaseId(groupId, artifactId, version);
		KieContainer kieContainer = kieServices.newKieContainer(releaseId);
	    KieSession kieSession = kieContainer.newKieSession(sessionName);
		return kieSession;
	}	
	
	public KieModule getModule(String groupId, 
		String artifactId, String version, String sessionName) {
		KieServices kieServices = KieServices.Factory.get();
		ReleaseId releaseId = kieServices.newReleaseId(groupId, artifactId, version);
		KieModule kieModule = kieServices.getRepository().getKieModule(releaseId);
		return kieModule;
	}	
}