package spongecell.guardian.test;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static spongecell.webhdfs.WebHdfsParams.DEFAULT_PERMISSIONS;
import static spongecell.webhdfs.WebHdfsParams.FILE;
import static spongecell.webhdfs.WebHdfsParams.FILE_STATUS;
import static spongecell.webhdfs.WebHdfsParams.FILE_STATUSES;
import static spongecell.webhdfs.WebHdfsParams.PERMISSION;
import static spongecell.webhdfs.WebHdfsParams.TYPE;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.mail.EmailException;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.junit.Assert;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.builder.Message.Level;
import org.kie.api.event.rule.DebugRuleRuntimeEventListener;
import org.kie.api.io.KieResources;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import spongecell.guardian.listener.RuleEventListener;
import spongecell.guardian.model.HDFSDirectory;
import spongecell.guardian.notification.GuardianEvent;
import spongecell.guardian.notification.SimpleMailClient;
import spongecell.guardian.notification.SlackGuardianWebHook;
import spongecell.webhdfs.FilePath;
import spongecell.webhdfs.WebHdfsConfiguration;
import spongecell.webhdfs.WebHdfsOps;
import spongecell.webhdfs.WebHdfsWorkFlow;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
@ContextConfiguration(classes = { HDFSBusinessRulesTest.class, WebHdfsWorkFlow.Builder.class, SimpleMailClient.class})
@EnableConfigurationProperties ({ WebHdfsConfiguration.class })
public class HDFSBusinessRulesTest extends AbstractTestNGSpringContextTests {
	private final static String BASE_PATH = "src/main/resources";
	private final static String RULES_PATH = "spongecell/guardian/rules/core";
	private KieSession kieSession;
	@Autowired WebHdfsWorkFlow.Builder webHdfsWorkFlowBuilder;
	@Autowired WebHdfsConfiguration webHdfsConfig;
	@Autowired SimpleMailClient simpleMailClient;
	
	@Test
	public void validateWorkFlowCreateDirFile() throws NoSuchMethodException, 
		SecurityException, UnsupportedEncodingException, URISyntaxException {
		StringEntity entity = new StringEntity("Greetings earthling!\n");
		FilePath path = getFilePathDTF();
		String fileName = path.getFile().getPath() + File.separator + webHdfsConfig.getFileName();
		
		WebHdfsWorkFlow workFlow = webHdfsWorkFlowBuilder
			.path(path.getFile().getPath())
			.addEntry("CreateBaseDir", 
				WebHdfsOps.MKDIRS, 
				HttpStatus.OK, 
				path.getFile().getPath())
			.addEntry("SetOwnerBaseDir",
				WebHdfsOps.SETOWNER, 
				HttpStatus.OK, 
				webHdfsConfig.getBaseDir(), 
				webHdfsConfig.getOwner(), 
				webHdfsConfig.getGroup())
			.addEntry("CreateAndWriteToFile", 
				WebHdfsOps.CREATE, 
				HttpStatus.CREATED, 
				entity)
			.addEntry("SetOwnerFile", WebHdfsOps.SETOWNER, 
				HttpStatus.OK, 
				fileName,
				webHdfsConfig.getOwner(), 
				webHdfsConfig.getGroup())
			.build();
		CloseableHttpResponse response = workFlow.execute(); 
		Assert.assertNotNull(response);
		Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.OK.value());
	}
	
	@Test(dependsOnMethods="validateWorkFlowCreateDirFile")
	public void validateWorkFlowFileCreateWrite() throws URISyntaxException, IOException {
		// Override the default file name.
		//********************************
		webHdfsConfig.setFileName("testfile1.txt");
		StringEntity entity = new StringEntity("Greetings earthling!\n");
		FilePath path = getFilePathDTF();
		String fileName = path.getFile().getPath() + File.separator + "testfile1.txt";
		
		WebHdfsWorkFlow workFlow = webHdfsWorkFlowBuilder
			.path(path.getFile().getPath())
			.addEntry("CreateAndWriteToFile", 
				WebHdfsOps.CREATE, 
				HttpStatus.CREATED, 
				entity)
			.addEntry("SetOwnerFile", WebHdfsOps.SETOWNER, 
				HttpStatus.OK, 
				fileName,
				webHdfsConfig.getOwner(), 
				webHdfsConfig.getGroup())
			.build();
		CloseableHttpResponse response = workFlow.execute(); 
		Assert.assertNotNull(response);
		Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.OK.value());	
	}
	
	@Test(dependsOnMethods="validateWorkFlowFileCreateWrite")
	public void validateWebHdfsListStatus() throws URISyntaxException,
		IOException, EmailException {
		String[] rules = { 
			"/" + RULES_PATH + "/" + "hdfs-directory.drl",
			"/" + RULES_PATH + "/" + "hdfs-directory-notification.drl",
			"/" + RULES_PATH + "/" + "hdfs-heston.drl",
		}; 
		initKie(rules);
		
		FilePath path = getFilePathDTF();
		
		WebHdfsWorkFlow workFlow = webHdfsWorkFlowBuilder
			.path(path.getFile().getPath())
			.addEntry("ListDirectoryStatus", 
				WebHdfsOps.LISTSTATUS, 
				HttpStatus.OK, 
				webHdfsConfig.getBaseDir())
			.build();
		
		CloseableHttpResponse response = workFlow.execute(); 
		Assert.assertNotNull(response);
		Assert.assertEquals(HttpStatus.OK.value(), response.getStatusLine().getStatusCode());			
		
		ArrayNode fileStatus = getFileStatus(response);
		
		//*******************************************
		// Test the rule.
		//*******************************************
		HDFSDirectory hdfsDir = new HDFSDirectory();
		hdfsDir.setNumChildren(fileStatus.size());
		hdfsDir.setOwner("root");
		hdfsDir.setFileStatus(fileStatus);
		
		Object[] facts = { hdfsDir, simpleMailClient };
		for (Object fact : facts) {
			kieSession.insert(fact);
		}
		int numRules = kieSession.fireAllRules();
		Assert.assertEquals(5, numRules);
	}
	
	@Test(dependsOnMethods="validateWorkFlowFileCreateWrite")
	public void validateWebHdfsListStatusEvent() throws URISyntaxException,
		IOException, EmailException {
		String[] rules = { 
			"/" + RULES_PATH + "/" + "hdfs-directory-notification.drl",
			"/" + RULES_PATH + "/" + "hdfs-heston.drl",
		}; 	
		initKie(rules);
		
		FilePath path = getFilePathDTF();
		
		WebHdfsWorkFlow workFlow = webHdfsWorkFlowBuilder
			.path(path.getFile().getPath())
			.addEntry("ListDirectoryStatus", 
				WebHdfsOps.LISTSTATUS, 
				HttpStatus.OK, 
				webHdfsConfig.getBaseDir())
			.build();
		
		CloseableHttpResponse response = workFlow.execute(); 
		Assert.assertNotNull(response);
		Assert.assertEquals(HttpStatus.OK.value(), response.getStatusLine().getStatusCode());			
		
		ArrayNode fileStatus = getFileStatus(response);
		
		//*******************************************
		// Test the rule.
		//*******************************************
		HDFSDirectory hdfsDir = new HDFSDirectory();
		hdfsDir.setNumChildren(fileStatus.size());
		hdfsDir.setOwner("root");
		hdfsDir.setFileStatus(fileStatus);
		hdfsDir.setTargetDir(path.getFile().getPath());
		
		GuardianEvent event = new GuardianEvent();
		event.dateTime = LocalDateTime.now( ).toString();
		event.absolutePath = webHdfsConfig.getBaseDir();
		event.setEventSeverity(GuardianEvent.severity.INFORMATIONAL.name());
		
		SlackGuardianWebHook slackClient = new SlackGuardianWebHook();
		
		Object[] facts = { hdfsDir, simpleMailClient, event, slackClient };
		for (Object fact : facts) {
			kieSession.insert(fact);
		}
		int numRules = kieSession.fireAllRules();
		Assert.assertEquals(6, numRules);
	}	
	
	private FilePath getFilePathDTF() {
		Assert.assertNotNull(webHdfsWorkFlowBuilder);
		
		// Override the default file name.
		//********************************
		DateTimeFormatter customDTF = new DateTimeFormatterBuilder()
	        .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
	        .appendValue(MONTH_OF_YEAR, 2)
	        .appendValue(DAY_OF_MONTH, 2)
	        .toFormatter();	
		
		FilePath path = new FilePath.Builder()
			.addPathSegment("data")
			.addPathSegment(customDTF.format(LocalDate.now()))
			.build();
		
		return path;
	}
	
	private ArrayNode getFileStatus(CloseableHttpResponse response) 
			throws JsonParseException, JsonMappingException, ParseException, IOException {
		ObjectNode dirStatus = new ObjectMapper().readValue(
			EntityUtils.toString(response.getEntity()), 
			new TypeReference<ObjectNode>() {
		});
		log.info("Directory status is: {} ", new ObjectMapper()
			.writerWithDefaultPrettyPrinter()
			.writeValueAsString(dirStatus));
		
		ArrayNode fileStatus  = new ObjectMapper().readValue(dirStatus
			.get(FILE_STATUSES)
			.get(FILE_STATUS).toString(),
			new TypeReference<ArrayNode>() { 
		});
		for (int i = 0; i < fileStatus.size(); i++) {
			JsonNode fileStatusNode = fileStatus.get(i);
			Assert.assertEquals(fileStatusNode.get(TYPE).asText(), FILE);
			Assert.assertEquals(fileStatusNode.get(PERMISSION).asText(), DEFAULT_PERMISSIONS);
		}		
		return fileStatus;
	}
	/**
	 * Configurable variables are: 
	 * 		path - the path to the rules.
	 * 		released version - the version of the rules. 
	 * 		release Id - the ID of the release. 
	 * 		session name - the name of the session.
	 * 
	 * @param rules
	 */
	private void initKie(String [] rules) {
		KieServices kieServices = KieServices.Factory.get();
		KieResources kieResources = kieServices.getResources();
		KieFileSystem kieFileSystem = kieServices.newKieFileSystem();
		KieRepository kieRepository = kieServices.getRepository();

		for (String rule : rules) {
			InputStream ruleIn = getClass().getResourceAsStream(rule);
			Assert.assertNotNull(ruleIn);
			String path = BASE_PATH + "/"  + RULES_PATH + "/" + rule;
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

		kieSession = kieContainer.newKieSession("hdfsDirCheckRule");
		kieSession.addEventListener(new RuleEventListener());
		kieSession.addEventListener(new DebugRuleRuntimeEventListener());		
	}
	
	@Test
	public void validateGenerateExcelWorkBook() throws IOException {
		Workbook workBook = new HSSFWorkbook();
	    //Workbook wb = new XSSFWorkbook();
	    CreationHelper createHelper = workBook.getCreationHelper();
	    Sheet sheet = workBook.createSheet("new sheet");

	    // Create a row and put some cells in it. Rows are 0 based.
	    Row row = sheet.createRow((short)0);
	    // Create a cell and put a value in it.
	    Cell cell = row.createCell(0);
	    cell.setCellValue(1);

	    // Or do it on one line.
	    row.createCell(1).setCellValue(1.2);
	    row.createCell(2).setCellValue(
	         createHelper.createRichTextString("This is a string"));
	    row.createCell(3).setCellValue(true);

	    // Write the output to a file
	    FileOutputStream fileOut = new FileOutputStream("workbook.xls");
	    workBook.write(fileOut);
	    fileOut.close();	
	    workBook.close();
	}
}	