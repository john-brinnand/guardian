package spongecell.guardian.notification;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailAttachment;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.MultiPartEmail;
import org.apache.commons.mail.SimpleEmail;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@Slf4j
@Getter @Setter
@EnableConfigurationProperties (SimpleMailClientConfiguration.class)
public class SimpleMailClient {
	private boolean sendMail = false;
	private boolean valid = true;
	private String message;
	@Autowired SimpleMailClientConfiguration smConfig;
	
	public SimpleMailClient () {
		this.message = "Greetings from the Guardian. I hope you are having a good day.";
	}
	
	public void send () throws EmailException {
		log.info ("Sending mail.");
		Email email = new SimpleEmail();
		email.setHostName("smtp.googlemail.com");
		email.setSmtpPort(465);
		email.setAuthenticator(new DefaultAuthenticator("guardian@spongecell.com", "677561726469616e!"));
		email.setSSLOnConnect(true);
		email.setFrom("guardian@spongecell.com");
		email.setSubject("TestMail");
		email.setMsg(message);
		email.addTo("john.brinnand@spongecell.com");
		email.send();	
	}
	
	public void send (String msg) throws EmailException {
		Email email = new SimpleEmail();
		email.setHostName(smConfig.getHostname());
		email.setSmtpPort(smConfig.getSmtpPort());
		email.setAuthenticator(new DefaultAuthenticator(smConfig.getUserName(), smConfig.getPwd()));
		email.setSSLOnConnect(smConfig.isSslOnConnect());
		email.setFrom(smConfig.getFrom());
		email.setSubject(smConfig.getSubject());
		email.setMsg(msg);
		for  (String consumer : Arrays.asList(smConfig.getConsumers())) {
			email.addTo(consumer);
		}
		email.send();	
	}	
	
	public void multipartSend (String msg) throws EmailException {
		EmailAttachment attachment = new EmailAttachment();
		attachment.setPath("/Users/jbrinnand/workspace/spongecell/guardian/workbook.xls");
		attachment.setDisposition(EmailAttachment.INLINE);
		attachment.setDescription("Spreadsheet.");
		attachment.setName("workbook.xls");
		
		MultiPartEmail email = new MultiPartEmail();
		email.setHostName(smConfig.getHostname());
		email.setSmtpPort(smConfig.getSmtpPort());
		email.setAuthenticator(new DefaultAuthenticator(smConfig.getUserName(), smConfig.getPwd()));
		email.setSSLOnConnect(smConfig.isSslOnConnect());
		email.setFrom(smConfig.getFrom());
		email.setSubject(smConfig.getSubject());
		email.setMsg(msg);
		for  (String consumer : Arrays.asList(smConfig.getConsumers())) {
			email.addTo(consumer);
		}
		email.attach(attachment);
		email.send();	
	}	
	
	public void sendExcelAttachment (String msg, Workbook workBook) throws EmailException, IOException {
		 // Write the output to a file
	    FileOutputStream fileOut = new FileOutputStream("./workbook.xls");
	    workBook.write(fileOut);
	    fileOut.close();	
	    workBook.close();
	    
		EmailAttachment attachment = new EmailAttachment();
		attachment.setPath("./workbook.xls");
		attachment.setDisposition(EmailAttachment.INLINE);
		attachment.setDescription("Spreadsheet.");
		attachment.setName("workbook.xls");
		
		MultiPartEmail email = new MultiPartEmail();
		email.setHostName(smConfig.getHostname());
		email.setSmtpPort(smConfig.getSmtpPort());
		email.setAuthenticator(new DefaultAuthenticator(smConfig.getUserName(), smConfig.getPwd()));
		email.setSSLOnConnect(smConfig.isSslOnConnect());
		email.setFrom(smConfig.getFrom());
		email.setSubject(smConfig.getSubject());
		email.setMsg(msg);
		for  (String consumer : Arrays.asList(smConfig.getConsumers())) {
			email.addTo(consumer);
		}
		email.attach(attachment);
		email.send();	
	}	
}
