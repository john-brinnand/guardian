package spongecell.guardian.notification;

import lombok.Getter;
import lombok.Setter;

import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter @Setter 
@ConfigurationProperties(prefix ="simplemailclient")
public class SimpleMailClientConfiguration {
	public String hostname = "smtp.googlemail.com";
	public int smtpPort = 465;
	public String userName = "guardian@spongecell.com"; 
	public String pwd = "677561726469616e!";
	public boolean sslOnConnect = true;
	public String from = "guardian@spongecell.com";
	public String subject = "TestMail";
	public String message = "Greetings from the Guardian Service. Hope you are having a good day... :-)";
	public String [] consumers = { "john.brinnand@spongecell.com" } ;
}
