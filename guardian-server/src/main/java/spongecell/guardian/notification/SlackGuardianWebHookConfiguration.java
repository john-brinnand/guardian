package spongecell.guardian.notification;

import lombok.Getter;
import lombok.Setter;

import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter @Setter 
@ConfigurationProperties(prefix ="guardian")
public class SlackGuardianWebHookConfiguration {
	/**
	 * https://hooks.slack.com/services/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
	 */
	public String scheme = "https";
	public String host = "hooks.slack.com";
	public String resource = "services"; 
	public String guardianCertificate = "T024F5RBX/B0CEA45U3/ZcNNJNwzkAOhiuXK87YxfSro";
}
