package spongecell.guardian.application;

import java.util.UUID;

import lombok.Getter;
import lombok.Setter;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties
@Setter @Getter
public class GuardianResourceConfiguration {
	private String  version  = "v1";
	private String  service  = "event_handler_service";
	private String  topic    = service + "_" + version;
	private String  id       = UUID.randomUUID().toString();
	private int     timeout  = 1000;
}
