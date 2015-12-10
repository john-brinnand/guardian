package spongecell.guardian.agent.yarn.jobinfo;

import lombok.Getter;
import lombok.Setter;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author jbrinnand
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "mapreduce.jobinfo")
public class ResourceManagerMapReduceJobInfoConfiguration {
	public String baseDir = "/data/guardian";
	public String fileName = "jobinfo-status.txt";
	public String owner = "spongecell";
	public String group = "supergroup";
	public String superUser = "root";
	public String overwrite = "true";
}
