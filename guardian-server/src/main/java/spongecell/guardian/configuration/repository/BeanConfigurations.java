package spongecell.guardian.configuration.repository;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface BeanConfigurations {

	/**
	 * The configurations associated with a Bean.
	 */
	String[] beans() default {};
	
	String parent() default ""; 
	
	boolean include() default true; 
}
