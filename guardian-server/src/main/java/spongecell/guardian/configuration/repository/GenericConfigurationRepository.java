package spongecell.guardian.configuration.repository;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


@Getter @Setter
@Slf4j
@Component
@Scope("prototype")
public class GenericConfigurationRepository implements IGenericConfigurationRepository {
	public String[] agents = {"step1", "step2", "step3"};
	private @Autowired ApplicationContext ctx;
	private int index = 0;
	private Map<String, String> configMap;
	private Map<String, ArrayList<String>> beanConfigMap;
	
	public GenericConfigurationRepository() {
		this.configMap = configMap;
		this.beanConfigMap = new LinkedHashMap<String, ArrayList<String>>(); 
	}
	
	public GenericConfigurationRepository (Map<String, String>configMap) {
		this.configMap = configMap;
	}
	
	public static Map<String, String> getConfigPrefixes(Class<?> clazz) {
		Map<String, String> beanConfigMap = new HashMap<String, String>(); 
		Method[] methods = clazz.getMethods();
		for (Method method: methods) {
			log.info(method.getName());
			Bean bean = method.getAnnotation(Bean.class);
			if (bean == null) {
				continue;
			}
			String beanName = bean.name()[0];
			ConfigurationProperties props = 
				method.getAnnotation(ConfigurationProperties.class);
			beanConfigMap.put(beanName, props.prefix());
		}
		return beanConfigMap;
	}
	
	public void addBeans(Class<?> clazz) {
		ArrayList<String> configBeanNames =  null;
		Method[] methods = clazz.getMethods();
		for (Method method: methods) {
			log.info(method.getName());
			Bean bean = method.getAnnotation(Bean.class);
			if (bean == null) {
				continue;
			}
			configBeanNames = new ArrayList<String>();
			String beanName = bean.name()[0];
			BeanConfigurations beanConfigs = method.getAnnotation(
					BeanConfigurations.class);
			ConfigurationProperties props = method.getAnnotation(
					ConfigurationProperties.class);		
			DependsOn dependsOn = method.getAnnotation(
					DependsOn.class);		
			
			if (props == null && dependsOn != null) {
				beanConfigMap.put(beanName, configBeanNames);
				continue;
			}
			if (beanConfigs == null && beanConfigMap.get(beanName) == null) {
				configBeanNames.add(props.prefix());
				beanConfigMap.put(beanName, configBeanNames);
				continue;
			}	
			if (beanConfigs == null && beanConfigMap.get(beanName) != null) {
				beanConfigMap.get(beanName).add(props.prefix());
				continue;
			}
			if (beanConfigs.include() == false) {
				continue;
			}
			if (beanConfigMap.get(beanConfigs.parent()) == null) {
				beanConfigMap.put(beanName, configBeanNames);
				continue;
			}
			if (beanConfigMap.get(beanConfigs.parent()) != null) {
				beanConfigMap.get(beanConfigs.parent()).add(props.prefix());
			}
			if (beanConfigs.parent() != null && 
				beanConfigMap.get(beanConfigs.parent()) == null) {
				beanConfigMap.put(beanConfigs.parent(), configBeanNames);
				continue;
			}
		
		}
	}
	
	public void addRegistryBeans(Class<?> clazz) {
		ArrayList<String> configBeanNames =  null;
		Method[] methods = clazz.getMethods();
		for (Method method: methods) {
			log.info(method.getName());
			Bean bean = method.getAnnotation(Bean.class);
			if (bean == null) {
				continue;
			}
			configBeanNames = new ArrayList<String>();
			String beanName = bean.name()[0];
			BeanConfigurations beanConfigs = method.getAnnotation(
					BeanConfigurations.class);
			ConfigurationProperties props = method.getAnnotation(
					ConfigurationProperties.class);		
			if (beanConfigs != null &&beanConfigs.include() == false) {
				continue;
			}			
			// This is the parent of all the beans. It has nothing 
			// above it. Add its properties to the map.
			//****************************************************
			if (beanConfigs != null && 
				beanConfigMap.get(beanConfigs.parent()) == null && props != null) {
				configBeanNames.add(props.prefix());
				beanConfigMap.put(beanConfigs.parent(), configBeanNames);
				continue;
			}
			if (beanConfigs != null && 
				beanConfigMap.get(beanConfigs.parent()) != null && props != null) {
				configBeanNames.add(props.prefix());
				beanConfigMap.get(beanConfigs.parent()).add(props.prefix());
				continue;
			}
			if (beanConfigMap.get(beanName) != null && props != null) {
				configBeanNames.add(props.prefix());
				beanConfigMap.get(beanName).add(props.prefix());
			}
		}
	}
	
	public <T> T getAgent (String agentId) {
		for (int i = 0; i < agents.length; i++) {
			if (agents[i].equals(agentId)) {
				@SuppressWarnings("unchecked")
				T config = (T)ctx.getBean(agentId);
				return config;
			}
		}
		return null;
	}
	
	public Set<Entry<String, String>> getConfigMapEntries() {
		return this.configMap.entrySet();
	}
	
	public Iterator<Entry<String, String>> mapIterator() {
		Set<Entry<String, String>> entries = configMap.entrySet();
		return entries.iterator();
	}
	
	public Iterator<Entry<String, ArrayList<String>>> agentIterator() {
		Set<Entry<String, ArrayList<String>>> entries = beanConfigMap.entrySet();
		return entries.iterator();
	}

	public Entry<String, String> getEntry(String name) {
		Set<Entry<String, String>> entries = configMap.entrySet();
		Iterator<Entry<String, String>> iter = entries.iterator();
		while (iter.hasNext()) {
			Entry<String, String> entry = iter.next();
			if (entry.getKey().equals(name)) {
				return entry;
			}
		}
		return null;
	}
	public Iterator<Object> iterator() {
		return new Iterator<Object>() {

			@Override
			public boolean hasNext() {
				if (agents.length <= index) {
					return false;
				}
				return true;	
			}

			@Override
			public Object next() {
				String id = agents[index];
				Object config = ctx.getBean(id);
				index++;
				return config;
			}
		};
	}

	@Override
	public ApplicationContext getApplicationContext() {
		return ctx;
	}
}