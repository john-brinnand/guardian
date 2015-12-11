package spongecell.guardian.agent.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lombok.Getter;

@Getter
public class Args implements Iterator<Object>{
	private ArrayList<Object> args;
	private Map<String, Object> map;
	private int index;
	
	public Args() {
		args = new ArrayList<Object>();
		index = 0;
		map = new HashMap<String, Object>();
	}

	@Override
	public boolean hasNext() {
		if (args.iterator().hasNext()) {
			return true;
		}
		return false;
	}

	@Override
	public Object next() {
		return args.iterator().next();
	}

	public void addArg(Object arg) {
		args.add(arg);
	}
	
	public void addArg(String key, Object arg) {
		map.put(key, arg);
	}
	
	public Object[] getArgs() {
		Set<Entry<String, Object>> entries = map.entrySet();
		Object [] args = new Object[entries.size()]; 
		int index = 0;
		for (Entry<String, Object> entry : entries) {
			Object value = entry.getValue();
			args[index] = value;
			index++;
		}
		return args;
	}
}
