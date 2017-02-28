package org.ss.util;

import org.apache.hadoop.conf.Configuration;

/**
 * 作用：管理Map/Reduce任务中的配置，主要功能是从其配置中取出用户自定义的配置值。 
 * 其中包含两种配置：
 * 1.阈值或其它数值；
 * 2.hdfs中的路径
 * 
 * @author zhouhong
 * 
 */

public class ConfigUtils {

	public static float getValue(Configuration conf, String property,int defaultValue) {
		return getValue(conf, property, (float) defaultValue);
	}

	public static float getValue(Configuration conf, String property,double defaultValue) {
		
		return getValue(conf, property, (float) defaultValue);
	}

	public static float getValue(Configuration conf, String property,float defaultValue) {
		String value = conf.get(property);
		System.out.println(property + " value:" + value);
		if (value != null) {
			return Float.parseFloat(value);
		} else {
			Loger.logOnScreen(ErrorLevel.WARM, "ConfigUtils: get " + property
					+ " failed, use " + defaultValue + " as default");
			return defaultValue;
		}
	}
	
	public static String getValue(Configuration conf, String property,String defaultValue) {
		String value = conf.get(property);
		if (value != null) {
			return value;
		} else {
			Loger.logOnScreen(ErrorLevel.WARM, "ConfigUtils: get " + property
					+ " failed, use " + defaultValue + " as default");
			return defaultValue;
		}
	}
	
	public static String getPath(Configuration conf, String property,Loger loger) {
		String path = conf.get(property);
		if (path != null) {
			return path;
		} else {
			loger.logFatalError(ErrorLevel.ERROR, "ConfigUtils: get "
					+ property + " failed");
			return null;
		}
	}
}
