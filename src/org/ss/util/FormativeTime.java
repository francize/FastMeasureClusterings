package org.ss.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 作用：自定义的格式化时间
 * @author zhouhong
 */
public class FormativeTime {
	/**
	 * 作用：获取当前时间，用于日志内容
	 * @return 当前时间
	 */
	public static String getDateToLog() {
		Date currentTime = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yy/MM/dd HH:mm:ss");
		String dateString = formatter.format(currentTime);
		return dateString;
	}

	/**
	 * 作用：获取当前时间，用于作为文件名
	 * @return 当前时间
	 */
	public static String getDateToFileName() {
		Date currentTime = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd.HH.mm.ss");
		String dateString = formatter.format(currentTime);
		return dateString;
	}
}
