package org.ss.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 作用：自定义的日志记录器。
 * 1.可以把日志打印到屏幕
 * 2.可以写到日志文件
 * 3.在发生致命错误的时候 终止程序
 * @author zhouhong
 */

public class Loger {

	private static Loger loger = null;
	private BufferedWriter bw = null;
	
	/**
	 * 作用：创建日志文件
	 */
	private Loger() {
		String logFileName = "UnknowClass-" + FormativeTime.getDateToFileName() + ".log";
		try {
			bw = new BufferedWriter(new FileWriter(new File(logFileName)));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 作用：创建日志文件
	 * @param className 作业日志文件名一部分的类名，用于标明产生些日志文件的类
	 */
	private Loger(String className) {
		String logFileName = className + "-" + FormativeTime.getDateToFileName() + ".log";
		try {
			bw = new BufferedWriter(new FileWriter(new File(logFileName)));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 作用：应用设计模式中的单例模式，返回唯一的日志实例
	 * @param className 作业日志文件名一部分的类名，用于标明产生些日志文件的类
	 * @return 返回一个唯一的日志实例
	 */
	public static Loger getInstance(String className) {
		if (loger == null) {
			if (className == null)
				loger = new Loger();
			else
				loger = new Loger(className);
		}
		return loger;
	}

	/**
	 * 作用：把日志写入日志文件
	 * @param level 日志等级
	 * @param msg 待写入日志的消息
	 */
	public void logToFile(ErrorLevel level, String msg) {
		try {
			switch (level) {
			case ERROR:
				bw.write(FormativeTime.getDateToLog() + " ERROR " + msg);
				break;
			case WARM:
				bw.write(FormativeTime.getDateToLog() + " WARM " + msg);
				break;
			case INFO:
				bw.write(FormativeTime.getDateToLog() + " INFO " + msg);
				break;
			default:
				bw.write(FormativeTime.getDateToLog() + " UNKW " + msg);
				break;
			}
			bw.newLine();
			bw.flush();
		} catch (IOException e) {
			logOnScreen(ErrorLevel.WARM,"Loger: Encounter error while writing log");
			e.printStackTrace();
		}
	}

	/**
	 * 作用：把日志写到屏幕
	 * @param level 日志等级
	 * @param msg 待写入日志的消息
	 */
	public static void logOnScreen(ErrorLevel level, String msg) {
		switch (level) {
		case ERROR:
			System.err.println(FormativeTime.getDateToLog() + " ERROR " + msg);
			break;
		case WARM:
			System.err.println(FormativeTime.getDateToLog() + " WARM " + msg);
			break;
		case INFO:
			System.err.println(FormativeTime.getDateToLog() + " INFO " + msg);
			break;
		default:
			System.err.println(FormativeTime.getDateToLog() + " UNKW " + msg);
			break;
		}
	}

	/**
	 * 作用：把日志写入日志文件并打印到屏幕
	 * @param level 日志等级
	 * @param msg 待写入日志的消息
	 */
	public void log(ErrorLevel level, String msg) {
		logOnScreen(level, msg);
		logToFile(level, msg);
	}

	/**
	 * 作用：把日志写入日志文件并打印到屏幕，最后终止程序
	 * @param level 日志等级
	 * @param msg 待写入日志的消息
	 */
	public void logFatalError(ErrorLevel level, String msg) {
		log(level, msg);
		System.exit(1);
	}
}
