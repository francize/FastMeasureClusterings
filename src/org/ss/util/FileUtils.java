package org.ss.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 作用：本地文件管理类
 * 
 * @author zhouhong
 */
public class FileUtils {

	/**
	 * 作用：在系统临时目录中创建一个临时的子目录
	 * 
	 * @param clazz
	 *            用于作为临时子目录名一部分的字符串
	 * @return 临时子目录绝对路径
	 */
	public static String generateLocalDirPath(@SuppressWarnings("rawtypes") Class clazz) {
		return System.getProperty("java.io.tmpdir") + File.separator + clazz.getSimpleName()
				+ System.currentTimeMillis();
	}

	/**
	 * 作用：以给定的文件的文件名在指定的目录下构造相应的目录(只构造目录名，不创建目录)
	 * 
	 * @param path
	 *            上级目录的绝对路径
	 * @param fileName
	 *            用于作为目录名基础的文件名
	 * @return 构造出来的目录的绝对路径
	 */
	public static File createGroupFile(String path, String fileName) {
		return new File(path + File.separator + "group_" + fileName);
	}

	/**
	 * 作用：清空指定目录下的所有文件
	 * 
	 * @param directory
	 *            待清空文件的目录路径
	 */
	public static void cleanDirectory(String directory) {
		removeDir(new File(directory));
		File dir = new File(directory);
		dir.mkdirs();
	}

	/**
	 * 作用：删除指定目录
	 * 
	 * @param dir
	 *            待删除的目录
	 * @return true 删除成功 false 删除失败
	 */
	public static boolean removeDir(File dir) {
		if (!dir.exists())
			return true;
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = removeDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}
		return dir.delete();
	}

	/**
	 * 作用：获取配置文件的路径
	 * 
	 * @param cwd
	 *            配置文件所在目录的上级目录
	 * @param fileName
	 *            配置文件的文件名
	 * @return 配置文件的路径
	 */
	public static String getFilePath(String cwd, String fileName) {
		String path = cwd + File.separator + fileName;
		System.out.println("Path: " + path);
		File file = new File(path);
		return file.exists() ? path : null;
	}

	/**
	 * 作用：把一个节点信息写入指定文件
	 * 
	 * @param node
	 *            一个节点的信息
	 * @param file
	 *            待写入的目标文件
	 * @throws IOException
	 *             写入文件出错
	 */
	public static void writeNode(String node, File file) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
		bw.write(node + "\t");
		bw.close();
	}
}
