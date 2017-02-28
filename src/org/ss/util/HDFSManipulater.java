package org.ss.util;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 作用：HDFS文件系统读写类
 * 
 * @author zhouhong
 */

public class HDFSManipulater {

	/**
	 * 作用：获取HDFS中某目录下的文件列表，忽略空文件及子目录
	 * 
	 * @param hdfsDir
	 *            HDFS中的某个目录
	 * @return 文件列表
	 * @throws IOException
	 *             读取文件出错时出现
	 */
	public static ArrayList<Path> readFilesList(String hdfsDir) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(hdfsDir), new Configuration());
		FileStatus[] fstat = fs.listStatus(new Path(hdfsDir));
		ArrayList<Path> pathList = new ArrayList<Path>();
		for (FileStatus fstatus : fstat) {
			if (fstatus.isDirectory() || fstatus.getLen() == 0)
				continue;
			pathList.add(fstatus.getPath());
		}
		return pathList;
	}

	/**
	 * 作用：把本地目录下的所有文件上传至HDFS
	 * 
	 * @param localPath
	 *            本地目录
	 * @param hdfsDir
	 *            HDFS中的目录
	 * @throws IOException
	 *             读取文件出错时出现
	 */
	public static void writeFilesToHDFS(String localPath, String hdfsDir) throws IOException {
		FileSystem hdfs = FileSystem.get(new Configuration());
		hdfs.copyFromLocalFile(new Path(localPath), new Path(hdfsDir));
	}

	/**
	 * 作用：从HDFS中删除一个目录/文件
	 * 
	 * @param hdfsPath
	 *            HDFS中的一个目录/文件的路径
	 * @return true 删除成功 false 删除失败
	 * @throws IOException
	 *             删除文件/目录出错
	 */
	public static boolean deleteHDFS(String hdfsPath) throws IOException {
		FileSystem hdfs = FileSystem.get(new Configuration());
		Path path = new Path(hdfsPath);
		return (hdfs.delete(path, true));
	}
}
