package org.ss.util;

import java.util.HashMap;

public class UnionFindSet {

	public static boolean union(long src, long dest, HashMap<Long, Long> tree, HashMap<Long, Integer> size) {
		if (!tree.containsKey(src)) {
			tree.put(src, src);
			size.put(src, 1);
		}
		if (!tree.containsKey(dest)) {
			tree.put(dest, dest);
			size.put(dest, 1);
		}
		long rootSrc = find(src, tree);
		long rootDest = find(dest, tree);
		if (0 == rootSrc || 0 == rootDest) {
			Loger loger = Loger.getInstance(null);
			loger.log(ErrorLevel.ERROR, "MergeFindSet: Using merge find set failed");
			System.exit(1);
		}

		if (rootDest == rootSrc)
			return false;

		int sizeSrc = size.get(rootSrc);
		int sizeDest = size.get(rootDest);
		if (sizeSrc < sizeDest) {
			tree.put(rootSrc, rootDest);
			size.put(rootDest, sizeSrc + sizeDest);
		} else {
			tree.put(rootDest, rootSrc);
			size.put(rootSrc, sizeSrc + sizeDest);
		}
		return true;
	}

	public static long find(long src, HashMap<Long, Long> tree) {
		if (!tree.containsKey(src))
			return 0;

		long tmp = src;
		while (tmp != tree.get(tmp)) {
			tree.put(tmp, tree.get(tree.get(tmp)));
			tmp = tree.get(tmp);
		}
		return tmp;
	}
}
