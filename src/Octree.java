import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Vector;

public class Octree implements Serializable {
	Node root;
	String tableName;
	String indexName;
	String[] strarrColName;

	public Octree(String tableName, String[] arr, Object min1, Object max1, Object min2, Object max2, Object min3,
			Object max3) throws FileNotFoundException, IOException {
		this.tableName = tableName;
		this.strarrColName = arr;
		for (int i = 0; i < arr.length; i++) {
			indexName += arr[i];
		}
		indexName += "Index";
		root = new Node(min1, max1, min2, max2, min3, max3);
	}

	public void insert(Object x, Object y, Object z, String pagePath) throws FileNotFoundException, IOException {
		root.insert(x, y, z, pagePath);
	}

	public Vector<String> search(Object x, Object y, Object z) {
		Vector<String> result = new Vector<String>();
		return root.search(x, y, z, result);

	}

	public void delete(Object x, Object y, Object z, String pagePath) {
		root.delete(x, y, z, pagePath);
	}

	public Vector<String> select(Object startX, Object endX, Object startY, Object endY, Object startZ, Object endZ) {
		Vector<String> pages= new Vector<String>();
		return this.root.getRange(startX, endX, startY, endY, startZ, endZ, pages);

	}
	public void serializeTree(String fileName) {
		try {
			FileOutputStream fileOut = new FileOutputStream(fileName);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(this);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}
	
	public static Octree deserializeTree(String fileName) {
		Octree octree = null;
		try {
			FileInputStream fileIn = new FileInputStream(fileName);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			octree = (Octree) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return octree;
	}

}
