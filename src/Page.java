import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable {
	
	private int ID;
	private int max;
	private Vector<Tuple> rows;
	private Object maxClusteringKey;
	String relativeFilePath;

	protected Page(int ID) throws IOException, SecurityException {
		this.ID = ID;
		this.rows = new Vector<Tuple>();
		//InputStream input = Page.class.getResourceAsStream("/DBApp.config");
		Properties properties = new Properties();
		properties.load(new FileInputStream("src/main/resources/DBApp.config"));
		this.max = Integer.parseInt(properties
				.getProperty("MaximumRowsCountinTablePage"));
	}

	public int getID() {
		return ID;
	}

	public void setID(int iD) {
		ID = iD;
	}

	public Vector<Tuple> getRows() {
		return rows;
	}



	public boolean checkPageCapacity() {
		if (this.getSize() < this.max)
			return true;
		else
			return false;
	}

	public int getSize() {
		return this.rows.size();
	}

	public void serializePage(String fileName) {
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
	
	public static Page deserializePage(String fileName) {
		Page page = null;
		try {
			FileInputStream fileIn = new FileInputStream(fileName);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			page = (Page) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return page;
	}

	public Object getMaxClusteringKey() {
		return this.maxClusteringKey;
	}

	public void setMaxClusteringKey() {
		this.maxClusteringKey = this.getRows().lastElement().getPrimaryKey();
	}
	
	public void printPage() {
		for(int i=0;i<this.rows.size();i++) {
			this.rows.get(i).display();
			System.out.println();
		}
		System.out.println("--------------------------------------------------------------------------------");
	}

}
