
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class Table implements Serializable {
	private String tableName;
	private Hashtable<String, String> htblColNameMin;
	private Hashtable<String, String> htblColNameMax;
	private Hashtable<String, String> htblColNameType;
	private String strClusteringKeyColumn;
	private Vector<Tuple> vectorOfTuples;
	Vector<Page> vectorOfPages;
	private Vector<Octree> indices;
	// private String[] strarrPagesAddresses;//file path???
	private String primaryKeyCol;// ?
	private int count = 0;

	public Table(String tableName, Hashtable htblColNameType, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax)
			throws SecurityException, IOException {
		this.tableName = tableName;
		this.htblColNameType = htblColNameType;
		this.vectorOfTuples = new Vector<Tuple>();
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		this.htblColNameMax = htblColNameMax;
		this.htblColNameMin = htblColNameMin;
		this.vectorOfPages = new Vector<Page>();
		this.indices = new Vector<Octree>();
		primaryKeyCol = strClusteringKeyColumn;

	}

	public Vector<Octree> getIndices() {
		return indices;
	}

	public Hashtable<String, String> getHtblColNameMin() {
		return htblColNameMin;
	}

	public Hashtable<String, String> getHtblColNameMax() {
		return htblColNameMax;
	}

	public Hashtable<String, String> getTableColType() {
		return this.htblColNameType;
	}

	public void setVectorOfTuples(Vector<Tuple> vectorOfTuples) {
		this.vectorOfTuples = vectorOfTuples;
	}

	public String getTableName() {
		return this.tableName;
	}

	public void insertInPage(Tuple tuple, Object primaryKey) throws SecurityException, IOException, DBAppException {
		if (this.vectorOfPages.size() == 0) {
			Page firstPage = new Page(count);
			count++;
			// firstPage.getRows().add(tuple);
			firstPage.serializePage("src/main/resources/data/" + this.tableName + " P." + firstPage.getID() + ".class");
			this.vectorOfPages.add(firstPage);
		}
		for (int i = 0; i < this.vectorOfPages.size(); i++) {
			Page p = vectorOfPages.get(i);
			// Page p = Page.deserializePage("src/main/resources/data/"+this.tableName + "
			// P." + i + ".class");
			if (p.getSize() == 0) {
				p = Page.deserializePage("src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
				p.getRows().add(tuple);
				insertIndex(tuple, "src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
				p.setMaxClusteringKey();
				p.serializePage("src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
				this.vectorOfPages.set(i, p);
				break;
			} else {
				if (compareToGeneral(primaryKey, p.getMaxClusteringKey()) < 0 && p.checkPageCapacity()) {
					p = Page.deserializePage(
							"src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
					int position = binarySearchInsert(p, primaryKey);
					p.getRows().add(position, tuple);
					insertIndex(tuple, "src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
					p.serializePage("src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
					this.vectorOfPages.set(i, p);
					break;
				} else {
					if (compareToGeneral(primaryKey, p.getMaxClusteringKey()) > 0 && p.checkPageCapacity()) {
						p = Page.deserializePage(
								"src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
						int position = binarySearchInsert(p, primaryKey);
						p.getRows().add(position, tuple);
						insertIndex(tuple, "src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
						p.setMaxClusteringKey();
						p.serializePage("src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
						this.vectorOfPages.set(i, p);
						break;
					} else if (compareToGeneral(primaryKey, p.getMaxClusteringKey()) < 0 && !p.checkPageCapacity()
							&& i < (this.vectorOfPages.size())) {
						p = Page.deserializePage(
								"src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
						Tuple removedTuple = p.getRows().remove(p.getRows().size() - 1);
						// Delete index
						int position = binarySearchInsert(p, primaryKey);
						p.getRows().add(position, tuple);
						insertIndex(tuple, "src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
						p.setMaxClusteringKey();
						p.serializePage("src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
						this.vectorOfPages.set(i, p);
						insertInPage(removedTuple, removedTuple.getPrimaryKey());
						break;
					} else if (compareToGeneral(primaryKey, p.getMaxClusteringKey()) > 0 && !p.checkPageCapacity()
							&& i == (this.vectorOfPages.size()) - 1) {
						Page newPage = new Page(count);
						count++;
						newPage.getRows().add(tuple);
						insertIndex(tuple,
								"src/main/resources/data/" + this.tableName + " P." + newPage.getID() + ".class");
						newPage.setMaxClusteringKey();
						newPage.serializePage(
								"src/main/resources/data/" + this.tableName + " P." + newPage.getID() + ".class");
						this.vectorOfPages.add(newPage);
						break;
					} else if (compareToGeneral(primaryKey, p.getMaxClusteringKey()) < 0 && !p.checkPageCapacity()
							&& i == (this.vectorOfPages.size()) - 1) {
						Page newPage = new Page(count);
						count++;
						newPage.getRows().add(tuple);
						insertIndex(tuple,
								"src/main/resources/data/" + this.tableName + " P." + newPage.getID() + ".class");
						newPage.setMaxClusteringKey();
						newPage.serializePage(
								"src/main/resources/data/" + this.tableName + " P." + newPage.getID() + ".class");
						this.vectorOfPages.add(newPage);
						break;
					}
				}
			}
		}
	}

	public void insertIndex(Tuple tuple, String pagePath) throws FileNotFoundException, IOException {
		Hashtable<String, Object> htblColNameValue = tuple.getHashTable();
		if (this.indices.size() > 0) {
			for (int i = 0; i < this.indices.size(); i++) {
				Octree tree = this.indices.get(i);
				tree = Octree.deserializeTree("src/main/resources/data/" + tree.indexName + ".class");
				String[] columns = tree.strarrColName;
				tree.insert(htblColNameValue.get(columns[0]), htblColNameValue.get(columns[1]),
						htblColNameValue.get(columns[2]), pagePath);
				tree.serializeTree("src/main/resources/data/" + tree.indexName + ".class");
			}
		}
	}

	public int binarySearchInsert(Page p, Object primaryKey) throws DBAppException {// check it
		int low = 0;
		int high = p.getRows().size() - 1;
		int mid = (low + high) / 2;
		int insertionPosition = -100;
		boolean flag = false;
		while (low <= high) {
			if (compareToGeneral(primaryKey, p.getRows().get(mid).getPrimaryKey()) > 0) {
				low = mid + 1;
				mid = (low + high) / 2;
			} else {
				if (compareToGeneral(primaryKey, p.getRows().get(mid).getPrimaryKey()) < 0) {
					high = mid - 1;
					mid = (low + high) / 2;
				} else if (compareToGeneral(primaryKey, p.getRows().get(mid).getPrimaryKey()) == 0) {
					throw new DBAppException("Primary key must be unique");
				}
			}
		}
		insertionPosition = low;
		return insertionPosition;
	}

	public void addTuple(Hashtable<String, Object> htblColNameValue)
			throws SecurityException, IOException, DBAppException {
		Tuple tuple = new Tuple(htblColNameValue, htblColNameValue.get(this.strClusteringKeyColumn));
		vectorOfTuples.add(tuple);
		Object primaryKey = tuple.getPrimaryKey();
		insertInPage(tuple, primaryKey);

	}

	public void updateTuple(Hashtable<String, Object> htblColNameValue, Object strClusteringKeyValue)
			throws FileNotFoundException, IOException {
		Octree tree = null;
		boolean indexExist = false;
		for (int i = 0; i < this.indices.size(); i++) {
			for (String s : this.indices.get(i).strarrColName) {
				if (s.equalsIgnoreCase(this.primaryKeyCol)) {
					indexExist = true;
					tree = this.indices.get(i);
				}

			}
		}

		if (indexExist) {
			Vector<String> x = new Vector<String>();
			if (tree.strarrColName[0].equalsIgnoreCase(primaryKeyCol)) {
				x = tree.search(strClusteringKeyValue, null, null);
			} else {
				if (tree.strarrColName[1].equalsIgnoreCase(primaryKeyCol)) {
					x = tree.search(null, strClusteringKeyValue, null);
				} else {
					x = tree.search(null, null, strClusteringKeyValue);
				}
			}
			
			for(int i = 0;i<x.size();i++) {
				Page p = Page.deserializePage(x.get(i));
				for(int j=0;j<p.getRows().size();j++) {
					Tuple tuple = p.getRows().get(j);
					if(this.compareToGeneral(p.getRows().get(j), strClusteringKeyValue) == 0) {
						for (Octree o : this.indices) {
							Octree.deserializeTree("src/main/resources/data/" + o.indexName + ".class");
							o.delete(tuple.getHashTable().get(o.strarrColName[0]),
									tuple.getHashTable().get(o.strarrColName[1]),
									tuple.getHashTable().get(o.strarrColName[2]),
									"src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
							o.serializeTree("src/main/resources/data/" + o.indexName + ".class");
						}

						for (String key : htblColNameValue.keySet()) {
							tuple.getHashTable().replace(key, htblColNameValue.get(key));
						}
						for (Octree o : this.indices) {
							Octree.deserializeTree("src/main/resources/data/" + o.indexName + ".class");
							o.insert(tuple.getHashTable().get(o.strarrColName[0]),
									tuple.getHashTable().get(o.strarrColName[1]),
									tuple.getHashTable().get(o.strarrColName[2]),
									"src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
							o.serializeTree("src/main/resources/data/" + o.indexName + ".class");
						}
					}
				}
			}

		}
		for (int i = 0; i < this.vectorOfPages.size(); i++) {
			// File file = new File(this.tableName + " P." + i + ".class");
			Page p = this.vectorOfPages.get(i);
			if (compareToGeneral(strClusteringKeyValue, p.getMaxClusteringKey()) <= 0) {
				p = Page.deserializePage("src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
				int low = 0;
				int high = p.getSize() - 1;
				int mid = (low + high) / 2;
				while (low <= high) {
					if (compareToGeneral(strClusteringKeyValue, p.getRows().get(mid).getPrimaryKey()) == 0) {
						Tuple tuple = p.getRows().get(mid);
						for (Octree o : this.indices) {
							Octree.deserializeTree("src/main/resources/data/" + o.indexName + ".class");
							o.delete(tuple.getHashTable().get(o.strarrColName[0]),
									tuple.getHashTable().get(o.strarrColName[1]),
									tuple.getHashTable().get(o.strarrColName[2]),
									"src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
							o.serializeTree("src/main/resources/data/" + o.indexName + ".class");
						}
						for (String key : htblColNameValue.keySet()) {
							tuple.getHashTable().replace(key, htblColNameValue.get(key));
						}
						for (Octree o : this.indices) {
							Octree.deserializeTree("src/main/resources/data/" + o.indexName + ".class");
							o.insert(tuple.getHashTable().get(o.strarrColName[0]),
									tuple.getHashTable().get(o.strarrColName[1]),
									tuple.getHashTable().get(o.strarrColName[2]),
									"src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
							o.serializeTree("src/main/resources/data/" + o.indexName + ".class");
						}
						// this.updateIndex(tuple,
						// "src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
						this.vectorOfPages.set(i, p);
						p.serializePage("src/main/resources/data/" + this.tableName + " P." + i + ".class");
						break;
					} else {
						if (compareToGeneral(strClusteringKeyValue, p.getRows().get(mid).getPrimaryKey()) < 0) {
							high = mid - 1;
							mid = (low + high) / 2;
						} else if (compareToGeneral(strClusteringKeyValue, p.getRows().get(mid).getPrimaryKey()) > 0) {
							low = mid + 1;
							mid = (low + high) / 2;
						}
					}
				}

			}
		}

	}

	public void updateIndex(Tuple t, String pagePath) throws FileNotFoundException, IOException {
		Hashtable<String, Object> colNameValue = t.getHashTable();
		Object x = null;
		Object y = null;
		Object z = null;
		for (int i = 0; i < indices.size(); i++) {
			x = colNameValue.get(indices.get(i).strarrColName[0]);
			y = colNameValue.get(indices.get(i).strarrColName[1]);
			z = colNameValue.get(indices.get(i).strarrColName[2]);
			indices.get(i).delete(x, y, z, pagePath);
			indices.get(i).insert(x, y, z, pagePath);
		}
	}

	public void deleteTuples(Hashtable<String, Object> htblColNameValue) {
		Vector<Vector<String>> result = new Vector<Vector<String>>();
		Set<String> arr = htblColNameValue.keySet();
		boolean useIndex = false;
		for (int i = 0; i < indices.size(); i++) {
			boolean flag = false;
			Octree tree = indices.get(i);
			tree = Octree.deserializeTree("src/main/resources/data/" + tree.indexName + ".class");
			String[] columns = tree.strarrColName;
			for (int j = 0; j < columns.length; i++) {
				if (arr.contains(columns[j])) {
					flag = true;
					useIndex = true;
				}
			}
			if (flag) {
				Object x = htblColNameValue.get(columns[0]);
				Object y = htblColNameValue.get(columns[1]);
				Object z = htblColNameValue.get(columns[2]);
				result.add(tree.search(x, y, z));
				tree.serializeTree("src/main/resources/data/" + tree.indexName + ".class");
			} else {
				tree.serializeTree("src/main/resources/data/" + tree.indexName + ".class");
			}
		}
		while (result.size() >= 2) {
			Vector<String> one = result.remove(0);
			Vector<String> two = result.remove(0);
			one.retainAll(two);
			result.add(0, one);
		}
		if (useIndex) {
			Vector<String> pages = result.get(0);
			for (int i = 0; i < pages.size(); i++) {
				Page p = Page.deserializePage(pages.get(i));
				for (int j = 0; j < p.getSize(); j++) {
					boolean flag = true;
					Hashtable<String, Object> row = p.getRows().get(j).getHashTable();
					for (String key : htblColNameValue.keySet()) {
						if (row.containsKey(key))
							if (compareToGeneral(row.get(key), htblColNameValue.get(key)) != 0)
								flag = false;
					}
					if (flag) {
						Tuple t = p.getRows().remove(j);
						for (int k = 0; k < indices.size(); k++) {
							Octree tree = indices.get(k);
							tree = Octree.deserializeTree("src/main/resources/data/" + tree.indexName + ".class");
							Hashtable<String, Object> tuple = t.getHashTable();
							Object x = tuple.get(tree.strarrColName[0]);
							Object y = tuple.get(tree.strarrColName[1]);
							Object z = tuple.get(tree.strarrColName[2]);
							String pagePath = pages.get(i);
							indices.get(k).delete(x, y, z, pagePath);
							tree.serializeTree("src/main/resources/data/" + tree.indexName + ".class");

						}
						this.vectorOfTuples.remove(t);

					}

				}
				if (p.getSize() == 0) {
					File file2 = new File(pages.get(i));
					file2.delete();
					this.vectorOfPages.remove(i);
				}

				else {
					this.vectorOfPages.set(i, p);
					p.serializePage(pages.get(i));
				}

			}

		} else {
			for (int i = 0; i < this.vectorOfPages.size(); i++) {
				Page p = vectorOfPages.get(i);
				p = Page.deserializePage("src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
				for (int j = 0; j < p.getSize(); j++) {
					boolean flag = true;
					Hashtable<String, Object> row = p.getRows().get(j).getHashTable();
					for (String key : htblColNameValue.keySet()) {
						if (row.containsKey(key))
							if (compareToGeneral(row.get(key), htblColNameValue.get(key)) != 0)
								flag = false;
					}
					if (flag) {
						Tuple t = p.getRows().remove(j);
						this.vectorOfTuples.remove(t);

					}

				}
				if (p.getSize() == 0) {
					File file2 = new File("src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
					file2.delete();
					this.vectorOfPages.remove(i);
				}

				else {
					this.vectorOfPages.set(i, p);
					p.serializePage("src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
				}

			}
		}
	}

	public String getStrClusteringKeyColumn() {
		return this.strClusteringKeyColumn;
	}

	public Vector<Tuple> getVectorOfTuples() {
		return this.vectorOfTuples;
	}

	public int compareToGeneral(Object o1, Object o2) {
		if (o1 instanceof Integer && o2 instanceof Integer)
			return Integer.compare((Integer) o1, (Integer) o2);
		if (o1 instanceof String && o2 instanceof String)
			return ((String) o1).compareTo((String) o2);
		if (o1 instanceof Double && o2 instanceof Double)
			return Double.compare((Double) o1, (Double) o2);
		if (o1 instanceof Date && o2 instanceof Date)
			return ((Date) o1).compareTo((Date) o2);

		return -10;
	}

	public Vector<Tuple> check(SQLTerm[] sqlTerms, String[] operators) throws DBAppException {
		Vector<Tuple> tuples = new Vector<Tuple>();
		for (int i = 0; i < this.vectorOfPages.size(); i++) {
			Page p = this.vectorOfPages.get(i);
			p = Page.deserializePage("src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
			for (int j = 0; j < p.getRows().size(); j++) {
				Tuple t = p.getRows().get(j);
				boolean flag = t.checkTuple(sqlTerms, operators);
				if (flag) {
					tuples.add(t);
				}
			}
			p.serializePage("src/main/resources/data/" + this.tableName + " P." + p.getID() + ".class");
		}
		return tuples;
	}

	public void serializeTable(String fileName) {
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

	public static Table deserializeTable(String fileName) {
		Table table = null;
		try {
			FileInputStream fileIn = new FileInputStream(fileName);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			table = (Table) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return table;
	}

	public void printTable() {
		for (int i = 0; i < vectorOfPages.size(); i++) {
			Page p = vectorOfPages.get(i);
			p.printPage();
		}
	}

}