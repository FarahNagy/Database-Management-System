
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ChoiceFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
//import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
//import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

public class DBApp {
	Vector<Table> allTables = new Vector<Table>();
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	//Vector<Octree> allIndices;

	// private FileWriter writer;
	/*
	 * public DBApp() { try { //this.writer = new
	 * FileWriter("src/main/resources/metadata.csv"); //writer.
	 * write("Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType, min, max \n"
	 * ); //writer.close(); } catch (IOException e) { e.printStackTrace(); }
	 * 
	 * }
	 */

	public void init() {
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws DBAppException, IOException {
		File file = new File("src/main/resources/data/" + strTableName + ".class");
		if (file.exists())
			throw new DBAppException();

		if (strTableName == null || strTableName == "")
			throw new DBAppException("Table name cannot be empty");

		checkMinMaxFields(htblColNameType, htblColNameMin, htblColNameMax);

		checkColNameUniqueness(htblColNameType.keySet().toArray());

		checkMinMaxType(htblColNameType, htblColNameMin, htblColNameMax);

		if (!htblColNameType.containsKey(strClusteringKeyColumn))
			throw new DBAppException("name of column to be clustering key does not exist");

		FileWriter myWriter = new FileWriter("src/main/resources/metadata.csv", true);

		Table newTable = new Table(strTableName, htblColNameType, strClusteringKeyColumn, htblColNameMin,
				htblColNameMax);
		this.allTables.add(newTable);
		Object[] types = htblColNameType.keySet().toArray();
		for (int i = 0; i < types.length; i++) {
			String type = (String) types[i];
			String line = "";
			// UPDATE set only the clustering key column true
			if (types[i].equals(strClusteringKeyColumn)) {
				line = newTable.getTableName() + "," + type + "," + htblColNameType.get(type) + "," + "True" + ","
						+ "null" + "," + "null" + "," + newTable.getHtblColNameMin().get(type) + ","
						+ newTable.getHtblColNameMax().get(type) + "\n";
			} else {
				line = newTable.getTableName() + "," + type + "," + htblColNameType.get(type) + "," + "False" + ","
						+ "null" + "," + "null" + "," + newTable.getHtblColNameMin().get(type) + ","
						+ newTable.getHtblColNameMax().get(type) + "\n";
			}

			myWriter.write(line);
		}
		myWriter.close();

		newTable.serializeTable("src/main/resources/data/" + strTableName + ".class");

	}

	public void checkMinMaxType(Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws DBAppException {
		Object[] colNames = htblColNameType.keySet().toArray();
		for (int k = 0; k < colNames.length; k++) {
			String type = htblColNameType.get(colNames[k]);
			String checkMin = htblColNameMin.get(colNames[k]);
			String checkMax = htblColNameMin.get(colNames[k]);
			char[] charArrMin = checkMin.toCharArray();
			char[] charArrMax = checkMax.toCharArray();
			switch (type) {
			case "java.lang.Integer":

				if (!(checkMin.matches("\\d+") && checkMax.matches("\\d+"))) {
					throw new DBAppException("minimum and maximum are not the same type");

				}
				if (Integer.parseInt(checkMin) > Integer.parseInt(checkMax)) {
					throw new DBAppException("minimum is greater than maximum");
				}
				break;
			case "java.lang.double":
				if (!checkMin.matches("-?\\d+(\\.\\d+)?"))
					throw new DBAppException("Not a double");
				if (!checkMax.matches("-?\\d+(\\.\\d+)?"))
					throw new DBAppException("Not a double");

				if (Double.parseDouble(checkMin) > Double.parseDouble(checkMax))
					throw new DBAppException("minimum is greater than maximum");
				break;
			case "java.util.Date":
				try {
					Date dateMin = formatter.parse(checkMin);
					Date dateMax = formatter.parse(checkMax);
					if (dateMin.compareTo(dateMax) > 0)
						throw new DBAppException("minimum is greater than maximum");
					break;
				} catch (ParseException e) {
					throw new DBAppException("not a date");
				}
			default:
				if (checkMin.compareTo(checkMax) > 0)
					throw new DBAppException("minimum is greater than maximum");

			}
		}
	}

	public void checkColNameUniqueness(Object[] arr) throws DBAppException {
		for (int i = 0; i < arr.length; i++) {
			int count = 0;
			String currCheck = (String) arr[i];
			for (int j = 0; j < arr.length; j++) {
				String comparing = (String) arr[j];
				if (comparing.equals(currCheck))
					count++;
			}
			if (count > 1)
				throw new DBAppException("Column name is not unique");
		}
	}

	public void checkMinMaxFields(Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws DBAppException {
		Object[] colNames = htblColNameType.keySet().toArray();
		Object[] colNamesMin = htblColNameMin.keySet().toArray();
		Object[] colNamesMax = htblColNameMax.keySet().toArray();
		if (colNames.length != colNamesMin.length || colNames.length != colNamesMax.length)
			throw new DBAppException("number of fields not equal");
		/*
		 * if(!(colNames.equals(colNamesMax) && colNames.equals(colNamesMax))) { throw
		 * new
		 * DBAppException("column names in max and min are not the same as in column types "
		 * ); }
		 */
		for (int j = 0; j < colNames.length; j++) {
			if (!(htblColNameType.keySet().contains(colNamesMin[j])
					|| htblColNameType.keySet().contains(colNamesMax[j])))
				throw new DBAppException("column names in max and min are not the same as in column types ");

		}
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, SecurityException, ParseException {

		if (!checkRange(strTableName, htblColNameValue)) {
			throw new DBAppException("Invalid values");
		}
		Table table = Table.deserializeTable("src/main/resources/data/" + strTableName + ".class");
		boolean flag = true;
		int count = 0;
		Set<String> setOfKeys = ((Map<String, Object>) htblColNameValue).keySet();
		BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String line = csvReader.readLine();
		while (line != null) {
			String[] currline = line.split(",");
			String tableName = currline[0];
			String columnName = currline[1];
			String columnType = currline[2];
			if (tableName.equals(strTableName)) {
				if (htblColNameValue.containsKey(columnName)) {
					if (!htblColNameValue.get(columnName).getClass().getName().equalsIgnoreCase(columnType)) {
						flag = false;
					}
				} else {
					htblColNameValue.put(columnName, new Null());
				}
				count++;
			}
			line = csvReader.readLine();

		}
		if (!flag) {
			throw new DBAppException("Wrong data types");
		}
		if (htblColNameValue.keySet().size() != count)
			throw new DBAppException("Extra columns");

		if (htblColNameValue.get(table.getStrClusteringKeyColumn()) instanceof Null)
			throw new DBAppException("must contain value for primary key");
		table.addTuple(htblColNameValue);
		table.serializeTable("src/main/resources/data/" + strTableName + ".class");
	}

	public boolean checkWithCSV(String strTableName, Hashtable<String, Object> htblColNameValue) throws IOException {
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String line = reader.readLine();
		boolean f = false;
		for (String colName : htblColNameValue.keySet()) {
			f = false;
			while (line != null) {
				String[] currLine = line.split(",");
				if (currLine[0].equals(strTableName) && currLine[1].equals(colName) && currLine[2].toLowerCase()
						.equals(htblColNameValue.get(colName).getClass().getName().toLowerCase())) {
					f = true;
					break;
				}
				line = reader.readLine();
			}
			if (!f)
				return false;
		}
		reader.close();
		return true;

	}

	public boolean checkRange(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws IOException, DBAppException, ParseException {
		@SuppressWarnings("resource")
		BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String line = reader.readLine();
		// line = reader.readLine();
		for (String colName : htblColNameValue.keySet()) {
			while (line != null) {
				String[] currLine = line.split(",");
				// UPDATE
				Object min = null;
				Object max = null;
				if (currLine[0].equals(strTableName) && currLine[1].equals(colName)) {
					if (currLine[2].equalsIgnoreCase("java.lang.Integer")) {
						min = Integer.parseInt(currLine[6]);
						max = Integer.parseInt(currLine[7]);

					} else {
						if (currLine[2].equalsIgnoreCase("java.lang.double")) {
							min = Double.parseDouble(currLine[6]);
							max = Double.parseDouble(currLine[7]);
						} else {
							if (currLine[2].equalsIgnoreCase("java.lang.String")) {
								min = currLine[6];
								max = currLine[7];
							} else {
								min = formatter.parse(currLine[6]);
								max = formatter.parse(currLine[7]);

							}
						}
					}
					if (!(compareToGeneral(htblColNameValue.get(colName), min) >= 0
							&& compareToGeneral(htblColNameValue.get(colName), max) <= 0)) {
						return false;
					} else {
						break;
					}
				}
				line = reader.readLine();
			}

		}
		reader.close();
		return true;

	}

	public Table getTableByName(String tableName) throws DBAppException {
		int s = this.allTables.size();
		Table t = null;
		for (int i = 0; i < s; i++) {
			if (this.allTables.get(i).getTableName().equalsIgnoreCase(tableName))
				t = this.allTables.get(i);
		}
		if (t == null)
			throw new DBAppException("Table does not exist");
		return t;
	}

	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ParseException {
		Table t = Table.deserializeTable("src/main/resources/data/" + strTableName + ".class"); // getTableByName(strTableName);
		String primaryField = t.getStrClusteringKeyColumn();
		if (htblColNameValue.containsKey(primaryField))
			throw new DBAppException("cant update primary Key");
		if (!checkRange(strTableName, htblColNameValue)) {
			throw new DBAppException("Out of range");
		}
		if (htblColNameValue.containsKey(primaryField))
			throw new DBAppException("Cannot update primary key");
		if (!checkWithCSV(strTableName, htblColNameValue))
			throw new DBAppException("Values to update with are invalid");
		Object clusteringKey;
		if (strClusteringKeyValue.matches("\\d+"))
			clusteringKey = Integer.parseInt(strClusteringKeyValue);
		else if (strClusteringKeyValue.matches("-?\\d+(\\.\\d+)?"))
			clusteringKey = Double.parseDouble(strClusteringKeyValue);
		else if (strClusteringKeyValue.matches("\\d+"))
			clusteringKey = Integer.parseInt(strClusteringKeyValue);
		else {
			try {
				Date date = formatter.parse(strClusteringKeyValue);
				clusteringKey = date;
			} catch (ParseException e) {
				clusteringKey = strClusteringKeyValue;

			}
		}
		t.updateTuple(htblColNameValue, clusteringKey);
		t.serializeTable("src/main/resources/data/" + strTableName + ".class");
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException {
		Table t = Table.deserializeTable("src/main/resources/data/" + strTableName + ".class");// getTableByName(strTableName);
		if (!checkWithCSV(strTableName, htblColNameValue))
			throw new DBAppException("Values to delete with are invalid");
		t.deleteTuples(htblColNameValue);
		t.serializeTable("src/main/resources/data/" + strTableName + ".class");

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

	// new method
	public Object castObject(String type, String object) throws ParseException {
		Object result = null;
		if (type.equalsIgnoreCase("java.lang.Integer")) {
			result = Integer.parseInt(object);
		} else {
			if (type.equalsIgnoreCase("java.lang.double")) {
				result = Double.parseDouble(object);
			} else {
				if (type.equalsIgnoreCase("java.lang.String")) {
					result = object;
				} else {
					formatter.parse(object);
				}
			}
		}
		return result;
	}

	// new method
	public void createIndex(String strTableName, String[] strarrColName)
			throws DBAppException, IOException, ParseException {
		boolean flag = false;
		boolean flagc1 = false;
		boolean flagc2 = false;
		boolean flagc3 = false;
		boolean flagi = false;
		String indexName = "";
		for (int i = 0; i < strarrColName.length; i++) {
			indexName += strarrColName[i];
		}
		indexName += "Index";

		if (strarrColName.length != 3) {
			throw new DBAppException("You need Three columns to create Index");
		}
		Object min1 = null;
		Object min2 = null;
		Object min3 = null;
		Object max1 = null;
		Object max2 = null;
		Object max3 = null;
		BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
		String line = csvReader.readLine();
		while (line != null) {
			String[] currline = line.split(",");
			String tableName = currline[0];
			String columnName = currline[1];
			String index = currline[4];
			if (strTableName.equals(tableName)) {
				flag = true;
				if (columnName.equals(strarrColName[0]) && currline[4].equals("null")) {
					flagc1 = true;
				}
				if (columnName.equals(strarrColName[1]) && currline[4].equals("null")) {
					flagc2 = true;
				}
				if (columnName.equals(strarrColName[2]) && currline[4].equals("null")) {
					flagc3 = true;
				}
				if (index.equals(indexName)) {
					flagi = true;
				}
				if (columnName.equals(strarrColName[0])) {
					min1 = castObject(currline[2], currline[6]);
					max1 = castObject(currline[2], currline[7]);
				}

				if (columnName.equals(strarrColName[1])) {
					min2 = castObject(currline[2], currline[6]);
					max2 = castObject(currline[2], currline[7]);
				}

				if (columnName.equals(strarrColName[2])) {
					min3 = castObject(currline[2], currline[6]);
					max3 = castObject(currline[2], currline[7]);
				}

			}
			line = csvReader.readLine();
		}

		if (flag != true) {
			throw new DBAppException("Table does not Exist");
		}
		if (!(flagc1 && flagc2 && flagc3)) {
			throw new DBAppException("Column does not Exist or has an Index");
		}
		if (strarrColName[0].equals(strarrColName[1]) || strarrColName[0].equals(strarrColName[2])
				|| strarrColName[1].equals(strarrColName[2])) {
			throw new DBAppException("Column name entered more than One time");
		}
		if (flagi) {
			throw new DBAppException("Index Already Exists");
		}

		Octree index = new Octree(strTableName, strarrColName, min1, max1, min2, max2, min3, max3);
		//allIndices.add(index);
		Table t = Table.deserializeTable("src/main/resources/data/" + strTableName + ".class");
		for (int i = 0; i < t.vectorOfPages.size(); i++) {
			Page p = t.vectorOfPages.get(i);
			p = Page.deserializePage("src/main/resources/data/" +strTableName + " P." + p.getID() + ".class");
			for (int j = 0; j < p.getRows().size(); j++) {
				Tuple tuple = p.getRows().get(j);
				Object x = tuple.getHashTable().get(strarrColName[0]);
				Object y = tuple.getHashTable().get(strarrColName[1]);
				Object z = tuple.getHashTable().get(strarrColName[2]);
				index.insert(x, y, z, "src/main/resources/data/" +strTableName + " P." + p.getID() + ".class");
			}
			p.serializePage("src/main/resources/data/" +strTableName + " P." + p.getID() + ".class");
		}
		t.getIndices().add(index);
		File inputFile = new File("src/main/resources/metadata.csv");
		File tempFile = new File("src/main/resources/temp.csv");
		BufferedReader csvreader = new BufferedReader(new FileReader(inputFile));
		FileWriter writer = new FileWriter("src/main/resources/temp.csv");
		String line2 = csvreader.readLine();
		while (line2 != null) {
			boolean flagFound = false;
			String[] data = line2.split(",");
			for (int i = 0; i < strarrColName.length; i++) {
				if (data[0].equalsIgnoreCase(strTableName) && data[1].equalsIgnoreCase(strarrColName[i])) {
					flagFound = true;
					break;
				}
			}

			if (flagFound) {
				data[5] = "Octree";
				data[4] = strarrColName[0] + strarrColName[1] + strarrColName[2] + "Index";
				String currentLine = String.join(",", data);
				writer.write(currentLine + System.getProperty("line.separator"));

			} else {
				String currentLine = String.join(",", data);
				writer.write(currentLine + System.getProperty("line.separator"));
			}

			line2 = csvreader.readLine();
		}

		csvreader.close();
		writer.close();
		
		BufferedReader csvReader2 = new BufferedReader(new FileReader(tempFile));
	    String row2 = csvReader2.readLine();
	    FileWriter writer2 = new FileWriter("src/main/resources/metadata.csv");
	    while(row2 != null) {
	    	writer2.write(row2 +"\n");
	    	row2 = csvReader2.readLine();
	    }
	    
	    tempFile.delete();
	    
	    
	    csvReader2.close();
	    writer2.close();
		index.serializeTree("src/main/resources/data/" + index.indexName + ".class");
		t.serializeTable("src/main/resources/data/" + strTableName + ".class");

	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		String tableName = "";
		Octree tree = null;
		Object[] min = new Object[3];
		Object[] max = new Object[3];
		if (arrSQLTerms.length > 0) {
			tableName = arrSQLTerms[0]._strTableName;
		}
		if (tableName.equals("")) {
			throw new DBAppException("No SQL Terms entered");
		}
		Table t = Table.deserializeTable("src/main/resources/data/" + tableName + ".class");
		boolean sameTable = true;
		for (int i = 0; i < arrSQLTerms.length; i++) {
			if (!tableName.equals(arrSQLTerms[i]._strTableName)) {
				sameTable = false;
			}
		}
		if (!sameTable) {
			throw new DBAppException("We don't support joins");
		}
		Vector<Tuple> result = new Vector<Tuple>();
		boolean foundIndex = false;
		boolean and = true;
		boolean notEqual = false;
		for (String operator : strarrOperators) {
			if (!operator.equalsIgnoreCase("and")) {
				and = false;
			}

		}
		for (SQLTerm sql : arrSQLTerms) {
			if (sql._strOperator.equalsIgnoreCase("!=")) {
				notEqual = true;
			}

		}

		if (arrSQLTerms.length == 3 && (!notEqual) && and) {
			String[] array = new String[3];
			for (int i = 0; i < arrSQLTerms.length; i++) {
				array[i] = arrSQLTerms[i]._strColumnName;
			}
			for (int j = 0; j < t.getIndices().size(); j++) {
				tree = t.getIndices().get(j);
				tree = Octree.deserializeTree("src/main/resources/data/" + tree.indexName + ".class");
				String[] columns = tree.strarrColName;
				if (array.equals(columns)) {
					foundIndex = true;
					break;
				}

			}
			if (foundIndex && tree != null) {
				for (int i = 0; i < tree.strarrColName.length; i++) {
					for (SQLTerm sql : arrSQLTerms) {
						if (sql._strColumnName.equalsIgnoreCase(tree.strarrColName[i])) {
							if (sql._strOperator.equals("=")) {
								min[i] = sql._objValue;
								max[i] = sql._objValue;
							}
							if (sql._strOperator.equals(">=")) {
								min[i] = sql._objValue;

							}
							if (sql._strOperator.equals(">")) {
								min[i] = increment(sql._objValue); // increment

							}
							if (sql._strOperator.equals("<")) {
								max[i] = decrement(sql._objValue); // decrement
							}
							if (sql._strOperator.equals("<=")) {
								max[i] = sql._objValue;
							}
						}

						if (min[i] == null) {
							if (i == 0)
								min[i] = tree.root.minX;
							else {
								if (i == 1)
									min[i] = tree.root.minY;
								else
									min[i] = tree.root.minZ;
							}
						}

						if (max[i] == null) {
							if (i == 0)
								max[i] = tree.root.maxX;
							else {
								if (i == 1)
									max[i] = tree.root.maxY;
								else
									max[i] = tree.root.maxZ;
							}
						}
					}
				}
				Object minX = null;
				Object minY = null;
				Object minZ = null;
				Object maxX = null;
				Object maxY = null;
				Object maxZ = null;
				for (int i = 0; i < min.length; i++) {
					if (i == 0) {
						minX = min[i];
					} else {
						if (i == 1)
							minY = min[i];
						else
							minZ = min[i];
					}

				}
				for (int j = 0; j < max.length; j++) {
					if (j == 0) {
						maxX = max[j];
					} else {
						if (j == 1)
							maxY = max[j];
						else
							maxZ = max[j];
					}
				}
				Vector<String> pages = tree.select(minX, maxX, minY, maxY, minZ, maxZ);
				for (int i = 0; i < pages.size(); i++) {
					Page p = Page.deserializePage(pages.get(i));
					for (int j = 0; j < p.getRows().size(); j++) {
						Tuple tuple = p.getRows().get(j);
						boolean flag = tuple.checkTuple(arrSQLTerms, strarrOperators);
						if (flag) {
							result.add(tuple);
						}
					}
					p.serializePage(pages.get(i));
				}
				tree.serializeTree("src/main/resources/data/" + tree.indexName + ".class");
			} else {
				result = t.check(arrSQLTerms, strarrOperators);
			}
		}
		t.serializeTable("src/main/resources/data/" + tableName + ".class");
		return result.iterator();
	}

	public Object increment(Object value) {

		if (value instanceof Double) {
			return ChoiceFormat.nextDouble((Double) value);
		}

		if (value instanceof Integer) {
			return (int) value + 1;
		}

		if (value instanceof String) {
			String s = (String) value;
			s.toUpperCase();
			int count = 1;
			for (int i = s.length() - 1; i > 0; i--) {
				char c = s.charAt(i);
				String first = s.substring(0, s.length() - count);
				String rest = s.substring(s.length() - (count - 1), s.length());
				if (!(c == 'Z')) {
					c++;
					s = first + c + rest;
					break;
				} else {
					c = 'A';
					s = first + c + rest;
				}
				count++;
			}
			s.toLowerCase();
			return s;

		}
		if (value instanceof Date) {
			Calendar c = Calendar.getInstance();
			c.setTime((Date) value);
			c.add(Calendar.DATE, 1);
			return formatter.format(c.getTime());
		}

		return null;
	}

	public Object decrement(Object value) {

		if (value instanceof Double) {
			return ChoiceFormat.previousDouble(((Double) value));
		}

		if (value instanceof Integer) {
			return (int) value - 1;
		}

		if (value instanceof String) {
			String s = (String) value;
			s.toUpperCase();
			int count = 1;
			for (int i = s.length() - 1; i >= 0; i--) {
				char c = s.charAt(i);
				String first = s.substring(0, s.length() - count);
				String rest = s.substring(s.length() - (count - 1), s.length());
				if (!(c == 'A')) {
					c--;
					s = first + c + rest;
					break;
				} else {
					c = 'Z';
					s = first + c + rest;
				}
				count++;
			}
			return s.toLowerCase();

		}
		if (value instanceof Date) {
			Calendar c = Calendar.getInstance();
			c.setTime((Date) value);
			c.add(Calendar.DATE, -1);
			return formatter.format(c.getTime());
		}
		return null;
	}

}