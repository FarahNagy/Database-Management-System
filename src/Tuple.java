
import java.io.Serializable;
import java.sql.Date;
import java.util.Hashtable;
import java.util.Vector;

public class Tuple implements Serializable {
	private Hashtable<String, Object> hashTable;
	private Object primaryKey;

	public Hashtable<String, Object> getHashTable() {
		return hashTable;
	}

	public Tuple(Hashtable<String, Object> hashtable, Object primaryKey) {
		this.primaryKey = primaryKey;
		this.hashTable = hashtable;
	}

	public String toString() {
		return hashTable.toString();
	}

	public Object getPrimaryKey() {
		return this.primaryKey;
	}

	public void display() {
		Object[] arr = this.hashTable.keySet().toArray();
		for (int i = 0; i < arr.length; i++) {
			System.out.println(arr[i] + ": " + this.hashTable.get(arr[i]));
			// System.out.println();
		}
	}

	public boolean checkTuple(SQLTerm[] sqlTerms, String[] operators) throws DBAppException {
		if (!(operators.length == sqlTerms.length - 1)) {
			throw new DBAppException("Number operators must be less than SQL Terms by one");
		}
		Vector<Boolean> flags = new Vector<Boolean>();
		for (int i = 0; i < sqlTerms.length; i++) {
			String column = sqlTerms[i]._strColumnName;
			Object value = this.hashTable.get(column);
			Object desiredValue = sqlTerms[i]._objValue;
			String operator = sqlTerms[i]._strOperator;
			if (operator.equalsIgnoreCase(">")) {
				if (compareToGeneral(value, desiredValue) > 0) {
					flags.add(true);

				} else {
					flags.add(false);

				}

			}
			if (operator.equalsIgnoreCase(">=")) {
				if (compareToGeneral(value, desiredValue) >= 0) {
					flags.add(true);

				} else {
					flags.add(false);

				}

			}
			if (operator.equalsIgnoreCase("<")) {
				if (compareToGeneral(value, desiredValue) < 0) {
					flags.add(true);

				} else {
					flags.add(false);

				}

			}
			if (operator.equalsIgnoreCase("<=")) {
				if (compareToGeneral(value, desiredValue) <= 0) {
					flags.add(true);

				} else {
					flags.add(false);

				}
			}
			if (operator.equalsIgnoreCase("!=")) {
				if (compareToGeneral(value, desiredValue) != 0) {
					flags.add(true);

				} else {
					flags.add(false);

				}

			}
			if (operator.equalsIgnoreCase("=")) {
				if (compareToGeneral(value, desiredValue) == 0) {
					flags.add(true);

				} else {
					flags.add(false);

				}

			}
		}
		int count = 0;
		while (flags.size() >= 2&&count<operators.length) {

			boolean one = flags.remove(0);
			boolean two = flags.remove(0);
			boolean flag;
			if (operators[count].equalsIgnoreCase("and")) {
				flag=one && two;
				flags.add(0,flag);
			}
			if (operators[count].equalsIgnoreCase("xor")) {
				flag=(((!one)&&two)||(one&&(!two)));
				flags.add(0,flag);
			}
			if (operators[count].equalsIgnoreCase("or")) {
				flag=one||two;
				flags.add(0,flag);
			}
			// one.retainAll(two);
			count++;
		}
		
		return flags.get(0);
		// >, >=, <, <=, != or =
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

}
