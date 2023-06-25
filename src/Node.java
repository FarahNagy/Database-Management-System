import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.Period;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public class Node implements Serializable {
//private Node data;
	private Vector<Point> points;
	private Vector<Node> children;
	Object minX;
	Object minY;
	Object minZ;
	Object maxX;
	Object maxY;
	Object maxZ;
	int max;
	Object middleX;
	Object middleY;
	Object middleZ;

	public Node(Object min1, Object max1, Object min2, Object max2, Object min3, Object max3)
			throws FileNotFoundException, IOException {
		// this.data=data;
		// children.setSize(8);
		// this.children = children;
		this.minX = min1;
		this.minY = min2;
		this.minZ = min3;
		this.maxX = max1;
		this.maxY = max2;
		this.maxZ = max3;
		this.middleX = this.getMiddle(min1, max1);
		this.middleY = this.getMiddle(min2, max2);
		this.middleZ = this.getMiddle(min3, max3);
		Properties properties = new Properties();
		properties.load(new FileInputStream("src/main/resources/DBApp.config"));
		this.max = Integer.parseInt(properties.getProperty("MaximumEntriesinOctreeNode"));
		points = new Vector<Point>();
	}

	public Vector<Node> getChildren() {
		return children;
	}

	public void insert(Object x, Object y, Object z, String pagePath) throws FileNotFoundException, IOException { // handle
																													// duplicates
		if (this.children == null) {
			for (Point p : this.points) {
				if (p.x.equals(x) && p.y.equals(y) && p.z.equals(z)) {
					p.locations.add(pagePath);
				}
			}
			Point point = new Point(x, y, z, pagePath);
			if (this.points.size() < this.max) {
				points.add(point);
			} else {
				children = new Vector<Node>();
				children.setSize(8);
				this.children.set(0, new Node(minX, middleX, minY, middleY, minZ, middleZ));
				this.children.set(1, new Node(minX, middleX, minY, middleY, middleZ, maxZ));
				this.children.set(2, new Node(minX, middleX, middleY, maxY, minZ, middleZ));
				this.children.set(3, new Node(minX, middleX, middleY, maxY, middleZ, maxZ));
				this.children.set(4, new Node(middleX, maxX, minY, middleY, minZ, middleZ));
				this.children.set(5, new Node(middleX, maxX, minY, middleY, middleZ, maxZ));
				this.children.set(6, new Node(middleX, maxX, middleY, maxY, minZ, middleZ));
				this.children.set(7, new Node(middleX, maxX, middleY, maxY, middleZ, maxZ));
				for (int i = 0; i < children.size(); i++) {
					if (compareToGeneral(x, children.get(i).minX) >= 0 && compareToGeneral(x, children.get(i).maxX) <= 0
							&& compareToGeneral(y, children.get(i).minY) >= 0
							&& compareToGeneral(y, children.get(i).maxY) <= 0
							&& compareToGeneral(z, children.get(i).minZ) >= 0
							&& compareToGeneral(z, children.get(i).maxZ) <= 0) {
						children.get(i).points.add(point);
					}
				}
			}
		} else {
			for (int i = 0; i < children.size(); i++) {
				if (compareToGeneral(x, children.get(i).minX) >= 0 && compareToGeneral(x, children.get(i).maxX) <= 0
						&& compareToGeneral(y, children.get(i).minY) >= 0
						&& compareToGeneral(y, children.get(i).maxY) <= 0
						&& compareToGeneral(z, children.get(i).minZ) >= 0
						&& compareToGeneral(z, children.get(i).maxZ) <= 0) {
					children.get(i).insert(x, y, z, pagePath);
				}
			}

		}

	}

	public Vector<String> search(Object x, Object y, Object z, Vector<String> result) {

		if (this.children.equals(null)) {
			Point p = null;
			for (int i = 0; i < this.points.size(); i++) {
				p = this.points.get(i);
				if ((x == null || p.x.equals(x)) && (y == null || p.y.equals(y)) && (z == null || p.z.equals(z))) {
					for (int j = 0; j < p.locations.size(); j++) {
						if (!(result.contains(p.locations.get(j)))) {
							result.add(p.locations.get(j));
						}
					}
				}
			}
		} else {
			Vector<Integer> nodes = children(x, y, z);
			for (int i = 0; i < nodes.size(); i++) {
				Node n = children.get(nodes.get(i));
				result = n.search(x, y, z, result);

			}
		}

		return result;
	}

	public Vector<Integer> children(Object x, Object y, Object z) {
		Vector<Integer> nodesX = new Vector<Integer>();
		Vector<Integer> nodesY = new Vector<Integer>();
		Vector<Integer> nodesZ = new Vector<Integer>();
		if (compareToGeneral(x, this.middleX) >= 0) {
			nodesX.add(4);
			nodesX.add(5);
			nodesX.add(6);
			nodesX.add(7);
		} else if (x == null) {
			nodesX.add(0);
			nodesX.add(1);
			nodesX.add(2);
			nodesX.add(3);
			nodesX.add(4);
			nodesX.add(5);
			nodesX.add(6);
			nodesX.add(7);
		} else {
			nodesX.add(0);
			nodesX.add(1);
			nodesX.add(2);
			nodesX.add(3);
		}

		if (compareToGeneral(y, this.middleY) >= 0) {
			nodesY.add(2);
			nodesY.add(3);
			nodesY.add(6);
			nodesY.add(7);
		} else if (y == null) {
			nodesY.add(0);
			nodesY.add(1);
			nodesY.add(2);
			nodesY.add(3);
			nodesY.add(4);
			nodesY.add(5);
			nodesY.add(6);
			nodesY.add(7);
		} else {
			nodesY.add(0);
			nodesY.add(1);
			nodesY.add(4);
			nodesY.add(5);
		}
		if (compareToGeneral(z, this.middleZ) >= 0) {
			nodesZ.add(1);
			nodesZ.add(3);
			nodesZ.add(5);
			nodesZ.add(7);
		} else if (z == null) {
			nodesZ.add(0);
			nodesZ.add(1);
			nodesZ.add(2);
			nodesZ.add(3);
			nodesZ.add(4);
			nodesZ.add(5);
			nodesZ.add(6);
			nodesZ.add(7);
		} else {
			nodesZ.add(0);
			nodesZ.add(2);
			nodesZ.add(4);
			nodesZ.add(6);
		}
		nodesY.retainAll(nodesZ);
		nodesX.retainAll(nodesY);
		return nodesX;

	}

	public Object getMiddle(Object o1, Object o2) {
		if (o1 instanceof Integer && o2 instanceof Integer)
			return ((int) o1 + (int) o2) / 2;
		if (o1 instanceof String && o2 instanceof String) {
			String s1 = (String) o1;
			String s2 = (String) o2;
			int min = s1.length() > s2.length() ? s2.length() : s1.length();
			int max = s1.length() > s2.length() ? s1.length() : s2.length();
			boolean b = s1.length() > s2.length();
			for (int i = min; i < max; i++) {
				if (b) {
					s2 += s1.charAt(i);
				} else {
					s1 += s2.charAt(i);
				}
			}
			int N = s1.length();
			String result = "";
			int[] a1 = new int[N + 1];
			for (int i = 0; i < N; i++) {
				a1[i + 1] = (int) s1.charAt(i) - 97 + (int) s2.charAt(i) - 97;
			}
			for (int i = N; i >= 1; i--) {
				a1[i - 1] += (int) a1[i] / 26;
				a1[i] %= 26;
			}
			for (int i = 0; i <= N; i++) {
				if ((a1[i] & 1) != 0) {

					if (i + 1 <= N) {
						a1[i + 1] += 26;
					}
				}

				a1[i] = (int) a1[i] / 2;
			}

			for (int i = 1; i <= N; i++) {
				result += (char) (a1[i] + 97);
			}
			return result;
		}
		if (o1 instanceof Double && o2 instanceof Double)
			return ((Double) o1 + (Double) o2) / 2;
		if (o1 instanceof Date && o2 instanceof Date) {
			return Period.between((LocalDate) o1, (LocalDate) o2);
		}
		return null;
	}

	public void delete(Object x, Object y, Object z, String pagePath) {
		if (this.children == null) {
			for (Point p : this.points) {
				if (p.x.equals(x) && p.y.equals(y) && p.z.equals(z)) {
					p.locations.remove(pagePath);
					if (p.locations.size() == 0) {
						points.remove(p);
					}
				}
			}
		} else {
			for (int i = 0; i < children.size(); i++) {
				if (compareToGeneral(x, children.get(i).minX) >= 0 && compareToGeneral(x, children.get(i).maxX) <= 0
						&& compareToGeneral(y, children.get(i).minY) >= 0
						&& compareToGeneral(y, children.get(i).maxY) <= 0
						&& compareToGeneral(z, children.get(i).minZ) >= 0
						&& compareToGeneral(z, children.get(i).maxZ) <= 0) {
					children.get(i).delete(x, y, z, pagePath);
				}
			}

		}

	}

	public Vector<String> getRange(Object startX, Object endX, Object startY, Object endY, Object startZ, Object endZ,
			Vector<String> result) {
		if (this.children == null) {
			for (Point p : this.points) {
				if (this.compareToGeneral(p.x, startX) >= 0 && this.compareToGeneral(p.x, endX) <= 0
						&& this.compareToGeneral(p.y, startY) >= 0 && this.compareToGeneral(p.y, endY) <= 0
						&& this.compareToGeneral(p.z, startZ) >= 0 && this.compareToGeneral(p.z, endZ) <= 0) {

					for (int j = 0; j < p.locations.size(); j++) {
						if (!(result.contains(p.locations.get(j)))) {
							result.add(p.locations.get(j));
						}
					}
				}

			}
		} else {
			Vector<Integer> x = new Vector<Integer>();
			Vector<Integer> y = new Vector<Integer>();
			Vector<Integer> z = new Vector<Integer>();

			if (compareToGeneral(startX, middleX) <= 0 && compareToGeneral(endX, middleX) >= 0) {
				x.add(0);
				x.add(1);
				x.add(2);
				x.add(3);
				x.add(4);
				x.add(5);
				x.add(6);
				x.add(7);
			} else {
				if (compareToGeneral(startX, middleX) >= 0) {
					x.add(4);
					x.add(5);
					x.add(6);
					x.add(7);
				} else {
					if (compareToGeneral(endX, middleX) < 0) {
						x.add(0);
						x.add(1);
						x.add(2);
						x.add(3);
					}
				}
			}

			if (compareToGeneral(startY, middleY) <= 0 && compareToGeneral(endY, middleY) >= 0) {
				y.add(0);
				y.add(1);
				y.add(2);
				y.add(3);
				y.add(4);
				y.add(5);
				y.add(6);
				y.add(7);
			} else {
				if (compareToGeneral(startY, middleY) >= 0) {
					y.add(2);
					y.add(3);
					y.add(6);
					y.add(7);

				} else {
					if (compareToGeneral(endY, middleY) < 0) {
						y.add(0);
						y.add(1);
						y.add(4);
						y.add(5);
					}

				}
			}

			if (compareToGeneral(startZ, middleZ) <= 0 && compareToGeneral(endZ, middleZ) >= 0) {
				z.add(0);
				z.add(1);
				z.add(2);
				z.add(3);
				z.add(4);
				z.add(5);
				z.add(6);
				z.add(7);
			} else {
				if (compareToGeneral(startZ, middleZ) >= 0) {
					z.add(1);
					z.add(3);
					z.add(5);
					z.add(7);

				} else {
					if (compareToGeneral(endZ, middleZ) < 0) {
						z.add(0);
						z.add(2);
						z.add(4);
						z.add(6);
					}
				}
			}

			y.retainAll(z);
			x.retainAll(y);
			for (int i = 0; i < x.size(); i++) {
				Node n = this.children.get((int) x.get(i));
				result = n.getRange(startX, endX, startY, endY, startZ, endZ, result);

			}
		}

		return result;
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
