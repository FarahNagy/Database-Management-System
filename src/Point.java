import java.io.Serializable;
import java.util.Vector;

public class Point implements Serializable{
Object x;
Object y;
Object z;
Vector<String> locations;//check // in case of duplicates
public Point(Object x,Object y,Object z,String pagePath) {
	this.x=x;
	this.y=y;
	this.z=z;
	locations= new Vector<String>();
	this.locations.add(pagePath);
}
}
