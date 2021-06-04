import java.io.Serializable;
import java.util.Hashtable;

public class BucketData implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	Hashtable<String, Object> data;
    String pageName;
    
    Comparable clusteringKey;
    
	public BucketData(Hashtable<String, Object> data, String pageName, Comparable clusteringKey) {
		this.data = data;
		this.pageName = pageName;
		this.clusteringKey = clusteringKey;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return data.toString();
	}
	
	


}
