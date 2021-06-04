import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class PageMetadata implements Serializable {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public Hashtable<Integer, Integer> pageNumber;
	public Vector<Comparable> maxInPages;
	
	
	public PageMetadata(Hashtable<Integer, Integer> pageNumber, Vector<Comparable> maxPerPages) {
		this.pageNumber = pageNumber;
		this.maxInPages = maxPerPages;
	}
	
	
	public Hashtable<Integer, Integer> getPageNumber() {
		return pageNumber;
	}
	public void setPageNumber(Hashtable<Integer, Integer> pageNumber) {
		this.pageNumber = pageNumber;
	}
	public Vector<Comparable> getMaxInPages() {
		return maxInPages;
	}
	public void setMaxInPages(Vector<Comparable> maxPerPages) {
		this.maxInPages = maxPerPages;
	}
	
	
	
	

}
