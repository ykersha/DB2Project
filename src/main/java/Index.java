import java.io.Serializable;
import java.util.Vector;

public class Index implements Serializable {

	private static final long serialVersionUID = 1L;

	String tableName;
	int depth;
	Object[] gridIndex; // index[][][] === index[age][id][name]
	Vector<String> colNames = new Vector<String>(); // <age,id,name>
	Vector<Vector<Comparable>> minRange = new Vector<Vector<Comparable>>(); // < <0,10,15,20......>, <100, 200, 300>,
																			// <Ahmed, mohamed> >
	Vector<Vector<Comparable>> maxRange = new Vector<Vector<Comparable>>();;// < <9,14,19,29>, <199, 299, 399>, <david,
																			// ziad> >
	int numberOfBuckets = 0;
	// max range is exclusive ie max = 9; then 9 is not included

	public Index(String tableName, String[] columnNames) {
		this.tableName = tableName;
		this.depth = columnNames.length;
		for (int i = 0; i < columnNames.length; i++)
			colNames.add(i, columnNames[i]);

		gridIndex = createGrid(depth);
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return colNames.toString();
	}
	// insert sth grid[0][1][1] value= "bucketName"
	// SetIndex( vector<positions>, bucketName, currpos )

	public void set(int[] positions, Object element) {
		Object[] grid = this.gridIndex;
		int i;
		for (i = 0; i < positions.length - 1; i++) {
			int curPos = positions[i];
			grid = (Object[]) grid[curPos];
		}
		grid[positions[i]] = element;
	}

	public void set(Vector<Integer> positions, Object element) {
		Object[] grid = this.gridIndex;

		int i;
		for (i = 0; i < positions.size() - 1; i++) {
			int curPos = positions.get(i);
			grid = (Object[]) grid[curPos];
		}
		grid[positions.get(i)] = element;

//		Vector<Integer> tmp = new Vector<Integer>();
//		for(Integer i : positions)
//			tmp.add(i);
//		
//		
//		while(tmp.size() > 1) {
//			int curPos = tmp.remove(0);
//			grid = (Object[]) grid[curPos];
//		}
//		int pos = tmp.remove(0);
//		grid[pos] = element;
	}

	public Object get(int[] positions) {
		Object[] grid = this.gridIndex;
		int i;
		for (i = 0; i < positions.length - 1; i++) {
			int curPos = positions[i];
			grid = (Object[]) grid[curPos];
		}
		return grid[positions[i]];
	}

	public Object get(Vector<Integer> positions) {
		Object[] grid = this.gridIndex;

		int i;
		for (i = 0; i < positions.size() - 1; i++) {
			int curPos = positions.get(i);
			grid = (Object[]) grid[curPos];
		}

		return grid[positions.get(i)];
//		old get with tmp
//		Vector<Integer> tmp = new Vector<Integer>();
//		for(Integer i : positions)
//			tmp.add(i);
//		
//		
//		while(tmp.size() > 1) {
//			int curPos = tmp.remove(0);
//			grid = (Object[]) grid[curPos];
//		}
//		return grid[tmp.remove(0)];
	}

	public Vector<Object> getRange(Vector<Integer> positions) {
		Vector<Object> res = new Vector<>();
		return getRange(positions, res);
	}

	// to search entire dimension set its position to -1
	public Vector<Object> getRange(Vector<Integer> positions, Vector<Object> res) {

		Object[] grid = this.gridIndex;

		Vector<Integer> tmp = new Vector<Integer>();
		for (Integer i : positions)
			tmp.add(i);

		for (int i = 0; i < tmp.size(); i++) {
			if (tmp.get(i) == -1) {
				for (int j = 0; j < 10; j++) {
					tmp.set(i, j);
					res = getRange(tmp, res);
				}
			}
		}

		int i;
		for (i = 0; i < tmp.size() - 1; i++) {
			int curPos = tmp.get(i);
			grid = (Object[]) grid[curPos];
		}

		Object o = grid[tmp.get(i)];

		if (o != null && !res.contains(o))
			res.add(o);

		return res;
	}

	public static Object[] createGrid(int d) {

		Object[] a = new Object[10];
		return createGrid(d, a);
	}

	public static Object[] createGrid(int d, Object[] res) {

		if (d <= 1) {
			return res;
		}

		for (int i = 0; i < res.length; i++) {
			Object[] a = new Object[10];
			res[i] = createGrid(d - 1, a);
		}

		return res;
	}

}
