import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

public class DBApp implements DBAppInterface {

	int maxPerPage;
	int maxPerBucket;
	String dir = System.getProperty("user.dir") + "/src/main/resources/data/";

	public DBApp() {
		this.init();
	}

	@Override
	public void init() {
		Properties prop = new Properties();
		try {

			String filePath = System.getProperty("user.dir");
			InputStream is = new FileInputStream(filePath.concat(("/src/main/resources/DBApp.config")));

			// load the properties file
			prop.load(is);
			maxPerPage = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
			maxPerBucket = Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));

			Files.createDirectories(Paths.get(filePath + "/src/main/resources/data/"));

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {

		try {
			String row = "";
			String filePath = System.getProperty("user.dir");
			BufferedReader csvReader = new BufferedReader(
					new FileReader(filePath.concat(("/src/main/resources/metadata.csv"))));
			boolean tableFound = false;
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(tableName)) {
					tableFound = true;
					break;
				}
			}
			if (tableFound)
				throw new DBAppException("Table " + tableName + " already exists");
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (String name : colNameType.keySet()) {
			String row = "";
			row += tableName + ",";
			row += name + ",";
			row += colNameType.get(name) + ",";
			row += (clusteringKey.equals(name) ? "True" : "False") + ",";
			row += "False" + ",";
			row += colNameMin.get(name) + ",";
			row += colNameMax.get(name);

			try {
				writeToFile(row);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	@SuppressWarnings("rawtypes")
	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		// first part loop over the csv then check if all colNames already indexed, if
		// yes throw exception(index aleardy exists) else
		// set indexed to true

		String clusteringKey = "";

		try {
			String row = "";
			String filePath = System.getProperty("user.dir");

			BufferedReader csvReader = new BufferedReader(
					new FileReader(filePath.concat(("/src/main/resources/metadata.csv"))));
			boolean tableFound = false;
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(tableName)) {
					tableFound = true;
					if (data[3].toLowerCase().equals("true")) {
						clusteringKey = data[1];
					}
				}
			}

			if (!tableFound)
				throw new DBAppException("Cannot create index for table " + tableName + " as it does not exist");
		} catch (IOException e) {
			e.printStackTrace();
		}

		boolean alreadyIndexed = true;

		for (int i = 0; i < columnNames.length; i++) {
			boolean validColNames = false;
			try {
				String row = "";
				String filePath = System.getProperty("user.dir");
				BufferedReader csvReader = new BufferedReader(
						new FileReader(filePath.concat(("/src/main/resources/metadata.csv"))));
				while ((row = csvReader.readLine()) != null) {
					String[] data = row.split(",");
					if (data[0].equals(tableName) && data[1].equals(columnNames[i])) {
						validColNames = true;
						if (data[4].equals("False")) {
							alreadyIndexed = false;
							// code to replace indexed False with True
							updateCSV(row);
						}
						break;

					}
				}
				if (!validColNames) {
					throw new DBAppException("One or more column names passed do not exist");
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
//		if (alreadyIndexed) {
//			throw new DBAppException("An index is already created on those columns");
//		}

		Index grid = new Index(tableName, columnNames);

		// Hazem will get the mins and max and data types from csv and place them in the
		// array :impelement getdataFromCsv()
		Vector<Vector<Object>> dataFromCsv = getDataFromCsv(tableName, columnNames);

		Vector<Object> dataTypes = dataFromCsv.get(0); // object here is string
		Vector<Object> min = dataFromCsv.get(1); // object here is comparable
		Vector<Object> max = dataFromCsv.get(2); // object here is comparable

		try {
			setIndexRanges(grid, columnNames, dataTypes, min, max);

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		populateIndex(grid, tableName, clusteringKey);

		int count = 1;
		File f = new File(this.dir + tableName + "Index" + ".class");
		if (f.exists()) {
			// append the new index to the list of indices
			try {
				Vector<Index> indices = (Vector<Index>) deserialize(tableName + "Index");
				indices.add(grid);
				serialize(indices, tableName + "Index");
			} catch (ClassNotFoundException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else {
			// create vector of indices & serialize
			Vector<Index> indices = new Vector<Index>();
			indices.add(grid);
			try {
				serialize(indices, tableName + "Index");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	@SuppressWarnings({ "resource", "unchecked", "rawtypes", "deprecation" })
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

		String clusteringKey = "";
		PageMetadata meta = null;

		boolean tableIndexed = false;
		String finalPageName = "";

		try {
			String row = "";
			String filePath = System.getProperty("user.dir");
			BufferedReader csvReader = new BufferedReader(
					new FileReader(filePath.concat(("/src/main/resources/metadata.csv"))));
			boolean tableNotFound = true;
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				data[5] = data[5].replaceAll("^[\"']+|[\"']+$", "");
				data[6] = data[6].replaceAll("^[\"']+|[\"']+$", "");

				String colName = data[1];

				if (data[0].equals(tableName)) {
					tableNotFound = false;
					if (Boolean.parseBoolean(data[3]) == true) {
						clusteringKey = data[1];
						if (colNameValue.get(colName) == null)
							throw new DBAppException("Clustering Key Cannot be null");
					}
					if (colNameValue.get(colName) != null && !colNameValue.get(colName).getClass().getName()
							.toLowerCase().equals(data[2].toLowerCase())) {
						throw new DBAppException(colName + " must be of type " + data[2]);
					}

					if (data[4].toLowerCase().equals("true")) {
						tableIndexed = true;
					}

					switch (data[2].toLowerCase()) {
					case "java.lang.integer":
						if (colNameValue.get(colName) != null
								&& (Integer) colNameValue.get(colName) > Integer.parseInt(data[6])) {
							throw new DBAppException(colNameValue.get(colName) + " is greater than the max value of "
									+ colName + " " + data[6]);
						}
						if (colNameValue.get(colName) != null
								&& (Integer) colNameValue.get(colName) < Integer.parseInt(data[5])) {
							throw new DBAppException(colNameValue.get(colName) + " is smaller than the min value of "
									+ colName + " " + data[5]);
						}
						break;

					case "java.lang.string":
						if (colNameValue.get(colName) != null
								&& ((String) colNameValue.get(colName)).compareToIgnoreCase(data[6].toString()) > 0) {
							throw new DBAppException(colNameValue.get(colName) + " is greater than the max value of "
									+ colName + " " + data[6]);
						}

						if (colNameValue.get(colName) != null
								&& ((String) colNameValue.get(colName)).compareToIgnoreCase(data[5].toString()) < 0) {
							throw new DBAppException(colNameValue.get(colName) + " is smaller than the min value of "
									+ colName + " " + data[5]);
						}
						break;

					case "java.lang.double":
						if (colNameValue.get(colName) != null
								&& (Double) colNameValue.get(colName) > Double.parseDouble(data[6])) {
							throw new DBAppException(colNameValue.get(colName) + " is greater than the max value of "
									+ colName + " " + data[6]);
						}
						if (colNameValue.get(colName) != null
								&& (Double) colNameValue.get(colName) < Double.parseDouble(data[5])) {
							throw new DBAppException(colNameValue.get(colName) + " is smaller than the min value of "
									+ colName + " " + data[5]);
						}
						break;

					case "java.util.date":

						String[] maxDate = data[6].split("-");
						String[] minDate = data[5].split("-");

						if (colNameValue.get(colName) != null && ((Date) colNameValue.get(colName))
								.after((new Date(Integer.parseInt(maxDate[0]) - 1900, Integer.parseInt(maxDate[1]) - 1,
										Integer.parseInt(maxDate[2]))))) {
							throw new DBAppException(colNameValue.get(colName) + " is greater than the max value of "
									+ colName + " " + data[6]);
						}

						if (colNameValue.get(colName) != null && ((Date) colNameValue.get(colName))
								.before(new Date(Integer.parseInt(minDate[0]) - 1900, Integer.parseInt(minDate[1]) - 1,
										Integer.parseInt(minDate[2])))) {
							throw new DBAppException(colNameValue.get(colName) + " is smaller than the min value of "
									+ colName + " " + data[5]);
						}
						break;
					}

				}
			}
			if (tableNotFound) {
				throw new DBAppException("Table " + tableName + " not found");
			}
			csvReader.close();
		} catch (FileNotFoundException e1) {
			throw new DBAppException("Database not found. Please create a table first.");
		} catch (IOException e2) {
			e2.printStackTrace();
		}

		try {
			meta = (PageMetadata) deserialize(tableName + "Meta");
		} catch (ClassNotFoundException | IOException e2) {
			Hashtable<Integer, Integer> pageNumber = new Hashtable<Integer, Integer>();
			pageNumber.put(0, 0);
			Vector<Comparable> maxInPages = new Vector<Comparable>();
			Vector<Hashtable<String, Object>> newPage = new Vector<Hashtable<String, Object>>();
			newPage.add((Hashtable) colNameValue.clone());
			try {
				maxInPages.add(0, (Comparable) colNameValue.get(clusteringKey));
				meta = new PageMetadata(pageNumber, maxInPages);
				serialize(meta, tableName + "Meta");
				serialize(newPage, tableName + "Page" + 0);
				finalPageName = tableName + "Page" + 0;
				return;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		// int pageNumber = initialPage;

		Hashtable<Integer, Integer> htblpageNum = meta.getPageNumber();

		int pageNumber = binarySearchPage(meta.getMaxInPages(), (Comparable) colNameValue.get(clusteringKey), 0,
				meta.getMaxInPages().size() - 1);

		System.out.println("BinarySearchPage = " + pageNumber);
		// while (true) {

		try {
			int pageNumberDisk = htblpageNum.get(pageNumber);
			Vector<Hashtable<String, Object>> curPage = (Vector<Hashtable<String, Object>>) deserialize(
					tableName + "Page" + pageNumberDisk);
			Hashtable<String, Object> lastTuple = curPage.lastElement();

			if (((Comparable) lastTuple.get(clusteringKey))
					.compareTo((Comparable) colNameValue.get(clusteringKey)) >= 0) {
				int insertPosition = binarySearch(curPage, (Comparable) colNameValue.get(clusteringKey), 0,
						curPage.size() - 1, clusteringKey);

				System.out.println("-------" + insertPosition);

				if (insertPosition == -1)
					throw new DBAppException("A row with clusteringKey value " + colNameValue.get(clusteringKey)
							+ " already exists in the table");

				curPage.add(insertPosition, (Hashtable) colNameValue.clone());

				// need to add a check if this pageNumber exists or not
//					meta.getMaxInPages().set(pageNumber,
//							(Comparable) curPage.get(curPage.size() - 1).get(clusteringKey));

				System.out.println(
						"inserted in " + tableName + "Page" + pageNumberDisk + " at postion " + insertPosition);

//after inserting the page has overflowed
				if (curPage.size() > this.maxPerPage) {

					lastTuple = curPage.lastElement();
					curPage.remove(curPage.size() - 1);

					if (pageNumber >= meta.getMaxInPages().size()) {
						meta.getMaxInPages().add((Comparable) curPage.get((curPage.size() - 1)).get(clusteringKey));
					} else {
						meta.getMaxInPages().set(pageNumber,
								(Comparable) curPage.get(curPage.size() - 1).get(clusteringKey));
					}

//if the next page exists and is full we insert a new page between "pageNumber" and "pageNumber + 1"

					if (htblpageNum.containsKey(pageNumber + 1)) {
						Vector<Hashtable<String, Object>> nextPage = (Vector<Hashtable<String, Object>>) deserialize(
								tableName + "Page" + htblpageNum.get((pageNumber + 1)));
						if (nextPage.size() == this.maxPerPage) {
							int newPageDiskNum = 0;
							int maxCodePageNum = 0;
							for (int i : htblpageNum.keySet()) {
								maxCodePageNum = (i > maxCodePageNum) ? i : maxCodePageNum;
								newPageDiskNum = (htblpageNum.get(i) > newPageDiskNum) ? htblpageNum.get(i)
										: newPageDiskNum;
							}
							newPageDiskNum++;
//shifts all the hashtable pointers to make room for the new page
							for (int i = maxCodePageNum + 1; i > pageNumber + 1; i--) {
								System.out.println("keys now " + i + ":" + htblpageNum.get(i - 1));
								htblpageNum.put(i, htblpageNum.get(i - 1));

								if (i >= meta.getMaxInPages().size()) {
									meta.getMaxInPages().add(meta.getMaxInPages().get(i - 1));
								} else {
									meta.getMaxInPages().set(i, meta.getMaxInPages().get(i - 1));
								}
							}

							htblpageNum.put(pageNumber + 1, newPageDiskNum);
							System.out.println("keys now " + (pageNumber + 1) + ":" + newPageDiskNum);

							serialize(curPage, tableName + "Page" + pageNumberDisk);
							finalPageName = tableName + "Page" + pageNumberDisk;
							Vector<Hashtable<String, Object>> newPage = new Vector<Hashtable<String, Object>>();

//--------------------------do not forget to handle this case for the index, delete old, insert new
							newPage.add(0, lastTuple);

							if ((pageNumber + 1) >= meta.getMaxInPages().size()) {
								meta.getMaxInPages()
										.add((Comparable) newPage.get(newPage.size() - 1).get(clusteringKey));
							} else {
								meta.getMaxInPages().set((pageNumber + 1),
										(Comparable) newPage.get(newPage.size() - 1).get(clusteringKey));
							}

							serialize(newPage, tableName + "Page" + newPageDiskNum);

							if (tableIndexed) {
								deleteRowFromIndex(tableName, lastTuple, (Comparable) lastTuple.get(clusteringKey));
								insertRowIntoIndex(tableName, lastTuple, tableName + "Page" + newPageDiskNum,
										(Comparable) colNameValue.get(clusteringKey));
//								System.out.println("Insert" + (Comparable) colNameValue.get(clusteringKey));
							}

							System.out.println("*Page " + (newPageDiskNum) + " has been created & row inserted at 0");
							meta.setPageNumber(htblpageNum);
							serialize(meta, tableName + "Meta");
						} else {
							serialize(curPage, tableName + "Page" + pageNumberDisk);
							finalPageName = tableName + "Page" + pageNumberDisk;
							serialize(meta, tableName + "Meta");
							if (tableIndexed) {
								deleteRowFromIndex(tableName, lastTuple, (Comparable) lastTuple.get(clusteringKey));
							}
							insertIntoTable(tableName, lastTuple);
						}

					} else {
						int newPageDiskNum = 0;
						int maxCodePageNum = 0;
						for (int i : htblpageNum.keySet()) {
							maxCodePageNum = (i > maxCodePageNum) ? i : maxCodePageNum;
							newPageDiskNum = (htblpageNum.get(i) > newPageDiskNum) ? htblpageNum.get(i)
									: newPageDiskNum;
						}
						newPageDiskNum++;
						serialize(curPage, tableName + "Page" + htblpageNum.get(pageNumber));
						finalPageName = tableName + "Page" + htblpageNum.get(pageNumber);

						if (pageNumber + 1 >= meta.getMaxInPages().size()) {
							meta.getMaxInPages().add((Comparable) curPage.get(curPage.size() - 1).get(clusteringKey));
						} else {
							meta.getMaxInPages().set(pageNumber + 1,
									(Comparable) curPage.get(curPage.size() - 1).get(clusteringKey));
						}

						htblpageNum.put(pageNumber + 1, newPageDiskNum);

						Vector<Hashtable<String, Object>> newPage = new Vector<Hashtable<String, Object>>();
						newPage.add(0, lastTuple);
						serialize(newPage, tableName + "Page" + newPageDiskNum);
//// remove here the old row from index and insert the new one

						System.out.println("*Page " + (newPageDiskNum) + " has been created & row inserted at 0");
						meta.setPageNumber(htblpageNum);
						// add part for maxInPage for the new page

						if ((pageNumber + 1) >= meta.getMaxInPages().size()) {
							meta.getMaxInPages().add((Comparable) newPage.get(newPage.size() - 1).get(clusteringKey));
						} else {
							meta.getMaxInPages().set(pageNumber + 1,
									(Comparable) newPage.get(newPage.size() - 1).get(clusteringKey));
						}

						serialize(meta, tableName + "Meta");

					}
				} else {
					serialize(curPage, tableName + "Page" + htblpageNum.get(pageNumber));
// insert to index here-----------------------------------------------------------------------*****************************************
					finalPageName = tableName + "Page" + htblpageNum.get(pageNumber);

				}

				// break;

			} else {
				try {
//handles the case where an element was deleted leaving gaps and the row could be inserted in this gap
					Vector<Hashtable<String, Object>> nextPage = (Vector) deserialize(
							tableName + "Page" + htblpageNum.get(pageNumber + 1));

					if (((Comparable) nextPage.get(0).get(clusteringKey))
							.compareTo((Comparable) colNameValue.get(clusteringKey)) > 0) {
						if (curPage.size() < this.maxPerPage) {
							curPage.add(curPage.size(), (Hashtable) colNameValue.clone());
							serialize(curPage, tableName + "Page" + htblpageNum.get(pageNumber));
							finalPageName = tableName + "Page" + htblpageNum.get(pageNumber);
							System.out.println("inserted in " + tableName + "Page" + htblpageNum.get(pageNumber)
									+ " at postion " + (curPage.size() - 1));

							if ((pageNumber) >= meta.getMaxInPages().size()) {
								meta.getMaxInPages()
										.add((Comparable) curPage.get(curPage.size() - 1).get(clusteringKey));
							} else {
								meta.getMaxInPages().set(pageNumber,
										(Comparable) curPage.get(curPage.size() - 1).get(clusteringKey));
							}
							serialize(meta, tableName + "Meta");

						}

					}

				} catch (ClassNotFoundException | IOException | NullPointerException e) {
					if (curPage.size() >= this.maxPerPage) {
						Vector<Hashtable<String, Object>> newPage = new Vector<Hashtable<String, Object>>();
						newPage.add(0, (Hashtable) colNameValue.clone());
						int maxCodePageNum = 0;
						int newPageDiskNum = 0;
						for (int i : htblpageNum.keySet()) {
							maxCodePageNum = (i > maxCodePageNum) ? i : maxCodePageNum;
							newPageDiskNum = (htblpageNum.get(i) > newPageDiskNum) ? htblpageNum.get(i)
									: newPageDiskNum;
						}
						newPageDiskNum++;
						htblpageNum.put(maxCodePageNum + 1, newPageDiskNum);
						meta.setPageNumber(htblpageNum);

						if ((maxCodePageNum + 1) >= meta.getMaxInPages().size()) {
							meta.getMaxInPages().add((Comparable) newPage.get(newPage.size() - 1).get(clusteringKey));
						} else {
							meta.getMaxInPages().set(maxCodePageNum + 1,
									(Comparable) newPage.get(newPage.size() - 1).get(clusteringKey));
						}

						serialize(meta, tableName + "Meta");
						serialize(newPage, tableName + "Page" + newPageDiskNum);
						finalPageName = tableName + "Page" + newPageDiskNum;
						System.out.println("*Page " + (newPageDiskNum) + " has been created & row inserted at 0");
					} else {
						// need to change maxInPages here
						curPage.add(curPage.size(), (Hashtable) colNameValue.clone());

						if ((pageNumber) >= meta.getMaxInPages().size()) {
							meta.getMaxInPages().add((Comparable) curPage.get(curPage.size() - 1).get(clusteringKey));
						} else {
							meta.getMaxInPages().set(pageNumber,
									(Comparable) curPage.get(curPage.size() - 1).get(clusteringKey));
						}

						serialize(meta, tableName + "Meta");
						serialize(curPage, tableName + "Page" + htblpageNum.get(pageNumber));
						finalPageName = tableName + "Page" + htblpageNum.get(pageNumber);
						System.out.println("inserted in " + tableName + "Page" + htblpageNum.get(pageNumber)
								+ " at postion " + (curPage.size() - 1));
					}
					// break;
				}
			}

		} catch (ClassNotFoundException | IOException e) {
			Vector<Hashtable<String, Object>> newPage = new Vector<Hashtable<String, Object>>();
			newPage.add((Hashtable) colNameValue.clone());
			try {
				int maxCodePageNum = 0;
				int newPageDiskNum = 0;
				for (int i : htblpageNum.keySet()) {
					maxCodePageNum = (i > maxCodePageNum) ? i : maxCodePageNum;
					newPageDiskNum = (htblpageNum.get(i) > newPageDiskNum) ? htblpageNum.get(i) : newPageDiskNum;
				}
				newPageDiskNum++;
				htblpageNum.put(maxCodePageNum + 1, newPageDiskNum);
				meta.setPageNumber(htblpageNum);

				if ((maxCodePageNum + 1) >= meta.getMaxInPages().size()) {
					meta.getMaxInPages().add((Comparable) newPage.get(newPage.size() - 1).get(clusteringKey));
				} else {
					meta.getMaxInPages().set(maxCodePageNum + 1,
							(Comparable) newPage.get(newPage.size() - 1).get(clusteringKey));
				}

				serialize(meta, tableName + "Meta");
				serialize(newPage, tableName + "Page" + newPageDiskNum);
				finalPageName = tableName + "Page" + newPageDiskNum;
				System.out.println(
						tableName + "Page" + newPageDiskNum + " page has been created & row inserted at index 0");
				// break;
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}

		// pageNumber++;
		// }

		if (tableIndexed) {
			insertRowIntoIndex(tableName, colNameValue, finalPageName, (Comparable) colNameValue.get(clusteringKey));
			System.out.println("inserted new tuple to index " + (Comparable) colNameValue.get(clusteringKey));
		}
	}

	@SuppressWarnings({ "unchecked", "deprecation" })
	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {

		boolean tableIndexed = false;

		PageMetadata meta = null;
		try {
			meta = (PageMetadata) deserialize(tableName + "Meta");
		} catch (ClassNotFoundException | IOException e2) {
			throw new DBAppException("Table " + tableName + " does not exist");
		}

		Hashtable<Integer, Integer> htblpageNum = meta.getPageNumber();

		String clusteringKeyName = "";
		String clusteringKeyType = "";
		Comparable comparableValue = clusteringKeyValue;
		Vector<String> colsToBeUpdated = new Vector<String>();

		try {
			String row = "";
			String filePath = System.getProperty("user.dir");
			BufferedReader csvReader = new BufferedReader(
					new FileReader(filePath.concat(("/src/main/resources/metadata.csv"))));
			boolean tableNotFound = true;
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(tableName)) {
					tableNotFound = false;
					if (Boolean.parseBoolean(data[3]) == true) {
						clusteringKeyName = data[1];
						clusteringKeyType = data[2];
					}
					String colName = data[1];
					colsToBeUpdated.add(colName);
					if (columnNameValue.get(colName) != null && !columnNameValue.get(colName).getClass().getName()
							.toLowerCase().equals(data[2].toLowerCase())) {
						throw new DBAppException(colName + " must be of type " + data[2]);
					}

					if (data[4].toLowerCase().equals("true")) {
						tableIndexed = true;
					}

					switch (data[2].toLowerCase()) {
					case "java.lang.integer":
						if (columnNameValue.get(colName) != null
								&& (Integer) columnNameValue.get(colName) > Integer.parseInt(data[6])) {
							throw new DBAppException(columnNameValue.get(colName) + " is greater than the max value of "
									+ colName + " " + data[6]);
						}
						if (columnNameValue.get(colName) != null
								&& (Integer) columnNameValue.get(colName) < Integer.parseInt(data[5])) {
							throw new DBAppException(columnNameValue.get(colName) + " is smaller than the min value of "
									+ colName + " " + data[5]);
						}
						break;

					case "java.lang.string":
						if (columnNameValue.get(colName) != null && ((String) columnNameValue.get(colName))
								.compareToIgnoreCase(data[6].toString()) > 0) {
							throw new DBAppException(columnNameValue.get(colName) + " is greater than the max value of "
									+ colName + " " + data[6]);
						}

						if (columnNameValue.get(colName) != null && ((String) columnNameValue.get(colName))
								.compareToIgnoreCase(data[5].toString()) < 0) {
							throw new DBAppException(columnNameValue.get(colName) + " is smaller than the min value of "
									+ colName + " " + data[5]);
						}
						break;

					case "java.lang.double":
						if (columnNameValue.get(colName) != null
								&& (Double) columnNameValue.get(colName) > Double.parseDouble(data[6])) {
							throw new DBAppException(columnNameValue.get(colName) + " is greater than the max value of "
									+ colName + " " + data[6]);
						}
						if (columnNameValue.get(colName) != null
								&& (Double) columnNameValue.get(colName) < Double.parseDouble(data[5])) {
							throw new DBAppException(columnNameValue.get(colName) + " is smaller than the min value of "
									+ colName + " " + data[5]);
						}
						break;

					case "java.util.date":

						String[] maxDate = data[6].split("-");
						String[] minDate = data[5].split("-");

						if (columnNameValue.get(colName) != null && ((Date) columnNameValue.get(colName))
								.after((new Date(Integer.parseInt(maxDate[0]) - 1900, Integer.parseInt(maxDate[1]) - 1,
										Integer.parseInt(maxDate[2]))))) {
							throw new DBAppException(columnNameValue.get(colName) + " is greater than the max value of "
									+ colName + " " + data[6]);
						}

						if (columnNameValue.get(colName) != null && ((Date) columnNameValue.get(colName))
								.before(new Date(Integer.parseInt(minDate[0]) - 1900, Integer.parseInt(minDate[1]) - 1,
										Integer.parseInt(minDate[2])))) {
							throw new DBAppException(columnNameValue.get(colName) + " is smaller than the min value of "
									+ colName + " " + data[5]);
						}
						break;
					}

				}
			}
			if (tableNotFound)
				throw new DBAppException("Table " + tableName + " does not exist");

			for (String name : columnNameValue.keySet()) {
				if (!colsToBeUpdated.contains(name)) {
					throw new DBAppException("Table " + tableName + " does not contain col " + name);
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		switch (clusteringKeyType.toLowerCase()) {
		case "java.lang.integer":
			comparableValue = Integer.parseInt(clusteringKeyValue);
			break;

		case "java.lang.double":
			comparableValue = Double.parseDouble(clusteringKeyValue);
			break;
		case "java.util.date":
			String[] date = clusteringKeyValue.split("-");
			comparableValue = new Date(Integer.parseInt(date[0]) - 1900, Integer.parseInt(date[1]) - 1,
					Integer.parseInt(date[2]));
			break;
		default:
			comparableValue = clusteringKeyValue;
		}
		// int pageNumber = 0;

		int pageNumber = binarySearchPage(meta.getMaxInPages(), comparableValue, 0, meta.getMaxInPages().size() - 1);

//		while (true) {
		Vector<Hashtable<String, Object>> curPage;
		try {
			curPage = (Vector<Hashtable<String, Object>>) deserialize(tableName + "Page" + htblpageNum.get(pageNumber));

			Hashtable<String, Object> lastTuple = curPage.lastElement();

			if (((Comparable) lastTuple.get(clusteringKeyName)).compareTo(comparableValue) >= 0) {
				int position = binarySearchUpdate(curPage, comparableValue, 0, curPage.size() - 1, clusteringKeyName);
				if (position == -1) {
					System.out.println("Row with value " + comparableValue.toString() + " does not exist");
//						throw new DBAppException("Row with value " + comparableValue.toString() + " does not exist");
//						return;
				}

				Hashtable<String, Object> selectedRow = curPage.get(position);
				if (tableIndexed) {
					deleteRowFromIndex(tableName, selectedRow, (Comparable) selectedRow.get(clusteringKeyName));
				}

				for (String name : columnNameValue.keySet()) {
					selectedRow.replace(name, columnNameValue.get(name));
				}

				curPage.set(position, selectedRow);

				if (tableIndexed) {
					insertRowIntoIndex(tableName, selectedRow, tableName + "Page" + htblpageNum.get(pageNumber),
							(Comparable) selectedRow.get(clusteringKeyName));
//					System.out.println("Update " + selectedRow.get(clusteringKeyName));
				}
				serialize(curPage, tableName + "Page" + htblpageNum.get(pageNumber));
//					break;

			}
		} catch (ClassNotFoundException | IOException e) {
			System.out.println("Row with value " + comparableValue.toString() + " does not exist");
//				throw new DBAppException("Row with value " + comparableValue.toString() + " does not exist");
		}
//			pageNumber++;
//		}
	}

	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {

		PageMetadata meta = null;
		try {
			meta = (PageMetadata) deserialize(tableName + "Meta");
		} catch (ClassNotFoundException | IOException e2) {
			throw new DBAppException("Table " + tableName + " does not exist");
		}

		Hashtable<Integer, Integer> htblpageNum = meta.getPageNumber();

		String clusteringKeyName = "";
		String clusteringKeyType = "";
		Vector<String> colNames = new Vector<String>();
		boolean tableIndexed = false;
		Vector<String> colsIndexed = new Vector<String>();

		try {
			String row = "";
			String filePath = System.getProperty("user.dir");
			BufferedReader csvReader = new BufferedReader(
					new FileReader(filePath.concat(("/src/main/resources/metadata.csv"))));
			boolean tableNotFound = true;
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(tableName)) {
					tableNotFound = false;
					if (Boolean.parseBoolean(data[3]) == true) {
						clusteringKeyName = data[1];
						clusteringKeyType = data[2];
					}
					colNames.add(data[1]);

					if (data[4].toLowerCase().equals("true")) {
						tableIndexed = true;
						if (columnNameValue.containsKey(data[1])) {
							colsIndexed.add(data[1]);
						}
					}
				}
			}
			if (tableNotFound)
				throw new DBAppException("Table " + tableName + " does not exist");
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (String name : columnNameValue.keySet()) {
			if (!colNames.contains(name)) {
				throw new DBAppException("The col " + name + " does not exist in table " + tableName);
			}
		}

		int pageNumber = 0;
		Comparable comparableValue;

		if (columnNameValue.containsKey(clusteringKeyName)) {

			comparableValue = (Comparable) columnNameValue.get(clusteringKeyName);
			pageNumber = binarySearchPage(meta.getMaxInPages(), (Comparable) columnNameValue.get(clusteringKeyName), 0,
					meta.getMaxInPages().size() - 1);

//			while (true) {
			Vector<Hashtable<String, Object>> curPage;
			try {
				curPage = (Vector<Hashtable<String, Object>>) deserialize(
						tableName + "Page" + htblpageNum.get(pageNumber));

				Hashtable lastTuple = curPage.lastElement();

				if (((Comparable) lastTuple.get(clusteringKeyName)).compareTo(comparableValue) >= 0) {
					int position = binarySearchUpdate(curPage, comparableValue, 0, curPage.size() - 1,
							clusteringKeyName);
					if (position == -1) {
						System.out.println("Row with value " + comparableValue.toString() + " does not exist");
//							throw new DBAppException("Row with value " + comparableValue.toString() + " does not exist");
					}

					boolean deleteRow = true;
					Hashtable<String, Object> row = curPage.get(position);
					for (String colName : columnNameValue.keySet()) {
						if (!(((Comparable) row.get(colName))
								.compareTo((Comparable) columnNameValue.get(colName)) == 0)) {
							deleteRow = false;
							break;
						}
					}

					if (deleteRow) {
						if (tableIndexed) {
							deleteRowFromIndex(tableName, curPage.get(position),
									(Comparable) curPage.get(position).get(clusteringKeyName));
						}
						curPage.remove(position);
					}

					if (curPage.size() == 0) {
						String filePath = System.getProperty("user.dir");
						File file = new File(filePath.concat(("/src/main/resources/data/" + tableName + "Page"
								+ htblpageNum.get(pageNumber) + ".class")));
						file.delete();

						int maxCodePageNum = 0;
						for (int i : htblpageNum.keySet()) {
							maxCodePageNum = (i > maxCodePageNum) ? i : maxCodePageNum;
						}
						for (int i = pageNumber; i < maxCodePageNum; i++) {
							htblpageNum.put(i, htblpageNum.get(i + 1));
							meta.getMaxInPages().set(i, meta.getMaxInPages().get(i + 1));

						}
						htblpageNum.remove(maxCodePageNum);
						meta.setPageNumber(htblpageNum);
						meta.getMaxInPages().remove(maxCodePageNum);
						serialize(meta, tableName + "Meta");

//							boolean renameSuccess = true;
//							int tmpPageNum = pageNumber;
//							while (renameSuccess) {
//								File oldName = new File(filePath.concat(("/src/main/resources/data/" + tableName
//										+ "Page" + (tmpPageNum + 1) + ".class")));
//								File newName = new File(filePath.concat(("/src/main/resources/data/" + tableName
//										+ "Page" + tmpPageNum + ".class")));
//								try {
//									renameSuccess = oldName.renameTo(newName);
//									if (renameSuccess)
//										System.out.println("File was renamed successfully");
//								} catch (NullPointerException e) {
//									break;
//								}
//								tmpPageNum++;
//							}

					} else {
						serialize(curPage, tableName + "Page" + htblpageNum.get(pageNumber));
						meta.getMaxInPages().set(pageNumber,
								(Comparable) curPage.get(curPage.size() - 1).get(clusteringKeyName));
						serialize(meta, tableName + "Meta");
					}

//						break;
				}
			} catch (ClassNotFoundException | IOException e) {
				System.out.println("Row with value " + comparableValue.toString() + " does not exist");
//					throw new DBAppException("Row with value " + comparableValue.toString() + " does not exist");
			}
//				pageNumber++;
//			}

//---------------------------------------------------------------------------------------------------------------------------
//modification
		} else if (tableIndexed && colsIndexed.size() > 0) {
			try {
				Vector<Index> indices = (Vector<Index>) deserialize(tableName + "Index");
				int max = 0;
				Index index = null;

				for (Index cur : indices) {
					int curCount = 0;
					Vector<String> curCols = cur.colNames;
					for (String s : columnNameValue.keySet()) {
						if (curCols.contains(s))
							curCount++;
					}
					if (curCount > max) {
						max = curCount;
						index = cur;
					}
				}

				Vector<Integer> pos = new Vector<Integer>();
				int colNum = 0;
				for (String col : index.colNames) {
					if (!columnNameValue.containsKey(col)) {
						pos.add(-1);
					} else {
						int p = indexBinarySearch(index.maxRange.get(colNum), (Comparable) columnNameValue.get(col), 0,
								9);
						pos.add(p);
					}
					colNum++;
				}

				Vector<Object> bucketNames = index.getRange(pos);
				Vector<String> pages = new Vector<String>();

				for (Object bucketObj : bucketNames) {
					String curBucket = (String) bucketObj;
					int count = 0;
					File f = new File(this.dir + curBucket + count + ".class");

					while (f.exists()) {
						try {
							Vector<BucketData> bucket = (Vector<BucketData>) deserialize(curBucket + count);
							for (int i = 0; i < bucket.size(); i++) {
								BucketData data = bucket.get(i);
								boolean addToPages = true;

								for (String s : data.data.keySet()) {
									if (columnNameValue.containsKey(s)) {
										Comparable compBucketValue = (Comparable) data.data.get(s);
										if (((Comparable) columnNameValue.get(s)).compareTo(compBucketValue) != 0)
											addToPages = false;
									}
								}
								if (addToPages && !pages.contains(data.pageName))
									pages.add(data.pageName);

							}
						} catch (ClassNotFoundException | IOException e) {
							e.printStackTrace();
						}

						count++;
						f = new File(this.dir + curBucket + count + ".class");
					}

				}
				for (String page : pages) {
					Vector<Hashtable<String, Object>> curPage;
					try {
						curPage = (Vector<Hashtable<String, Object>>) deserialize(page);

						Vector<Hashtable<String, Object>> rowsToBedeleted = new Vector<Hashtable<String, Object>>();

						for (Hashtable<String, Object> row : curPage) {
							boolean deleteRow = true;
							for (String colName : columnNameValue.keySet()) {
								if (!(((Comparable) row.get(colName))
										.compareTo((Comparable) columnNameValue.get(colName)) == 0)) {
									deleteRow = false;
									break;
								}
							}
							if (deleteRow) {
								rowsToBedeleted.add(row);
							}
						}
						for (Hashtable<String, Object> row : rowsToBedeleted) {

							if (tableIndexed) {
								deleteRowFromIndex(tableName, row, (Comparable) row.get(clusteringKeyName));
							}

							System.out.println("Index delete " + row + " from page " + page);

							curPage.remove(row);
							if (curPage.size() == 0) {
								String filePath = System.getProperty("user.dir");
								File file = new File(filePath.concat(("/src/main/resources/data/" + tableName + "Page"
										+ htblpageNum.get(pageNumber) + ".class")));
								file.delete();

								int maxCodePageNum = 0;
								for (int i : htblpageNum.keySet()) {
									maxCodePageNum = (i > maxCodePageNum) ? i : maxCodePageNum;
								}
								for (int i = pageNumber; i < maxCodePageNum; i++) {
									htblpageNum.put(i, htblpageNum.get(i + 1));
									meta.getMaxInPages().set(i, meta.getMaxInPages().get(i + 1));
								}
								htblpageNum.remove(maxCodePageNum);
								meta.setPageNumber(htblpageNum);
								meta.getMaxInPages().remove(maxCodePageNum);
								serialize(meta, tableName + "Meta");

							} else {
								meta.getMaxInPages().set(pageNumber,
										(Comparable) curPage.get(curPage.size() - 1).get(clusteringKeyName));
								serialize(meta, tableName + "Meta");
								serialize(curPage, tableName + "Page" + htblpageNum.get(pageNumber));
							}

						}

					} catch (ClassNotFoundException | IOException e) {
						break;
					}
				}

			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}

//---------------------------------------------------------------------------------------------------------------------------			
		} else {
			while (true) {
				Vector<Hashtable<String, Object>> curPage;
				try {
					curPage = (Vector<Hashtable<String, Object>>) deserialize(
							tableName + "Page" + htblpageNum.get(pageNumber));

					Vector<Hashtable<String, Object>> rowsToBedeleted = new Vector<Hashtable<String, Object>>();

					for (Hashtable<String, Object> row : curPage) {
						boolean deleteRow = true;
						for (String colName : columnNameValue.keySet()) {
							if (!(((Comparable) row.get(colName))
									.compareTo((Comparable) columnNameValue.get(colName)) == 0)) {
								deleteRow = false;
								break;
							}
						}
						if (deleteRow) {
							rowsToBedeleted.add(row);
						}
					}
					for (Hashtable<String, Object> row : rowsToBedeleted) {

						if (tableIndexed) {
							deleteRowFromIndex(tableName, row, (Comparable) row.get(clusteringKeyName));
						}

						curPage.remove(row);
						if (curPage.size() == 0) {
							String filePath = System.getProperty("user.dir");
							File file = new File(filePath.concat(("/src/main/resources/data/" + tableName + "Page"
									+ htblpageNum.get(pageNumber) + ".class")));
							file.delete();

							int maxCodePageNum = 0;
							for (int i : htblpageNum.keySet()) {
								maxCodePageNum = (i > maxCodePageNum) ? i : maxCodePageNum;
							}
							for (int i = pageNumber; i < maxCodePageNum; i++) {
								htblpageNum.put(i, htblpageNum.get(i + 1));
								meta.getMaxInPages().set(i, meta.getMaxInPages().get(i + 1));
							}
							htblpageNum.remove(maxCodePageNum);
							meta.setPageNumber(htblpageNum);
							meta.getMaxInPages().remove(maxCodePageNum);
							serialize(meta, tableName + "Meta");

//							boolean renameSuccess = true;
//							int tmpPageNum = pageNumber;
//							while (renameSuccess) {
//								File oldName = new File(filePath.concat(("/src/main/resources/data/" + tableName
//										+ "Page" + (tmpPageNum + 1) + ".class")));
//								File newName = new File(filePath.concat(("/src/main/resources/data/" + tableName
//										+ "Page" + tmpPageNum + ".class")));
//								try {
//									renameSuccess = oldName.renameTo(newName);
//									System.out.println("File was renamed successfully");
//								} catch (NullPointerException e) {
//									break;
//								}
//								tmpPageNum++;
//							}
						} else {
							meta.getMaxInPages().set(pageNumber,
									(Comparable) curPage.get(curPage.size() - 1).get(clusteringKeyName));
							serialize(meta, tableName + "Meta");
							serialize(curPage, tableName + "Page" + htblpageNum.get(pageNumber));
						}

					}

				} catch (ClassNotFoundException | IOException e) {
					break;
				}
				pageNumber++;
			}

		}

		if (meta.getPageNumber().isEmpty() && meta.getMaxInPages().isEmpty()) {
			String filePath = System.getProperty("user.dir");
			File file = new File(filePath.concat(("/src/main/resources/data/" + tableName + "Meta" + ".class")));
			file.delete();
		}

	}

	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {

		boolean indexed = false;
		Vector<Index> indices = null;
		Vector<Hashtable<String, Object>> prevResult = null;

		for (int i = 0; i < sqlTerms.length; i++) {
			SQLTerm op = sqlTerms[i];
			if (!checkValidSQLTerm(op)) {
				throw new DBAppException("invalid SQL Query");
			}
		}

		File indexFile = new File(this.dir + sqlTerms[0]._strTableName + "Index" + ".class");
		if (indexFile.exists()) {
			try {
				indices = (Vector<Index>) deserialize(sqlTerms[0]._strTableName + "Index");
				indexed = true;
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}
		}

		boolean first = true;

		Vector<Vector<Integer>> sqlIndices = new Vector<Vector<Integer>>();
		if (indexed) {
			for (int i = 0; i < sqlTerms.length; i++) {
				Vector<Integer> s = new Vector<Integer>();
				SQLTerm cur = sqlTerms[i];
				int j = 0;
				for (Index index : indices) {
					if (index.colNames.contains(cur._strColumnName))
						s.add(j);
					j++;
				}
				sqlIndices.add(s);
			}
		}

		if (sqlTerms.length == 1) {
			if (indexed) {
				return execOneTermIndex(sqlTerms[0], sqlIndices.get(0), indices);
			} else if (containsClustering(sqlTerms[0])) {
				return execTermBinary(sqlTerms[0]).iterator();
			} else {
				return execTermLinearly(sqlTerms[0]).iterator();
			}

		}

		for (int i = 0; i < arrayOperators.length; i++) {

			String operator = arrayOperators[i];
			SQLTerm op1 = sqlTerms[i];
			SQLTerm op2 = sqlTerms[i + 1];

			if (indexed && (sqlIndices.get(i + 1).size() > 0 || (first && sqlIndices.get(i).size() > 0))) {
				// sqlTerm[i + 1] is indexed
				if (operator.equals("OR") || operator.equals("XOR")) {
					if (first) {
						prevResult = execOperationIndex(op1, op2, operator, sqlIndices.get(i), sqlIndices.get(i + 1),
								indices);
						first = false;
					} else {
						// not first
						prevResult = execOperationIndex(prevResult, op2, operator, sqlIndices.get(i), indices);
					}

				} else {
					// AND
					int numOfAnds = 0;
					for (int j = i; j < arrayOperators.length; j++) {
						if (arrayOperators[j].equals("AND"))
							numOfAnds++;
					}

					Vector<SQLTerm> andedTerms = new Vector<SQLTerm>();
					Vector<Vector<Integer>> andedTermsInd = new Vector<Vector<Integer>>();
					int j;

					if (first)
						j = 0;
					else
						j = i + 1;

					while (j < i + numOfAnds + 1) {
						andedTerms.add(sqlTerms[j]);
						andedTermsInd.add(sqlIndices.get(j));
						j++;
					}

					i = i + numOfAnds - 1;

					Vector<Hashtable<String, Object>> r = execOperationAnd(andedTerms, andedTermsInd, indices);
					if (first) {
						prevResult = r;
						first = false;
					} else {
						prevResult = intersect(r, prevResult);
					}
				}
			} else {
				// sqlTerm not indexed
				if (first) {
					prevResult = execOperation(op1, op2, operator);
					first = false;
				} else {
					// not first
					prevResult = execOperation(prevResult, op2, operator);
				}
			}
		}
//		System.out.println(prevResult);

		return prevResult.iterator();
	}

	private Iterator execOneTerm(SQLTerm sqlTerm) {
		// TODO Auto-generated method stub
		return null;
	}

	private Iterator execOneTermIndex(SQLTerm op, Vector<Integer> indices1, Vector<Index> tableIndices)
			throws DBAppException {

		boolean op1Clustering = containsClustering(op);
		String oper1 = op._strOperator;

		Vector<Hashtable<String, Object>> r1;

		if (op1Clustering && (oper1.equals("=") || oper1.equals(">=") || oper1.equals(">")))
			r1 = execTermBinary(op);
		else {
			if (indices1.isEmpty()) {
				r1 = execTermLinearly(op);
			} else {
				Index indexUsed = tableIndices.get(indices1.get(0));
				r1 = execTermUsingIndex(op, indexUsed);
			}
		}

		return r1.iterator();

	}

	public Vector<Hashtable<String, Object>> execOperationAnd(Vector<SQLTerm> andedTerms,
			Vector<Vector<Integer>> termIndexNum, Vector<Index> tableIndices) throws DBAppException {

		Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();
		boolean first = true;

		while (andedTerms.size() > 0) {

			Vector<Vector<SQLTerm>> commonIndex = new Vector<Vector<SQLTerm>>();

			for (int i = 0; i < tableIndices.size(); i++) {
				Vector<SQLTerm> terms = new Vector<SQLTerm>();
				for (int j = 0; j < termIndexNum.size(); j++) {
					Vector<Integer> curTerm = termIndexNum.get(j);
					if (curTerm.contains(i))
						terms.add(andedTerms.get(j));
				}
				commonIndex.add(terms);
			}

			Vector<SQLTerm> max = commonIndex.get(0);
			Index indexToBeUsed = tableIndices.get(0);
			int counter = 0;

			for (Vector<SQLTerm> tmp : commonIndex) {
				if (tmp.size() > max.size()) {
					max = tmp;
					indexToBeUsed = tableIndices.get(counter);
				}
				counter++;
			}

			for (int i = andedTerms.size() - 1; i >= 0; i--) {
				SQLTerm cur = andedTerms.get(i);
				if (max.contains(cur)) {
					andedTerms.remove(i);
					termIndexNum.remove(i);
				}
			}

			if (max.size() > 0) {
				if (first) {
					result = execANDTermsUsingIndex(max, indexToBeUsed);
					first = false;
				} else {
					result = intersect(result, execANDTermsUsingIndex(max, indexToBeUsed));
				}
			} else {

				for (SQLTerm op : andedTerms) {
					result = execOperation(result, op, "AND");
				}
				break;
			}

		}
		return result;
	}

	public Vector<Hashtable<String, Object>> execANDTermsUsingIndex(Vector<SQLTerm> andedTerms, Index index) {

		Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();
		Vector<Vector<Integer>> coordinatesToSearch = findCoordinates(andedTerms, index);
		String tableName = andedTerms.get(0)._strTableName;

		Vector<String> buckets = new Vector<String>();
		Vector<String> pages = new Vector<String>();

		for (Vector<Integer> v : coordinatesToSearch) {
			String s = (String) index.get(v);
			if (!buckets.contains(s))
				buckets.add(s);
		}

		for (String curBucket : buckets) {
			int count = 0;
			File f = new File(this.dir + curBucket + count + ".class");

			while (f.exists()) {
				try {
					Vector<BucketData> bucket = (Vector<BucketData>) deserialize(curBucket + count);
					// System.out.println(bucket);
					for (int i = 0; i < bucket.size(); i++) {
						BucketData data = bucket.get(i);
						if (satisfySql(data.data, andedTerms)) {
							if (!pages.contains(data.pageName)) {
								pages.add(data.pageName);
							}
						}
					}
				} catch (ClassNotFoundException | IOException e) {
					e.printStackTrace();
				}
				count++;
				f = new File(this.dir + curBucket + count + ".class");
			}
		}
		for (String pageName : pages) {
			try {
				Vector<Hashtable<String, Object>> page = (Vector<Hashtable<String, Object>>) deserialize(pageName);
				for (int j = 0; j < page.size(); j++) {
					Hashtable<String, Object> row = page.get(j);
					if (satisfySql(row, andedTerms)) {
						result.add(row);
					}
				}
			} catch (ClassNotFoundException | IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	public boolean satisfySql(Hashtable<String, Object> row, Vector<SQLTerm> andedTerms) {
		boolean flag = true;

		for (SQLTerm sql : andedTerms) {
			String colName = sql._strColumnName;
			Comparable compValue = (Comparable) sql._objValue;
			String operator = sql._strOperator;

			Comparable rowValue = (Comparable) row.get(colName);

			if (operator.equals("=")) {
				if (rowValue.compareTo(compValue) == 0) {
					flag = flag && true;
				} else {
					flag = false;
					break;
				}
			} else if (operator.equals(">")) {
				if (rowValue.compareTo(compValue) > 0) {
					flag = flag && true;
				} else {
					flag = false;
					break;
				}

			} else if (operator.equals(">=")) {
				if (rowValue.compareTo(compValue) >= 0) {
					flag = flag && true;
//					System.out.println(">=" + rowValue);
				} else {
					flag = false;
					break;
				}

			} else if (operator.equals("<")) {
				if (rowValue.compareTo(compValue) < 0) {
					flag = flag && true;
				} else {
					flag = false;
					break;
				}

			} else if (operator.equals("<=")) {
				if (rowValue.compareTo(compValue) <= 0) {
					flag = flag && true;
				} else {
					flag = false;
					break;
				}

			} else if (operator.equals("!=")) {
				if (rowValue.compareTo(compValue) != 0) {
					flag = flag && true;
				} else {
					flag = false;
					break;
				}
			}
		}
		return flag;
	}

	public Vector<Vector<Integer>> findCoordinates(Vector<SQLTerm> andedTerms, Index index) {

		Vector<Vector<Integer>> coordinates = new Vector<Vector<Integer>>();
		String tableName = andedTerms.get(0)._strTableName;
		Vector<String> colNames = index.colNames;

		Vector<Vector<Integer>> allowedValues = new Vector<Vector<Integer>>();

		for (int i = 0; i < index.depth; i++) {
			Vector<Integer> v = new Vector<Integer>();
			allowedValues.add(v);
		}

		for (SQLTerm sql : andedTerms) {

			String colName = sql._strColumnName;
			Comparable compValue = (Comparable) sql._objValue;
			String operator = sql._strOperator;

			int colPositionInIndex = colNames.indexOf(colName);
			int value = indexBinarySearch(index.maxRange.get(colPositionInIndex), compValue, 0, 9);

			if (operator.equals("=")) {
				if (!allowedValues.get(colPositionInIndex).contains(value))
					allowedValues.get(colPositionInIndex).add(value);
			} else if (operator.equals(">=") || operator.equals(">")) {
				for (int i = value; i < 10; i++) {
					if (!allowedValues.get(colPositionInIndex).contains(i))
						allowedValues.get(colPositionInIndex).add(i);

				}
			} else if (operator.equals("<=") || operator.equals("<")) {
				for (int i = 0; i <= value; i++) {
					if (!allowedValues.get(colPositionInIndex).contains(i))
						allowedValues.get(colPositionInIndex).add(i);
				}
			} else if (operator.equals("!=")) {
				for (int i = 0; i <= 10; i++) {
					if (!allowedValues.get(colPositionInIndex).contains(i))
						allowedValues.get(colPositionInIndex).add(i);
				}
			}
		}

//		System.out.println("allow " + allowedValues);
		for (int i = 0; i < allowedValues.size(); i++) {
			if (allowedValues.get(i).size() == 0) {

				for (int j = 0; j < 10; j++) {
					allowedValues.get(i).add(j);
				}
			}
		}

		coordinates = perms(allowedValues);
		return coordinates;
	}

	public Vector<Vector<Integer>> perms(Vector<Vector<Integer>> arr) {

		Vector<Vector<Integer>> res = new Vector<Vector<Integer>>();
		int n = arr.size();

		int[] indices = new int[n];
		for (int i = 0; i < n; i++)
			indices[i] = 0;

		while (true) {

			Vector<Integer> temp = new Vector<Integer>();
			for (int i = 0; i < n; i++) {
				temp.add(arr.get(i).get(indices[i]));
			}
			res.add(temp);

			int next = n - 1;
			while (next >= 0 && (indices[next] + 1 >= arr.get(next).size()))
				next--;

			if (next < 0)
				return res;

			indices[next]++;
			for (int i = next + 1; i < n; i++)
				indices[i] = 0;
		}
	}

	public Vector<Hashtable<String, Object>> execOperationIndex(Vector<Hashtable<String, Object>> r1, SQLTerm op,
			String operator, Vector<Integer> indicesOfSql, Vector<Index> tableIndices) throws DBAppException {

		Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();

		boolean op1Clustering = containsClustering(op);
		String oper1 = op._strOperator;

		Vector<Hashtable<String, Object>> r2;

		if (op1Clustering && (oper1.equals("=") || oper1.equals(">=") || oper1.equals(">")))
			r2 = execTermBinary(op);
		else {
			if (indicesOfSql.isEmpty()) {
				r2 = execTermLinearly(op);
			} else {
				Index indexUsed = tableIndices.get(indicesOfSql.get(0));
				r2 = execTermUsingIndex(op, indexUsed);
			}
		}

		if (operator.equals("OR")) {
			result = union(r1, r2);
		} else if (operator.equals("XOR")) {
			result = unionMinusIntersect(r1, r2);
		}

		return result;
	}

	public Vector<Hashtable<String, Object>> execOperationIndex(SQLTerm op1, SQLTerm op2, String operator,
			Vector<Integer> indices1, Vector<Integer> indices2, Vector<Index> tableIndices) throws DBAppException {

		Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();

		boolean op1Clustering = containsClustering(op1);
		boolean op2Clustering = containsClustering(op2);
		String oper1 = op1._strOperator;
		String oper2 = op2._strOperator;

		Vector<Hashtable<String, Object>> r1;
		Vector<Hashtable<String, Object>> r2;

		if (op1Clustering && (oper1.equals("=") || oper1.equals(">=") || oper1.equals(">")))
			r1 = execTermBinary(op1);
		else {
			if (indices1.isEmpty()) {
				r1 = execTermLinearly(op1);
			} else {
				Index indexUsed = tableIndices.get(indices1.get(0));
				r1 = execTermUsingIndex(op1, indexUsed);
			}
		}

		if (op2Clustering && (oper2.equals("=") || oper2.equals(">=") || oper2.equals(">")))
			r2 = execTermBinary(op2);
		else {
			if (indices2.isEmpty()) {
				r2 = execTermLinearly(op2);
			} else {
				Index indexUsed = tableIndices.get(indices2.get(0));
				r2 = execTermUsingIndex(op2, indexUsed);
			}
		}

		if (operator.equals("OR")) {
			result = union(r1, r2);
		} else if (operator.equals("XOR")) {
			result = unionMinusIntersect(r1, r2);
		}

		return result;
	}

	public Vector<Hashtable<String, Object>> execTermUsingIndex(SQLTerm sql, Index index) throws DBAppException {

		Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();

		String colName = sql._strColumnName;
		Comparable compValue = (Comparable) sql._objValue;
		String operator = sql._strOperator;
		Vector<String> colNames = index.colNames;

		int colPositionInIndex = colNames.indexOf(colName);

		Vector<String> pages = new Vector<String>();

		if (operator.equals("=")) {
			Vector<Integer> coordinates = new Vector<Integer>();
			for (int i = 0; i < colNames.size(); i++) {
				if (i == colPositionInIndex) {
					int value = indexBinarySearch(index.maxRange.get(colPositionInIndex), compValue, 0, 9);
					coordinates.add(value);
				} else {
					coordinates.add(-1);
				}
			}

			Vector<Object> buckets = index.getRange(coordinates);

			for (Object curBucketObj : buckets) {
				String curBucket = (String) curBucketObj;
				curBucket = curBucket.substring(0, curBucket.length());
				int count = 0;

				File f = new File(this.dir + curBucket + count + ".class");

				while (f.exists()) {
					try {
						Vector<BucketData> bucket = (Vector<BucketData>) deserialize(curBucket + count);
						for (int i = 0; i < bucket.size(); i++) {
							BucketData data = bucket.get(i);
							Comparable compBucketValue = (Comparable) data.data.get(colName);

							if (compBucketValue.compareTo(compValue) == 0) {
								if (!pages.contains(data.pageName))
									pages.add(data.pageName);

							}
						}
					} catch (ClassNotFoundException | IOException e) {
						e.printStackTrace();
					}

					count++;
					f = new File(this.dir + curBucket + count + ".class");
				}
			}

			for (String pageName : pages) {
				try {
					Vector<Hashtable<String, Object>> page = (Vector<Hashtable<String, Object>>) deserialize(pageName);
					for (int j = 0; j < page.size(); j++) {
						Hashtable<String, Object> row = page.get(j);
						Comparable compRowValue = (Comparable) row.get(colName);
						if (compRowValue.compareTo(compValue) == 0) {
							result.add(row);
						}
					}
				} catch (ClassNotFoundException | IOException e) {
					e.printStackTrace();
				}
			}

		} else if (operator.equals(">=") || operator.equals(">")) {

			Vector<Integer> coordinates = new Vector<Integer>();
			int positionOfCol = indexBinarySearch(index.maxRange.get(colPositionInIndex), compValue, 0, 9);

			Vector<Object> buckets = null;
			while (positionOfCol < 10) {

				for (int i = 0; i < colNames.size(); i++) {
					if (i == colPositionInIndex) {
						coordinates.add(positionOfCol);
					} else {
						coordinates.add(-1);
					}
				}

				buckets = merge(index.getRange(coordinates), buckets);
				positionOfCol++;
				coordinates.clear();
			}

			for (Object curBucketObj : buckets) {
				String curBucket = (String) curBucketObj;
				curBucket = curBucket.substring(0, curBucket.length());
				int count = 0;

				File f = new File(this.dir + curBucket + count + ".class");

				while (f.exists()) {
					try {
						Vector<BucketData> bucket = (Vector<BucketData>) deserialize(curBucket + count);

						for (int i = 0; i < bucket.size(); i++) {
							BucketData data = bucket.get(i);
							Comparable compBucketValue = (Comparable) data.data.get(colName);

							if (compBucketValue.compareTo(compValue) >= 0) {
								if (!pages.contains(data.pageName))
									pages.add(data.pageName);
							}
						}
					} catch (ClassNotFoundException | IOException e) {
						e.printStackTrace();
					}

					count++;
					f = new File(this.dir + curBucket + count + ".class");
				}
			}

			for (String pageName : pages) {
				try {
					Vector<Hashtable<String, Object>> page = (Vector<Hashtable<String, Object>>) deserialize(pageName);
					for (int j = 0; j < page.size(); j++) {
						Hashtable<String, Object> row = page.get(j);
						Comparable compRowValue = (Comparable) row.get(colName);
						if (compRowValue.compareTo(compValue) > 0) {
							result.add(row);
						} else if (compRowValue.compareTo(compValue) == 0 && operator.equals(">=")) {
							result.add(row);
						}
					}
				} catch (ClassNotFoundException | IOException e) {
					e.printStackTrace();
				}
			}

		} else if (operator.equals("<=") || operator.equals("<")) {
			Vector<Integer> coordinates = new Vector<Integer>();
			int positionOfCol = indexBinarySearch(index.maxRange.get(colPositionInIndex), compValue, 0, 9);

			Vector<Object> buckets = null;
			while (positionOfCol >= 0) {
				for (int i = 0; i < colNames.size(); i++) {
					if (i == colPositionInIndex) {
						coordinates.add(positionOfCol);
					} else {
						coordinates.add(-1);
					}
				}

				buckets = merge(index.getRange(coordinates), buckets);
				positionOfCol--;
				coordinates.clear();
			}

			for (Object curBucketObj : buckets) {
				String curBucket = (String) curBucketObj;
				curBucket = curBucket.substring(0, curBucket.length());
				int count = 0;

				File f = new File(this.dir + curBucket + count + ".class");

				while (f.exists()) {
					try {
						Vector<BucketData> bucket = (Vector<BucketData>) deserialize(curBucket + count);

						for (int i = 0; i < bucket.size(); i++) {
							BucketData data = bucket.get(i);
							Comparable compBucketValue = (Comparable) data.data.get(colName);

							if (compBucketValue.compareTo(compValue) <= 0) {
								if (!pages.contains(data.pageName)) {
									pages.add(data.pageName);
								}
							}
						}
					} catch (ClassNotFoundException | IOException e) {
						e.printStackTrace();
					}

					count++;
					f = new File(this.dir + curBucket + count + ".class");
				}
			}

			for (String pageName : pages) {
				try {
					Vector<Hashtable<String, Object>> page = (Vector<Hashtable<String, Object>>) deserialize(pageName);
					for (int j = 0; j < page.size(); j++) {
						Hashtable<String, Object> row = page.get(j);
						Comparable compRowValue = (Comparable) row.get(colName);
						if (compRowValue.compareTo(compValue) < 0) {
							result.add(row);
						} else if (compRowValue.compareTo(compValue) == 0 && operator.equals("<=")) {
							result.add(row);
						}
					}
				} catch (ClassNotFoundException | IOException e) {
					e.printStackTrace();
				}
			}

		} else if (operator.equals("!=")) {
			result = execTermLinearly(sql);
		}

		return result;
	}

	public Vector<Object> merge(Vector<Object> b1, Vector<Object> b2) {
		if (b1 == null)
			b1 = new Vector<Object>();

		if (b2 == null)
			return b1;

		for (Object o : b2) {
			if (!b1.contains(o))
				b1.add(o);
		}
		return b1;
	}

	public Vector<Hashtable<String, Object>> execOperation(Vector<Hashtable<String, Object>> r1, SQLTerm op,
			String operator) throws DBAppException {

		Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();
		boolean opClustering = containsClustering(op);
		String oper = op._strOperator;
		Vector<Hashtable<String, Object>> r2;

		if (opClustering && (oper.equals("=") || oper.equals(">=") || oper.equals(">")))
			r2 = execTermBinary(op);
		else
			r2 = execTermLinearly(op);

		if (operator.equals("AND")) {
			result = intersect(r1, r2);
		} else if (operator.equals("OR")) {
			result = union(r1, r2);
		} else if (operator.equals("XOR")) {
			result = unionMinusIntersect(r1, r2);
		}

		return result;
	}

	public Vector<Hashtable<String, Object>> execOperation(SQLTerm op1, SQLTerm op2, String operator)
			throws DBAppException {

		Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();

		boolean op1Clustering = containsClustering(op1);
		boolean op2Clustering = containsClustering(op2);
		String oper1 = op1._strOperator;
		String oper2 = op2._strOperator;

		Vector<Hashtable<String, Object>> r1;
		Vector<Hashtable<String, Object>> r2;

		if (op1Clustering && (oper1.equals("=") || oper1.equals(">=") || oper1.equals(">")))
			r1 = execTermBinary(op1);
		else
			r1 = execTermLinearly(op1);

		if (op2Clustering && (oper2.equals("=") || oper2.equals(">=") || oper2.equals(">")))
			r2 = execTermBinary(op2);
		else
			r2 = execTermLinearly(op2);

		if (operator.equals("AND")) {
			result = intersect(r1, r2);
		} else if (operator.equals("OR")) {
			result = union(r1, r2);
		} else if (operator.equals("XOR")) {
			result = unionMinusIntersect(r1, r2);
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	public Vector<Hashtable<String, Object>> execTermBinary(SQLTerm sql) throws DBAppException {

		Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();
		String tableName = sql._strTableName;
		String colName = sql._strColumnName;
		Comparable compValue = (Comparable) sql._objValue;
		int pageNumber = 0;
		String operator = sql._strOperator;
		PageMetadata meta = null;

		try {
			meta = (PageMetadata) deserialize(tableName + "Meta");
		} catch (ClassNotFoundException | IOException e2) {
			throw new DBAppException("Table " + tableName + " does not exist");
		}
		Hashtable<Integer, Integer> htblpageNum = meta.getPageNumber();

		pageNumber = binarySearchPage(meta.getMaxInPages(), compValue, 0, meta.getMaxInPages().size() - 1);
		Vector<Hashtable<String, Object>> curPage;

		if (operator.equals("=")) {
			try {
				curPage = (Vector<Hashtable<String, Object>>) deserialize(
						tableName + "Page" + htblpageNum.get(pageNumber));
				for (Hashtable<String, Object> row : curPage) {
					Comparable rowValue = (Comparable) row.get(colName);
					if (rowValue instanceof Double && compValue instanceof Double) {
						if (Double.compare((Double) rowValue, (Double) compValue) == 0)
							result.add(row);
					}

					else if (rowValue.compareTo(compValue) == 0) {
						result.add(row);
					}
				}

			} catch (ClassNotFoundException | IOException e1) {
				e1.printStackTrace();
			}

		} else if (operator.equals(">") || operator.equals(">=")) {

			File f = new File(this.dir + tableName + "Page" + htblpageNum.get(pageNumber) + ".class");

			while (f.exists()) {
				try {
					curPage = (Vector<Hashtable<String, Object>>) deserialize(
							tableName + "Page" + htblpageNum.get(pageNumber));

					for (Hashtable<String, Object> row : curPage) {
						Comparable rowValue = (Comparable) row.get(colName);
						if (rowValue.compareTo(compValue) > 0) {
							result.add(row);
						} else if (rowValue.compareTo(compValue) == 0 && operator.equals(">=")) {
							result.add(row);
						}
					}
				} catch (ClassNotFoundException | IOException e) {
					break;
				}
				pageNumber++;
				f = new File(this.dir + tableName + "Page" + htblpageNum.get(pageNumber) + ".class");
			}
		}
		return result;
	}

	public boolean containsClustering(SQLTerm op) {
		String tableName = op._strTableName;
		String col = op._strColumnName;
		try {
			String row = "";
			String filePath = System.getProperty("user.dir");
			BufferedReader csvReader = new BufferedReader(
					new FileReader(filePath.concat(("/src/main/resources/metadata.csv"))));

			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(tableName)) {
					if (data[3].toLowerCase().equals("true")) {
						String colName = data[1];
						if (colName.equals(col)) {
							return true;
						}
					}
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public Vector<Hashtable<String, Object>> unionMinusIntersect(Vector<Hashtable<String, Object>> r1,
			Vector<Hashtable<String, Object>> r2) {

		Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();

		for (Hashtable<String, Object> ht : r1) {
			if (!result.contains(ht) && !r2.contains(ht)) {
				result.add(ht);
			}
		}
		for (Hashtable<String, Object> ht : r2) {
			if (!result.contains(ht) && !r1.contains(ht)) {
				result.add(ht);
			}
		}
		return result;
	}

	public Vector<Hashtable<String, Object>> intersect(Vector<Hashtable<String, Object>> r1,
			Vector<Hashtable<String, Object>> r2) {

		Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();

		for (Hashtable<String, Object> ht : r1) {
			if (!result.contains(ht) && r2.contains(ht)) {
				result.add(ht);
			}
		}
		return result;
	}

	public Vector<Hashtable<String, Object>> union(Vector<Hashtable<String, Object>> r1,
			Vector<Hashtable<String, Object>> r2) {

		Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();

		for (Hashtable<String, Object> ht : r1) {
			if (!result.contains(ht)) {
				result.add(ht);
			}
		}
		for (Hashtable<String, Object> ht : r2) {
			if (!result.contains(ht)) {
				result.add(ht);
			}
		}
		return result;
	}

	public Vector<Hashtable<String, Object>> execTermLinearly(SQLTerm sql) throws DBAppException {

		Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();
		String tableName = sql._strTableName;
		String colName = sql._strColumnName;
		Comparable compValue = (Comparable) sql._objValue;
		int pageNumber = 0;
		String operator = sql._strOperator;
		PageMetadata meta = null;

		try {
			meta = (PageMetadata) deserialize(tableName + "Meta");
		} catch (ClassNotFoundException | IOException e2) {
			throw new DBAppException("Table " + tableName + " does not exist");
		}

		Hashtable<Integer, Integer> htblpageNum = meta.getPageNumber();

		File f = new File(this.dir + tableName + "Page" + htblpageNum.get(pageNumber) + ".class");

		while (f.exists()) {
			Vector<Hashtable<String, Object>> curPage;
			try {
				curPage = (Vector<Hashtable<String, Object>>) deserialize(
						tableName + "Page" + htblpageNum.get(pageNumber));

				for (Hashtable<String, Object> row : curPage) {

					Comparable rowValue = (Comparable) row.get(colName);

					if (operator.equals("=")) {
						if (rowValue instanceof Double && compValue instanceof Double) {
							if (Double.compare((Double) rowValue, (Double) compValue) == 0)
								result.add(row);
						} else if (rowValue.compareTo(compValue) == 0) {
							result.add(row);
						}
					} else if (operator.equals(">=")) {
						if (rowValue.compareTo(compValue) >= 0) {
							result.add(row);
						}
					} else if (operator.equals("<=")) {
						if (rowValue.compareTo(compValue) <= 0) {
							result.add(row);
						}
					} else if (operator.equals(">")) {
						if (rowValue.compareTo(compValue) > 0) {
							result.add(row);
						}
					} else if (operator.equals("<")) {
						if (rowValue.compareTo(compValue) < 0) {
							result.add(row);
						}
					} else if (operator.equals("!=")) {
						if (rowValue.compareTo(compValue) != 0) {
							result.add(row);
						}
					}
				}

			} catch (ClassNotFoundException | IOException e) {
				break;
			}
			pageNumber++;
			f = new File(this.dir + tableName + "Page" + htblpageNum.get(pageNumber) + ".class");
		}
		return result;
	}

	public boolean checkValidSQLTerm(SQLTerm sql) throws DBAppException {
		boolean res = true;
		boolean colExists = false;
		String tableName = sql._strTableName;

		try {
			String row = "";
			String filePath = System.getProperty("user.dir");
			BufferedReader csvReader = new BufferedReader(
					new FileReader(filePath.concat(("/src/main/resources/metadata.csv"))));
			boolean tableNotFound = true;
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (data[0].equals(tableName)) {
					tableNotFound = false;

					String colName = data[1];
					if (colName.equals(sql._strColumnName)) {
						colExists = true;

						switch (data[2].toLowerCase()) {
						case "java.lang.integer":
							if (!(sql._objValue instanceof Integer))
								res = false;
							break;

						case "java.lang.double":
							if (!(sql._objValue instanceof Double))
								res = false;
							break;
						case "java.util.date":
							if (!(sql._objValue instanceof Date))
								res = false;
							break;
						case "java.lang.string":
							if (!(sql._objValue instanceof String))
								res = false;
							break;
						default:

						}

					}

				}
			}
			if (tableNotFound)
				throw new DBAppException("Table " + tableName + " does not exist");

		} catch (IOException e) {
			e.printStackTrace();
		}

		return res && colExists;
	}

	public static void writeToFile(String input) throws IOException {

		String filePath = System.getProperty("user.dir");
		FileWriter fstream = new FileWriter(filePath.concat(("/src/main/resources/metadata.csv")), true);
		BufferedWriter out = new BufferedWriter(fstream);
		out.write(input + "\n");
		out.close();
	}

	public static void overwriteFile(String input) throws IOException {

		String filePath = System.getProperty("user.dir");
		FileWriter fstream = new FileWriter(filePath.concat(("/src/main/resources/metadata.csv")), false);
		BufferedWriter out = new BufferedWriter(fstream);
		out.write(input + "\n");
		out.close();
	}

	public static void serialize(Object object, String filename) throws IOException {

		String filePath = System.getProperty("user.dir");
		FileOutputStream file = new FileOutputStream(
				filePath.concat(("/src/main/resources/data/" + filename + ".class")));
		ObjectOutputStream out = new ObjectOutputStream(file);
		out.writeObject(object);
		out.close();
		file.close();
	}

	public static Object deserialize(String filename) throws ClassNotFoundException, IOException {

		String filePath = System.getProperty("user.dir");
		FileInputStream file = new FileInputStream(
				filePath.concat(("/src/main/resources/data/" + filename + ".class")));
		ObjectInputStream in = new ObjectInputStream(file);

		Object object = in.readObject();
		in.close();
		file.close();

		return object;
	}

	public static int binarySearch(Vector<Hashtable<String, Object>> v, Comparable key, int lo, int hi,
			String ClusteringKeyName) {
		if (hi >= lo) {
			int mid = lo + (hi - lo) / 2;
			if (((Comparable) v.get(mid).get(ClusteringKeyName)).compareTo(key) == 0) {
				return -1;
			}
			if (((Comparable) v.get(mid).get(ClusteringKeyName)).compareTo(key) > 0) {
				return binarySearch(v, key, lo, mid - 1, ClusteringKeyName);// search in left subarray
			} else {
				return binarySearch(v, key, mid + 1, hi, ClusteringKeyName);// search in right subarray
			}
		}
		return lo;

	}

	public static int binarySearchPage(Vector<Comparable> v, Comparable key, int lo, int hi) {
		if (key instanceof Double) {
			if (hi >= lo) {
				int mid = lo + (hi - lo) / 2;
				if (Double.compare((Double) v.get(mid), (Double) key) == 0) {
					return mid;
				}
				if (Double.compare((Double) v.get(mid), (Double) key) > 0) {
					return binarySearchPage(v, key, lo, mid - 1);// search in left subarray
				} else {
					return binarySearchPage(v, key, mid + 1, hi);// search in right subarray
				}
			}

			if (lo >= v.size())
				return v.size() - 1;

			return lo;

		} else {
			if (hi >= lo) {
				int mid = lo + (hi - lo) / 2;
				if (v.get(mid).compareTo(key) == 0) {
					return mid;
				}
				if (v.get(mid).compareTo(key) > 0) {
					return binarySearchPage(v, key, lo, mid - 1);// search in left subarray
				} else {
					return binarySearchPage(v, key, mid + 1, hi);// search in right subarray
				}
			}

			if (lo >= v.size())
				return v.size() - 1;

			return lo;
		}

	}

	public static int binarySearchUpdate(Vector<Hashtable<String, Object>> v, Comparable key, int lo, int hi,
			String ClusteringKeyName) {
		if (key instanceof Double) {
			if (hi >= lo) {
				int mid = lo + (hi - lo) / 2;

				if (Double.compare((Double) v.get(mid).get(ClusteringKeyName), (Double) key) == 0) {
					return mid;
				}
				if (Double.compare((Double) v.get(mid).get(ClusteringKeyName), (Double) key) > 0) {
					return binarySearchUpdate(v, key, lo, mid - 1, ClusteringKeyName);// search in left subarray
				} else {
					return binarySearchUpdate(v, key, mid + 1, hi, ClusteringKeyName);// search in right subarray
				}
			}
			return -1;
		} else {
			if (hi >= lo) {
				int mid = lo + (hi - lo) / 2;
				if (((Comparable) v.get(mid).get(ClusteringKeyName)).compareTo(key) == 0) {
					return mid;
				}
				if (((Comparable) v.get(mid).get(ClusteringKeyName)).compareTo(key) > 0) {
					return binarySearchUpdate(v, key, lo, mid - 1, ClusteringKeyName);// search in left subarray
				} else {
					return binarySearchUpdate(v, key, mid + 1, hi, ClusteringKeyName);// search in right subarray
				}
			}
			return -1;
		}

	}

	public void setIndexRanges(Index grid, String[] columnNames, Vector<Object> dataTypes, Vector<Object> min,
			Vector<Object> max) throws ParseException {

		// ranges are ex 10-20, 10 is inclusive, 20 is exclusive except in last range
		for (int i = 0; i < dataTypes.size(); i++) {

			Vector<Comparable> newVec1 = new Vector<Comparable>();
			grid.maxRange.add(i, newVec1);
			Vector<Comparable> newVec2 = new Vector<Comparable>();
			grid.minRange.add(i, newVec2);

			String type = ((String) dataTypes.get(i)).toLowerCase();

			if (type.equals("java.lang.integer")) {
				int colLo = Integer.parseInt((String) min.get(i));
				int colHi = Integer.parseInt((String) max.get(i));
				int del = (colHi - colLo) / 10;

				int lo = colLo;
				int hi = colLo + del;
				for (int j = 0; j < 10; j++) {
					grid.minRange.get(i).add(j, lo);
					grid.maxRange.get(i).add(j, hi);
					lo = hi;
					hi = lo + del;
					if (hi > colHi)
						hi = colHi;
					else if (j == 9) {
						grid.maxRange.get(i).set(9, colHi);
					}
				}
			} else if (type.equals("java.lang.double")) {
				double colLo = Double.parseDouble((String) min.get(i));
				double colHi = Double.parseDouble((String) max.get(i));
				double del = (colHi - colLo) / 10.0;
				double lo = colLo;
				double hi = colLo + del;
				for (int j = 0; j < 10; j++) {
					grid.minRange.get(i).add(j, lo);
					grid.maxRange.get(i).add(j, hi);
					lo = hi;
					hi = lo + del;
					if (hi > colHi)
						hi = colHi;
					else if (j == 9) {
						grid.maxRange.get(i).set(9, colHi);
					}
				}
			} else if (type.equals("java.lang.string")) {

				String colLo = (String) min.get(i);
				String colHi = (String) max.get(i);

				Vector<Vector<Comparable>> vec = getStringRange(colLo, colHi);
				grid.minRange.set(i, vec.get(0));
				grid.maxRange.set(i, vec.get(1));

			} else if (type.equals("java.util.date")) {

				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				Date colLow = format.parse((String) min.get(i));
				Date colHigh = format.parse((String) max.get(i));
				long colLo = colLow.getTime();
				long colHi = colHigh.getTime();
				long del = (colHi - colLo) / 10;
				long lo = colLo;
				long hi = colLo + del;
				for (int j = 0; j < 10; j++) {
					grid.minRange.get(i).add(j, new Date(lo));
					grid.maxRange.get(i).add(j, new Date(hi));
					lo = hi;
					hi = lo + del;
					if (hi > colHi)
						hi = colHi;
					else if (j == 9) {
						grid.maxRange.get(i).set(9, colHigh);
					}
				}

			}

		}

//		System.out.println(grid.minRange.get(0));
//		System.out.println(grid.maxRange.get(0));
//		
//		System.out.println(grid.minRange.get(1));
//		System.out.println(grid.maxRange.get(1));

	}

	public static Vector<Vector<Object>> getDataFromCsv(String tableName, String[] columnNames) {

		Vector<Vector<Object>> dataFromCsv = new Vector<Vector<Object>>();
		Vector<Object> dataTypes = new Vector<Object>();
		Vector<Object> minValues = new Vector<Object>();
		Vector<Object> maxValues = new Vector<Object>();
		for (int i = 0; i < columnNames.length; i++) {
			try {
				String row = "";
				String filePath = System.getProperty("user.dir");
				BufferedReader csvReader = new BufferedReader(
						new FileReader(filePath.concat(("/src/main/resources/metadata.csv"))));
				while ((row = csvReader.readLine()) != null) {
					String[] data = row.split(",");
					if (data[0].equals(tableName) && data[1].equals(columnNames[i])) {
						dataTypes.add(i, data[2]);
						minValues.add(i, data[5]);
						maxValues.add(i, data[6]);
						break;

					}
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		dataFromCsv.add(0, dataTypes);
		dataFromCsv.add(1, minValues);
		dataFromCsv.add(2, maxValues);
		return dataFromCsv;
	}

	public void updateCSV(String oldRow) {

		String info = "";
		String newRow = "";

		try {
			String row = "";
			String filePath = System.getProperty("user.dir");
			BufferedReader csvReader = new BufferedReader(
					new FileReader(filePath.concat(("/src/main/resources/metadata.csv"))));
			while ((row = csvReader.readLine()) != null) {
				String[] data = row.split(",");
				if (row.equals(oldRow)) {
					newRow += data[0] + ",";
					newRow += data[1] + ",";
					newRow += data[2] + ",";
					newRow += data[3] + ",";
					newRow += "True" + ",";
					newRow += data[5] + ",";
					newRow += data[6];
					info += newRow + "\n";
				} else
					info += row + "\n";
			}
			info = info.trim();

		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			overwriteFile(info);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void populateIndex(Index grid, String tableName, String clusteringKey) throws DBAppException {

		Vector<String> colNames = grid.colNames;

		PageMetadata meta = null;
		try {
			meta = (PageMetadata) deserialize(tableName + "Meta");
		} catch (ClassNotFoundException | IOException e2) {
			return;
		}

		Hashtable<Integer, Integer> htblpageNum = meta.pageNumber;

		int pageNumber = 0;

		while (true) {
			Vector<Hashtable<String, Object>> curPage;
			try {
				curPage = (Vector<Hashtable<String, Object>>) deserialize(
						tableName + "Page" + htblpageNum.get(pageNumber));

				String pageName = tableName + "Page" + htblpageNum.get(pageNumber);

				for (Hashtable<String, Object> row : curPage) {

					Comparable clusteringKeyObject = (Comparable) row.get(clusteringKey);
					Vector<Integer> insertPos = new Vector<Integer>();
					Hashtable<String, Object> indexedCols = new Hashtable<String, Object>();
					for (String colName : colNames) {

						indexedCols.put(colName, row.get(colName));
						int i;

						for (i = 0; i < colNames.size(); i++) {
							if (colNames.get(i).equals(colName))
								break;
						}
						Vector<Comparable> maxValues = grid.maxRange.get(i);
						Comparable c = (Comparable) row.get(colName);
						int pos = indexBinarySearch(maxValues, c, 0, 9);
						insertPos.add(pos);
					}
					insertIntoIndex(grid, insertPos, indexedCols, pageName, tableName, clusteringKeyObject);
				}

			} catch (ClassNotFoundException | IOException e) {
				break;
			}
			pageNumber++;
		}
	}

	public void insertIntoIndex(Index grid, Vector<Integer> insertPos, Hashtable<String, Object> data, String pageName,
			String tableName, Comparable clusteringKey) {

		String bucketName = tableName + "Bucket";
		for (String s : grid.colNames)
			bucketName += s;
		for (Integer i : insertPos)
			bucketName += i;
		bucketName += "_";

		BucketData bucketData = new BucketData(data, pageName, clusteringKey);

		if (grid.get(insertPos) == null) {
			// create first bucket
			Vector<BucketData> bucket = new Vector<BucketData>();
			bucket.add(bucketData);
			try {
				serialize(bucket, bucketName + "0");
				grid.numberOfBuckets++;
				grid.set(insertPos, bucketName);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else {
			// check if bucket is full then insert
			try {

				int count = 0;
				File f = new File(this.dir + bucketName + 0 + ".class");
				while (f.exists()) {
					count++;
					f = new File(this.dir + bucketName + count + ".class");
				}

				Vector<BucketData> bucket = (Vector<BucketData>) deserialize(bucketName + (count - 1));
				if (bucket.size() >= maxPerBucket) {
					Vector<BucketData> newbucket = new Vector<BucketData>();
					newbucket.add(bucketData);
					serialize(newbucket, bucketName + (count));
					grid.numberOfBuckets++;

				} else {
					bucket.add(bucketData);
					serialize(bucket, bucketName + (count - 1));

				}

			} catch (ClassNotFoundException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	public static int indexBinarySearch(Vector<Comparable> maxValues, Comparable c, int lo, int hi) {
		if (hi >= lo) {
			int mid = lo + (hi - lo) / 2;
			Comparable mi = maxValues.get(mid);

			if (c.compareTo(mi) == 0) {
				return (mid == 9) ? 9 : mid + 1;
			}
			if (c.compareTo(mi) > 0) {
				return indexBinarySearch(maxValues, c, mid + 1, hi);
			} else {
				return indexBinarySearch(maxValues, c, lo, mid - 1);
			}
		}

		if (lo > 9)
			lo = 9;

		if (lo < 0)
			lo = 0;

		return lo;
	}

	public void insertRowIntoIndex(String tableName, Hashtable<String, Object> colNameValue, String pageName,
			Comparable clusteringKey) {

		try {
			Vector<Index> indices = (Vector<Index>) deserialize(tableName + "Index");

			for (Index grid : indices) {
				Hashtable<String, Object> bucketData = new Hashtable<String, Object>();
				Vector<Integer> insertPos = new Vector<Integer>();

				for (String colName : grid.colNames) {
					bucketData.put(colName, colNameValue.get(colName));

					int i;
					for (i = 0; i < grid.colNames.size(); i++) {
						if (grid.colNames.get(i).equals(colName))
							break;
					}
					Vector<Comparable> maxValues = grid.maxRange.get(i);
					Comparable c = (Comparable) colNameValue.get(colName);
					int pos = indexBinarySearch(maxValues, c, 0, 9);
					insertPos.add(pos);

				}

				insertIntoIndex(grid, insertPos, bucketData, pageName, tableName, clusteringKey);

			}

		} catch (ClassNotFoundException | IOException e) {
			// should never enter the catch unless disaster strikes
			e.printStackTrace();
		}
	}

	public void deleteRowFromIndex(String tableName, Hashtable<String, Object> colNameValue, Comparable clusteringKey) {

		// colNameValue contains all the data of the row

		try {
			Vector<Index> indices = (Vector<Index>) deserialize(tableName + "Index");

			for (Index grid : indices) {
				Hashtable<String, Object> bucketData = new Hashtable<String, Object>();
				Vector<Integer> deletePos = new Vector<Integer>();

				for (String colName : grid.colNames) {
					bucketData.put(colName, colNameValue.get(colName));

					int i;
					for (i = 0; i < grid.colNames.size(); i++) {
						if (grid.colNames.get(i).equals(colName))
							break;
					}
					Vector<Comparable> maxValues = grid.maxRange.get(i);
					Comparable c = (Comparable) colNameValue.get(colName);
					int pos = indexBinarySearch(maxValues, c, 0, 9);
					deletePos.add(pos);

				}

				String bucketName = (String) grid.get(deletePos);
				int count = 0;
				File f = new File(this.dir + bucketName + count + ".class");

				boolean deleteSuccess = false;

				while (f.exists()) {
					Vector<BucketData> bucket = (Vector<BucketData>) deserialize(bucketName + count);

					Vector<BucketData> toDelete = new Vector<BucketData>();
					for (BucketData row : bucket) {
//						System.out.println(row);
						if (row.clusteringKey.compareTo(clusteringKey) == 0) {
							toDelete.add(row);
							deleteSuccess = true;
						}
					}

					for (BucketData row : toDelete) {
						bucket.remove(row);
					}

					if (bucket.isEmpty()) {

						boolean renameSuccess = true;
						int tmpPageNum = count;

						File file = new File(this.dir + bucketName + tmpPageNum + ".class");
						file.delete();

						while (renameSuccess) {
							File oldName = new File(this.dir + bucketName + (tmpPageNum + 1) + ".class");
							File newName = new File(this.dir + bucketName + tmpPageNum + ".class");
							try {
								renameSuccess = oldName.renameTo(newName);
								if (renameSuccess)
									System.out.println("File was renamed successfully");
							} catch (NullPointerException e) {
								break;
							}
							tmpPageNum++;
						}
						count--;
					} else {
						serialize(bucket, bucketName + count);
					}

					if (deleteSuccess)
						break;
					else {
						count++;
						f = new File(this.dir + bucketName + count + ".class");
					}
				}
			}

		} catch (ClassNotFoundException | IOException e) {
			// should never enter the catch unless disaster strikes
			e.printStackTrace();
		}

	}

	public Vector<Vector<Comparable>> getStringRange(String minStr, String maxStr) {
		Vector<Vector<Comparable>> res = new Vector<Vector<Comparable>>();

		Vector<Comparable> min = new Vector<Comparable>();
		Vector<Comparable> max = new Vector<Comparable>();

		if (minStr.charAt(0) >= 'A' && maxStr.charAt(0) <= 'Z') {
			min.add("A");
			min.add("C");
			min.add("E");
			min.add("G");
			min.add("J");
			min.add("M");
			min.add("P");
			min.add("S");
			min.add("U");
			min.add("X");

			max.add("C");
			max.add("E");
			max.add("G");
			max.add("J");
			max.add("M");
			max.add("P");
			max.add("S");
			max.add("U");
			max.add("X");
			max.add("Z");

		} else if (minStr.charAt(0) >= 'a' && maxStr.charAt(0) <= 'z') {
			min.add("a");
			min.add("c");
			min.add("e");
			min.add("g");
			min.add("j");
			min.add("m");
			min.add("p");
			min.add("s");
			min.add("u");
			min.add("x");

			max.add("c");
			max.add("e");
			max.add("g");
			max.add("j");
			max.add("m");
			max.add("p");
			max.add("s");
			max.add("u");
			max.add("x");
			max.add("z");

		} else if (minStr.charAt(0) >= 'A' && maxStr.charAt(0) <= 'z') {
			min.add("A");
			min.add("F");
			min.add("K");
			min.add("P");
			min.add("U");
			min.add("a");
			min.add("f");
			min.add("k");
			min.add("p");
			min.add("u");

			max.add("F");
			max.add("K");
			max.add("P");
			max.add("U");
			max.add("[");
			max.add("f");
			max.add("k");
			max.add("p");
			max.add("u");
			max.add("z");
		} else if (minStr.charAt(0) >= '0' && maxStr.charAt(0) <= '9') {
			min.add("0");
			min.add("1");
			min.add("2");
			min.add("3");
			min.add("4");
			min.add("5");
			min.add("6");
			min.add("7");
			min.add("8");
			min.add("9");

			max.add("1");
			max.add("2");
			max.add("3");
			max.add("4");
			max.add("5");
			max.add("6");
			max.add("7");
			max.add("8");
			max.add("9");
			max.add(":");
		} else {
			min.add("0");
			min.add("5");
			min.add("A");
			min.add("G");
			min.add("N");
			min.add("T");
			min.add("a");
			min.add("g");
			min.add("n");
			min.add("t");

			max.add("5");
			max.add(":");
			max.add("G");
			max.add("N");
			max.add("T");
			max.add("[");
			max.add("g");
			max.add("n");
			max.add("t");
			max.add("z");
		}

		res.add(min);
		res.add(max);
		return res;
	}

	public static void main(String[] args) throws DBAppException, ClassNotFoundException, IOException {

		String strTableName = "students";
		DBApp dbApp = new DBApp();

		Hashtable htblColNameValue = new Hashtable();

//		htblColNameValue.put("id", new Integer(20));  
//		htblColNameValue.put("name", new String("Zaky Noor"));
//		htblColNameValue.put("gpa", new Double(0.88));
//		dbApp.insertIntoTable(strTableName, htblColNameValue);	
//		
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer(22)); 
//		htblColNameValue.put("name", new String("Hazem"));
//		htblColNameValue.put("gpa", new Double(3.6));
//		dbApp.insertIntoTable(strTableName, htblColNameValue);
//		htblColNameValue.clear();
//	
//		
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer(60));  
//		htblColNameValue.put("name", new String("Ahmed Noor"));
//		htblColNameValue.put("gpa", new Double(0.95));
//		dbApp.insertIntoTable(strTableName, htblColNameValue);
//		htblColNameValue.clear();
//		
//		htblColNameValue.put("id", new Integer(80));  
//		htblColNameValue.put("name", new String("Dalia Noor"));
//		htblColNameValue.put("gpa", new Double(1.25));
//		dbApp.insertIntoTable(strTableName, htblColNameValue);
//		htblColNameValue.clear();
//		
//		htblColNameValue.put("id", new Integer(3)); 
//		htblColNameValue.put("name", new String("John Noor"));
//		htblColNameValue.put("gpa", new Double(1.5));
//		dbApp.insertIntoTable(strTableName, htblColNameValue);
//		htblColNameValue.clear();
//	
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer(5)); 
//		htblColNameValue.put("name", new String("Ahmed Noor"));
//		htblColNameValue.put("gpa", new Double(0.95));
//		dbApp.insertIntoTable(strTableName, htblColNameValue);
//		
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer(38));  
//		htblColNameValue.put("name", new String("David"));
//		htblColNameValue.put("gpa", new Double(2.88));
//		dbApp.insertIntoTable(strTableName, htblColNameValue);	
//

//------------------------------------------------------------------------------------

//		htblColNameValue.clear();
//		htblColNameValue.put("gpa", new Double(0.71));
//		htblColNameValue.put("name", "Hazem Hassan");
//		dbApp.updateTable(strTableName, "22", htblColNameValue);

//		
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer(38));
//		dbApp.deleteFromTable(strTableName, htblColNameValue);

//				htblColNameValue.clear();
//		htblColNameValue.put("gpa", new Double(3.6));
//		dbApp.deleteFromTable(strTableName, htblColNameValue);
//		
//		htblColNameValue.clear();
//		htblColNameValue.put("name", "Zaky Noor");
//		dbApp.deleteFromTable(strTableName, htblColNameValue);

//
//		System.out.println();

//		htblColNameValue.clear();
//		htblColNameValue.put("gpa", new Double(1.47));
//		dbApp.deleteFromTable(strTableName, htblColNameValue);
//		
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer(22));
//		dbApp.deleteFromTable(strTableName, htblColNameValue);
//		
//		
//		
//		htblColNameValue.put("gpa", new Double(1047));
//		dbApp.updateTable(strTableName, "5674567", htblColNameValue);

//		
//		String pname = "pcs";
//		System.out.println(((PageMetadata) deserialize(pname + "Meta")).getPageNumber().toString());
//		System.out.println(((PageMetadata) deserialize(pname + "Meta")).getMaxInPages().toString());
//		System.out.println();

//		System.out.println(deserialize(pname + "Page0").toString());
//		System.out.println(deserialize(pname + "Page1").toString());
//		System.out.println(deserialize(pname + "Page2").toString());
//		System.out.println(deserialize(pname + "Page3").toString());
//		System.out.println(deserialize(pname + "Page4").toString());
//		System.out.println(deserialize(pname + "Page5").toString());
//		System.out.println(deserialize(pname + "Page6").toString());
//		System.out.println(deserialize(pname + "Page7").toString());
//		

//-------------------------------------------------------------------------------------------------------
//
//		htblColNameValue.put("id", new Integer(104));
//		htblColNameValue.put("name", new String("Ahmed 5"));
//		htblColNameValue.put("gpa", new Double(1.35));
//		dbApp.insertIntoTable(strTableName, htblColNameValue);
//
//		
//		
//		System.out.println(deserialize("StudentPage0").toString());
//		System.out.println(deserialize("StudentPage1").toString());
//		System.out.println(deserialize("StudentPage2").toString());
//		System.out.println(deserialize("StudentPage3").toString());
//		System.out.println(deserialize("StudentPage4").toString());
//		System.out.println(deserialize("StudentPage5").toString());
//		System.out.println(deserialize("StudentPage6").toString());
//
//		System.out.println("--------------------------------------");
//
//		SQLTerm[] arrSQLTerms = new SQLTerm[2];
//
//		arrSQLTerms[0] = new SQLTerm();
//		arrSQLTerms[1] = new SQLTerm();
//		arrSQLTerms[2] = new SQLTerm();
//		arrSQLTerms[3] = new SQLTerm();
//
//		arrSQLTerms[0]._strTableName = "students";
//		arrSQLTerms[0]._strColumnName = "id";
//		arrSQLTerms[0]._strOperator = "=";
//		arrSQLTerms[0]._objValue = "88-4229";
//
//		arrSQLTerms[1]._strTableName = "students";
//		arrSQLTerms[1]._strColumnName = "gpa";
//		arrSQLTerms[1]._strOperator = ">=";
//		arrSQLTerms[1]._objValue = new Double(3.5);
//
//		arrSQLTerms[2]._strTableName = "students";
//		arrSQLTerms[2]._strColumnName = "gpa";
//		arrSQLTerms[2]._strOperator = "=";
//		arrSQLTerms[2]._objValue = new Double(1.47);
//
//		arrSQLTerms[3]._strTableName = "students";
//		arrSQLTerms[3]._strColumnName = "dob";
//		arrSQLTerms[3]._strOperator = ">=";
//		arrSQLTerms[3]._objValue = new Date(1995 - 1900, 5 - 1, 30);
//
//		String[] strarrOperators = new String[1];
//		strarrOperators[0] = "OR";
//		strarrOperators[1] = "AND";
//		strarrOperators[2] = "AND";
//
//		Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
//
//		System.out.println(
//				"-------------------------------------------------------------------------------------------------------");
//		System.out.println("Results:");
//
//		while (resultSet.hasNext())
//			System.out.println(resultSet.next());
//		System.out.println(
//				"-------------------------------------------------------------------------------------------------------");
//
//		System.out.println();
//
//		Vector indexVector = (Vector) deserialize("StudentIndex");
//		System.out.println(indexVector);
//
//		Index index = (Index) indexVector.get(0);
//
//		System.out.println(index.minRange);
//		System.out.println(index.maxRange);

//		System.out.println();
//		System.out.println(deserialize("StudentBucketidgpa02_0").toString());
//		System.out.println(deserialize("StudentBucketidgpa02_1").toString());
//		System.out.println(deserialize("StudentBucketidgpa03_0").toString());
//		System.out.println(deserialize("StudentBucketidgpa03_1").toString());
//		System.out.println(deserialize("studentsBucketidgpa99_0").toString());

//		System.out.println(deserialize("StudentBucketname0_0").toString());
//		System.out.println(deserialize("StudentBucketname0_1").toString());
//		System.out.println(deserialize("StudentBucketname0_2").toString());
//		System.out.println(deserialize("StudentBucketname1_0").toString());
//		System.out.println(deserialize("studentsIndex").toString());

//		System.out.println(deserialize("studentsBucketfirst_namelast_name15_0").toString());

//		String[] names = { "id", "first_name" };
//		Vector <Vector<Object>> data = getDataFromCsv("students", names);
//		for(int i = 0; i < data.size(); i++) {
//			System.out.print(data.get(i));
//		}

//		String[] cols = {"name"};
//		
//		dbApp.createIndex(strTableName, cols);
//		

//		
//		String[] data = { "hours"};
//		dbApp.createIndex("courses", data);

	}

}
