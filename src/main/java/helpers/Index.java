package helpers;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.io.*;
import java.util.*;

import static builders.IteratorBuilder.schemas;
import static helpers.CommonLib.TABLE_DIRECTORY;
import static helpers.CommonLib.blockSize;

public class Index {

    final static Map<String, List<BufferedReader>> bufferedReaderMap = new HashMap<String, List<BufferedReader>>();
    final static Map<String, List<String>> tempFileMap = new HashMap<String, List<String>>();
    public static List<String> indexFileLists = new ArrayList<String>();
    public static Map<String, String> indexMap = new HashMap<String, String>();
    static IndexSort sort = null;
    static List<String> fullFile = new ArrayList<String>();

    private static CommonLib commonLib = CommonLib.getInstance();
    private static int indexBlockSize = 0;
    private static LineNumberReader br = null;
    private static int initialCapacity = 20;


    private static List<Integer> pqSortIndex = new ArrayList<Integer>();

    //region Priority Queue
    private static PriorityQueue<HashMap<BufferedReader, String>> priorityQueue = new PriorityQueue<HashMap<BufferedReader, String>>(initialCapacity,
            new Comparator<HashMap<BufferedReader, String>>() { //TODO: Keep NULL values at the end in PQ.
                @Override
                public int compare(HashMap<BufferedReader, String> o1, HashMap<BufferedReader, String> o2) {
                    String a[] = null;
                    String b[] = null;

                    for (Map.Entry<BufferedReader, String> entry1 : o1.entrySet()) {
                        a = entry1.getValue().split("\\|");
                        break;
                    }

                    for (Map.Entry<BufferedReader, String> entry2 : o2.entrySet()) {
                        b = entry2.getValue().split("\\|");
                        break;
                    }

                    for (int i = 0; i < pqSortIndex.size(); i++) {
                        int index = pqSortIndex.get(i);

                        try {
                            if (isNumber(a[index])) {

                                double pv1 = Double.parseDouble(a[index]);
                                double pv2 = Double.parseDouble(b[index]);

                                if (pv1 < pv2)
                                    return -1;
                                else if (pv1 > pv2)
                                    return 1;
                                else {
                                    continue;
                                }

                            } else {

                                return a[index].compareTo(b[index]);
                            }


                        } catch (Exception e) {

                        }
                    }
                    return -1;
                }
            });

    //endregion

    String path = System.getProperty("user.dir");
    String fileName = "/createTableTeam3.sql";
    HashMap<String, Integer> hashMap = new HashMap();
    private List<String> indexList = new ArrayList<String>();


    public Index() {
        init();
    }


    public static void createIndex(CreateTable createTable) {
        init();

        if (createTable.getTable().getName().equals("LINEITEM"))
            indexBlockSize = 100000;
        else if (createTable.getTable().getName().equals("ORDERS"))
            indexBlockSize = 50000;
        else if (createTable.getTable().getName().equals("PART"))
            indexBlockSize = 50000;
        else if (createTable.getTable().getName().equals("CUSTOMER"))
            indexBlockSize = 50000;
        else if (createTable.getTable().getName().equals("SUPPLIER"))
            indexBlockSize = 50000;
        else if (createTable.getTable().getName().equals("PARTSUPP"))
            indexBlockSize = 50000;
        else if (createTable.getTable().getName().equals("NATION"))
            indexBlockSize = 50000;
        else
            indexBlockSize = 50000;

        String indexes[] = indexMap.get(createTable.getTable().getName()).split("\\|");

        boolean sorted = false; // TODO: change the flag

//        for (int i = 0; i < indexes.length; i++) {
//            buildIndex(createTable.getTable().getName(), indexes[i], sorted);
//            sorted = false;
//            //buildIndex(createTable.getTable().getName(), indexes[i], isPrimaryKey(createTable, indexes[i]));
//        }

        buildIndex(createTable.getTable().getName(), "dummy", sorted);
    }

    private static boolean isPrimaryKey(CreateTable createTable, String columnName) {

        List<net.sf.jsqlparser.statement.create.table.Index> indexList = createTable.getIndexes();

        for (String indexColumns : indexList.get(0).getColumnsNames()) {
            if (indexColumns.equals(columnName))
                return true;
        }

        return false;

    }

    private static void buildIndex(String table, String column, boolean sorted) {

        String[] indexList = indexMap.get(table).split("\\|");

        String line1;
        try {
            br = new LineNumberReader(new FileReader(TABLE_DIRECTORY + table + CommonLib.extension));

            // starts for lineitem

            if (table.equals("LINEITEM")) {

                while ((line1 = br.readLine()) != null) {

                    if ((br.getLineNumber() + 1) % blockSize == 0) {

                        boolean sortedd = true;
                        for (int i = 0; i < indexList.length; i++) {
                            System.out.println("building Index for " + table + " " + indexList[i]);
                            if (!sortedd)
                                sortList(table, indexList[i]);
                            saveData(table, indexList[i]);
                            sortedd = false;
                        }

                        fullFile.clear();
                    }

                    fullFile.add(line1);
                }

                // Process last set of data in fullFile List.
                if (fullFile.size() != 0) {
                    boolean sortedd = true;
                    for (int i = 0; i < indexList.length; i++) {
                        System.out.println("building Index for " + table + " " + indexList[i]);
                        if (!sortedd)
                            sortList(table, indexList[i]);
                        saveData(table, indexList[i]);
                        sortedd = false;
                    }

                    fullFile.clear();
                }

                // index creation
                for (int i = 0; i < indexList.length; i++) {
                    makeIndexOfLineItem(table, indexList[i]);
                }

                return;
            }


            // ends here


            while ((line1 = br.readLine()) != null) {

                if ((br.getLineNumber() + 1) % blockSize == 0) { // TODO: only lineitem table will get in this condition

                    for (int i = 0; i < indexList.length; i++) {
                        //sortList(table, indexList[i]);
                        makeIndex(table, indexList[i]);
                    }

                    System.out.println("LIST SIZE " + fullFile.size());
                    fullFile.clear();
                }

                fullFile.add(line1);
            }


            boolean sortedd = true;
            for (int i = 0; i < indexList.length; i++) {
                System.out.println("building Index for " + table + " " + indexList[i]);
                if (!sortedd)
                    sortList(table, indexList[i]);
                makeIndex(table, indexList[i]);
                sortedd = false;
            }

            fullFile.clear();

            System.out.println("asdf");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void makeIndexOfLineItem(String table, String column) {

        String line;
        int cnt = 0;
        fullFile.clear();
        //    /Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/TEMP_LINEITEM_RETURNFLAG_100007
        // Open bufferedReaders for all sorted files

        List<String> fileList = tempFileMap.get(table + "_" + column);

        pqSortIndex.clear();
        pqSortIndex.add(getIndexOfColumn(table, column));

        try {

            for (String files : fileList) {
                //String file = files.substring(files.lastIndexOf("/") + 1);
                BufferedReader br = new BufferedReader(new FileReader(files));

                List<BufferedReader> newList;

                if (bufferedReaderMap.containsKey(table + "_" + column))
                    newList = bufferedReaderMap.get(table + "_" + column);
                else
                    newList = new ArrayList<BufferedReader>();

                newList.add(br);

                bufferedReaderMap.put(table + "_" + column, newList);

                // fill priority queue for first time

                HashMap<BufferedReader, String> map = new HashMap<BufferedReader, String>();
                map.put(br, br.readLine());
                priorityQueue.add(map);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        // make indexes

       // String indexFileName = TABLE_DIRECTORY +  "INDEX_" + table + "_" + column + "_" + commonLib.getFileSequenceNumber();

        try {
            while ((line = getDataFromFiles(table, column)) != null) {

                if ((cnt + 1) % indexBlockSize == 0) {

//                    writeDataDisk(fullFile, table, column, indexFileName);
//
                    cnt = 0;
                    makeIndex(table, column);
                    fullFile.clear();
                }

                cnt++;

                fullFile.add(line); // ToDo : can we use fullfile here? Used FULLFILE list coz makeIndex uses this.
            }

            if (cnt != 0) {

               // writeDataDisk(fullFile, table, column, indexFileName);
                makeIndex(table, column);

                cnt = 0;
                fullFile.clear();
            }

            //indexFileLists.add(indexFileName);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static String getDataFromFiles(String table, String column) {
        try {

            Map<BufferedReader, String> map = priorityQueue.poll();


            if (map == null && priorityQueue.size() > 0) {
                while (map == null && priorityQueue.size() > 0)
                    map = priorityQueue.poll();
            }

            if (map == null) {

                for (BufferedReader br : bufferedReaderMap.get(table + "_" + column)) {
                    br.close();
                    br = null;
                }

                return null;
            }


            for (Map.Entry<BufferedReader, String> entry : map.entrySet()) {
                String line;
                if ((line = entry.getKey().readLine()) != null) {
                    HashMap<BufferedReader, String> newMap = new HashMap<BufferedReader, String>();
                    newMap.put(entry.getKey(), line);
                    priorityQueue.add(newMap);
                }

                return entry.getValue();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private static void saveData(String table, String column) {

        String fileName = TABLE_DIRECTORY + "TEMP_" + table + "_" + column + "_" + commonLib.getFileSequenceNumber();

        String key = table + "_" + column;
        List<String> value;
        if (tempFileMap.containsKey(key))
            value = tempFileMap.get(key);
        else
            value = new ArrayList<String>();

        value.add(fileName);

        tempFileMap.put(table + "_" + column, value);

        try {
            writeDataDisk(fullFile, table, column, fileName);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void makeIndex(String table, String column) {

        String tuple[];

        int indexOfColumn = getIndexOfColumn(table, column);

        try {

            List<String> data = new ArrayList<String>(); // TODO Might be an overhead
            List<String> indexDataList = new ArrayList<String>();

            int linenumber = 0;
            String startKeyValue = "";
            String endKeyValue = "";
            String prev = "";

            for (String line : fullFile) {

                if (linenumber == 0) {
                    tuple = line.split("\\|");
                    startKeyValue = tuple[indexOfColumn];
                }

                if (linenumber == indexBlockSize) {
                    tuple = line.split("\\|");
                    endKeyValue = tuple[indexOfColumn];
                }

                if (linenumber == indexBlockSize) {
                    String splitFileName = TABLE_DIRECTORY + table + "_" + column + "_" + commonLib.getFileSequenceNumber();
                    writeDataDisk(data, table, column, splitFileName);
                    linenumber = 0;
                    data.clear();

                    indexDataList.add(startKeyValue + "|" + endKeyValue + "|" + splitFileName);
                    continue;
                }

                data.add(line);
                linenumber++;

                prev = line;
            }

            if (linenumber != 0) {

                tuple = prev.split("\\|");
                endKeyValue = tuple[indexOfColumn];

                String splitFileName = TABLE_DIRECTORY + table + "_" + column + "_" + commonLib.getFileSequenceNumber();
                writeDataDisk(data, table, column, splitFileName);
                data.clear();

                indexDataList.add(startKeyValue + "|" + endKeyValue + "|" + splitFileName);
            }

            String indexFileName = TABLE_DIRECTORY + "INDEX_" + table + "_" + column + "_" + commonLib.getFileSequenceNumber();
            indexFileLists.add(indexFileName);

            writeDataDisk(indexDataList, table, column, indexFileName);

            data.clear();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static List<Integer> getIndexOfSortKey(String table, int indexOfColumn) {
        List<Integer> list = new ArrayList<Integer>();

        List<ColumnDefinition> columnDefinitionList = schemas.get(table).getColumnDefinitions();


        return list;
    }

    private static String getLine(boolean sorted, String table) {

        String value = null;

        if (!sorted) {
            try {
                value = sort.getTuple();
                if (value == null)
                    return null;

            } catch (Exception e) {
                e.printStackTrace();
            }
            String values[] = value.split("\\|");
            String ret = "";
            for (int i = 0; i < values.length; i++)
                ret = ret + "|" + values[i];

            return ret.substring(1);
        } else {

            try {
                if (br == null)
                    br = new LineNumberReader(new FileReader(TABLE_DIRECTORY + table + CommonLib.extension));

                return br.readLine();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "";
    }

    private static List<Column> getColumnList(CreateTable createTable, String col) {
        List<Column> columnList = new ArrayList<Column>();

//        for (ColumnDefinition columnDefinition : createTable.getColumnDefinitions()) {
//            Column column = new Column();
//            column.setColumnName(columnDefinition.getColumnName());
//            column.setTable(new Table(createTable.getTable().getName()));
//            columnList.add(column);
//        }

        Column column = new Column();
        column.setColumnName(col);
        column.setTable(new Table(createTable.getTable().getName()));
        columnList.add(column);
        return columnList;

    }

    public static void writeDataDisk(List<String> data, String table, String column, String filename) throws Exception {

        File file = new File(filename);
        file.createNewFile();

        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));

        for (String row : data) {

            bufferedWriter.write(row + "\n");
        }

        bufferedWriter.close();
        bufferedWriter = null;

    }

    private static int getIndexOfColumn(String table, String column) {
        int index = 0;
        CreateTable createTable = schemas.get(table.toUpperCase());

        for (ColumnDefinition columnDefinition : createTable.getColumnDefinitions()) {
            if (columnDefinition.getColumnName().equals(column))
                return index;
            index++;
        }

        return index;
    }

    private static void init() {
//        indexMap.put("LINEITEM", "ORDERKEY|LINENUMBER|RETURNFLAG|RECEIPTDATE|SHIPDATE");
        indexMap.put("LINEITEM", "ORDERKEY|SHIPDATE");
        indexMap.put("ORDERS", "ORDERKEY|ORDERDATE");
        indexMap.put("PART", "PARTKEY");
        indexMap.put("CUSTOMER", "CUSTKEY|MKTSEGMENT");
        indexMap.put("SUPPLIER", "SUPPKEY|NATIONKEY");
        indexMap.put("PARTSUPP", "PARTKEY|SUPPKEY");
        indexMap.put("NATION", "NATIONKEY");
        indexMap.put("REGION", "REGIONKEY");
    }

    public static boolean isNumber(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static void sortList(String table, String column) {

        List<ColumnDefinition> columnDefinitionList = schemas.get(table).getColumnDefinitions();
        final List<Integer> indexOfSortKey = new ArrayList<Integer>();

        int i = 0;
        for (ColumnDefinition columnDefinition : columnDefinitionList) {
            if (columnDefinition.getColumnName().equals(column)) {
                indexOfSortKey.add(i);
                break;
            }
            i++;
        }


        Collections.sort(fullFile, new Comparator<String>() {
            @Override
            public int compare(String a1, String b1) {

                String a[] = a1.split("\\|");
                String b[] = b1.split("\\|");

                for (int i = 0; i < indexOfSortKey.size(); i++) {
                    int index = indexOfSortKey.get(i);

                    try {

                        if (isNumber(a[index])) {

                            double pv1 = Double.parseDouble(a[index]);
                            double pv2 = Double.parseDouble(b[index]);

                            if (pv1 < pv2)
                                return -1;
                            else if (pv1 > pv2)
                                return 1;
                            else {
                                continue;
                            }

                        } else {

                            return a[index].compareTo(b[index]);
                        }

                    } catch (Exception e) {

                    }
                }
                return -1;
            }
        });
    }

    public List<String> getIndexList(String tableName, String indexColumnName) {

//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_SHIPDATE_100012");
//        indexFileLists.add("INDEX_LINEITEM_RECEIPTDATE_100051");
//        indexFileLists.add("INDEX_LINEITEM_RETURNFLAG_100038");
//        indexFileLists.add("INDEX_LINEITEM_LINENUMBER_100025");
//        indexFileLists.add("INDEX_LINEITEM_ORDERKEY_100012");

       /* indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_ORDERKEY_100049");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_ORDERKEY_100051");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_ORDERKEY_100053");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_ORDERKEY_100055");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_ORDERKEY_100057");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_ORDERKEY_100059");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_ORDERKEY_100061");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_ORDERKEY_100063");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_ORDERKEY_100065");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_ORDERKEY_100067");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_ORDERKEY_100069");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_ORDERKEY_100071");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_SHIPDATE_100073");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_SHIPDATE_100075");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_SHIPDATE_100077");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_SHIPDATE_100079");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_SHIPDATE_100081");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_SHIPDATE_100083");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_SHIPDATE_100085");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_SHIPDATE_100087");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_SHIPDATE_100089");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_SHIPDATE_100091");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_SHIPDATE_100093");
        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/INDEX_LINEITEM_SHIPDATE_100095");*/

        String col[] = null;
        String indexFileName = "";
        List<String> list = new ArrayList<String>();
        List<String> filelist = new ArrayList<String>();

        if (indexColumnName.contains(".")) {
            col = indexColumnName.split("\\.");
            indexFileName = "INDEX_" + tableName + "_" + col[1];
        } else {
            indexFileName = "INDEX_" + tableName + "_" + indexColumnName;
        }

        for (String filename : indexFileLists) {
            String file = filename.substring(filename.lastIndexOf("/") + 1, filename.lastIndexOf("_"));
            if (indexFileName.equals(file))
                list.add(filename);
        }

        BufferedReader br = null;
        String line = null;
        try {
            for(int i =0; i < list.size(); i++) {
                br = new BufferedReader(new FileReader(list.get(i)));

                while ((line = br.readLine()) != null)
                    filelist.add(line);

                br.close();
                br = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        //list.remove(0);

        //indexFileLists.clear();
        return filelist;
    }

}
