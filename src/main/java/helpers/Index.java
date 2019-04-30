package helpers;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.io.*;
import java.util.*;

import static builders.IteratorBuilder.schemas;


public class Index {


    final static Map<String, List<BufferedReader>> bufferedReaderMap = new HashMap<String, List<BufferedReader>>();
    final static Map<String, List<String>> tempFileMap = new HashMap<String, List<String>>();


    public static List<String> indexFileLists = new ArrayList<String>();
    public static Map<String, String> indexMap = new HashMap<String, String>();
    static IndexSort sort = null;
    static List<String> fullFile = new ArrayList<String>();
    static String path = System.getProperty("user.dir");
    static HashMap<String, TreeMap<String, List<String>>> tableColTreeMap = new HashMap<String, TreeMap<String, List<String>>>();
    static Map<String, Integer> indexOfIndexKey = new HashMap<String, Integer>();
    static TreeMap<String, List<String>> LINEITEM_ORDERKEY_TREEMAP = new TreeMap<String, List<String>>();
    static TreeMap<String, List<String>> LINEITEM_LINENUMBER_TREEMAP = new TreeMap<String, List<String>>();
    static TreeMap<String, List<String>> LINEITEM_RETURNFLAG_TREEMAP = new TreeMap<String, List<String>>();
    static TreeMap<String, List<String>> LINEITEM_RECEIPTDATE_TREEMAP = new TreeMap<String, List<String>>();

    //endregion
    static TreeMap<String, List<String>> LINEITEM_SHIPDATE_TREEMAP = new TreeMap<String, List<String>>();
    static TreeMap<String, List<String>> PART_PARTKEY_TREEMAP = new TreeMap<String, List<String>>();
    static TreeMap<String, List<String>> CUSTOMER_CUSTKEY_TREEMAP = new TreeMap<String, List<String>>();
    static TreeMap<String, List<String>> CUSTOMER_MKTSEGMENT_TREEMAP = new TreeMap<String, List<String>>();
    static TreeMap<String, List<String>> SUPPLIER_SUPPKEY_TREEMAP = new TreeMap<String, List<String>>();
    static TreeMap<String, List<String>> SUPPLIER_NATIONKEY_TREEMAP = new TreeMap<String, List<String>>();
    static TreeMap<String, List<String>> PARTSUPP_PARTKEY_TREEMAP = new TreeMap<String, List<String>>();
    static TreeMap<String, List<String>> PARTSUPP_SUPPKEY_TREEMAP = new TreeMap<String, List<String>>();
    static TreeMap<String, List<String>> ORDERS_ORDERKEY_TREEMAP = new TreeMap<String, List<String>>();
    static TreeMap<String, List<String>> ORDERS_ORDERDATE_TREEMAP = new TreeMap<String, List<String>>();
    static TreeMap<String, List<String>> NATION_NATIONKEY_TREEMAP = new TreeMap<String, List<String>>();
    static TreeMap<String, List<String>> REGION_REGIONKEY_TREEMAP = new TreeMap<String, List<String>>();

    static Map<String, TreeMap> colIndexMap = new HashMap<String, TreeMap>();
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

    // endregion

    static private boolean oncePK = true;
    String fileName = "/createTableTeam3.sql";
    HashMap<String, Integer> hashMap = new HashMap();
    private List<String> indexList = new ArrayList<String>();

        final static String TABLE_DIRECTORY = CommonLib.TABLE_DIRECTORY;
//    final static String TABLE_DIRECTORY = path + "/";
    public Index() {
        init();
    }

    public static void setIndexFileLists(List<String> indexFileLists) {
        Index.indexFileLists = indexFileLists;
    }

    public static void createIndex(CreateTable createTable, boolean build) {

        // region variables
        indexMap.put("LINEITEM", "ORDERKEY|LINENUMBER|RETURNFLAG|RECEIPTDATE|SHIPDATE");
//        indexMap.put("LINEITEM", "SHIPDATE");
        indexMap.put("ORDERS", "ORDERKEY|ORDERDATE");
        indexMap.put("PART", "PARTKEY");
        indexMap.put("CUSTOMER", "CUSTKEY");
        indexMap.put("SUPPLIER", "SUPPKEY|NATIONKEY");
        indexMap.put("PARTSUPP", "PARTKEY|SUPPKEY");
        indexMap.put("NATION", "NATIONKEY");
        indexMap.put("REGION", "REGIONKEY");

        if (createTable.getTable().getName().equals("LINEITEM"))
            indexBlockSize = 50000;
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

        String tableName = createTable.getTable().getName();

        if (tableName.equals("LINEITEM")) {

            colIndexMap.put("LINEITEM_ORDERKEY", LINEITEM_ORDERKEY_TREEMAP);
            colIndexMap.put("LINEITEM_LINENUMBER", LINEITEM_LINENUMBER_TREEMAP);
            colIndexMap.put("LINEITEM_RETURNFLAG", LINEITEM_RETURNFLAG_TREEMAP);
            colIndexMap.put("LINEITEM_RECEIPTDATE", LINEITEM_RECEIPTDATE_TREEMAP);
            colIndexMap.put("LINEITEM_SHIPDATE", LINEITEM_SHIPDATE_TREEMAP);

        } else if (tableName.equals("PART")) {

            colIndexMap.put("PART_PARTKEY", PART_PARTKEY_TREEMAP);

        } else if (tableName.equals("ORDERS")) {

            colIndexMap.put("ORDERS_ORDERKEY", ORDERS_ORDERKEY_TREEMAP);
            colIndexMap.put("ORDERS_ORDERDATE", ORDERS_ORDERDATE_TREEMAP);

        } else if (tableName.equals("CUSTOMER")) {

            colIndexMap.put("CUSTOMER_CUSTKEY", CUSTOMER_CUSTKEY_TREEMAP);
            colIndexMap.put("CUSTOMER_MKTSEGMENT", CUSTOMER_MKTSEGMENT_TREEMAP);

        } else if (tableName.equals("SUPPLIER")) {

            colIndexMap.put("SUPPLIER_SUPPKEY", SUPPLIER_SUPPKEY_TREEMAP);
            colIndexMap.put("SUPPLIER_NATIONKEY", SUPPLIER_NATIONKEY_TREEMAP);

        } else if (tableName.equals("PARTSUPP")) {

            colIndexMap.put("PARTSUPP_PARTKEY", PARTSUPP_PARTKEY_TREEMAP);
            colIndexMap.put("PARTSUPP_SUPPKEY", PARTSUPP_SUPPKEY_TREEMAP);

        } else if (tableName.equals("NATION")) {

            colIndexMap.put("NATION_NATIONKEY", NATION_NATIONKEY_TREEMAP);

        } else if (tableName.equals("REGION")) {

            colIndexMap.put("REGION_REGIONKEY", REGION_REGIONKEY_TREEMAP);

        }

        indexFileLists.clear();
        //indexMap.clear();
        indexOfIndexKey.clear();
        tableColTreeMap.clear();

        // endregion variables

        String indexes[] = indexMap.get(createTable.getTable().getName()).split("\\|");


        List<String> l = Arrays.asList(indexes);


        /**
         * Save index of index-column.
         */


        int i = 0;
        for (ColumnDefinition columnDefinition : createTable.getColumnDefinitions()) {

            if (l.contains(columnDefinition.getColumnName())) {
                indexOfIndexKey.put(columnDefinition.getColumnName(), i);
            }

            i++;
        }

        /**
         * Create list of TreeMap, one TreeMap for each indexes.
         */


        if(build) {
            long start = 0;
            long end = 0;
            long res = 0;
            for (String index : indexes) {
                start = System.currentTimeMillis();
                buildIndex(createTable.getTable().getName(), index, true);
                end = System.currentTimeMillis();
                res = end - start;
                //System.out.println("Building indexes for " + index + " Time taken:" + res);
            }

            try {
                writeDataDisk(indexFileLists, "INDEX", "ALL", TABLE_DIRECTORY + createTable.getTable().getName() + "_INDEX_LIST");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static boolean isPrimaryKey(String table, String columnName) {

        if (table.equals("LINEITEM") && columnName.equals("ORDERKEY"))
            return true;
        if (table.equals("PART") && columnName.equals("PARTKEY"))
            return true;
        if (table.equals("CUSTOMER") && columnName.equals("CUSTKEY"))
            return true;
        if (table.equals("SUPPLIER") && columnName.equals("SUPPKEY"))
            return true;
        if (table.equals("NATION") && columnName.equals("NATIONKEY"))
            return true;
        if (table.equals("REGION") && columnName.equals("REGIONKEY"))
            return true;
        if (table.equals("ORDERS") && columnName.equals("ORDERKEY"))
            return true;
        if (table.equals("PARTSUPP") && columnName.equals("PARTKEY"))
            return true;

        return false;

    }

    private static void buildIndex(String table, String column, boolean sorted) {

        //String[] indexList = indexMap.get(table).split("\\|");

        long position = 0;
        String line1;
        try {
            br = new LineNumberReader(new FileReader(CommonLib.TABLE_DIRECTORY + table + CommonLib.extension));

            //Integer pkIndex = getPrimarykeyIndex(table);
            List<String> pkFileNameList = new ArrayList<String>();
            List<String> indexDataListpk = new ArrayList<String>();

            boolean istrue = true;
            while ((line1 = br.readLine()) != null) {

                //if (br.getLineNumber() == 6001214)
                //System.out.println(line1);
                String tuple[] = line1.split("\\|");

                List<String> list;
                if (colIndexMap.get(table + "_" + column).containsKey(tuple[indexOfIndexKey.get(column)])) {
                    list = (List<String>) colIndexMap.get(table + "_" + column).get(tuple[indexOfIndexKey.get(column)]);
                } else {
                    list = new ArrayList<String>();
                }

                list.add(Long.toString(position));
                colIndexMap.get(table + "_" + column).put(tuple[indexOfIndexKey.get(column)], list);

                position += line1.length() + 1;


                line1 = null;

                if ((br.getLineNumber()) % 500000 == 0) {
                    //System.out.println(br.getLineNumber());
                    br = null;
                    br = new LineNumberReader(new FileReader(CommonLib.TABLE_DIRECTORY + table + CommonLib.extension));
                    br.skip(position);
                }
            }

            /**
             * writing index
             */

            List<String> indexFiles = new ArrayList<String>();

            for (Map.Entry<String, TreeMap> entry : colIndexMap.entrySet()) {
                if (entry.getValue().size() != 0) {

                    String filename = entry.getKey();


                    indexFiles.add(filename);
                    TreeMap<String, List<String>> treeMap = entry.getValue();

                    try {
                        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(TABLE_DIRECTORY + filename));

                        for (Map.Entry<String, List<String>> treeMapEntry : treeMap.entrySet()) {
                            String key = treeMapEntry.getKey();
                            for (String val : treeMapEntry.getValue()) {
                                bufferedWriter.write(key + "|" + val + "\n");
                            }
                        }

                        bufferedWriter.close();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            LINEITEM_LINENUMBER_TREEMAP.clear();
            LINEITEM_RETURNFLAG_TREEMAP.clear();
            LINEITEM_RECEIPTDATE_TREEMAP.clear();
            LINEITEM_SHIPDATE_TREEMAP.clear();
            LINEITEM_ORDERKEY_TREEMAP.clear();
            PART_PARTKEY_TREEMAP.clear();
            SUPPLIER_SUPPKEY_TREEMAP.clear();
            SUPPLIER_NATIONKEY_TREEMAP.clear();
            NATION_NATIONKEY_TREEMAP.clear();
            REGION_REGIONKEY_TREEMAP.clear();
            CUSTOMER_CUSTKEY_TREEMAP.clear();
            CUSTOMER_MKTSEGMENT_TREEMAP.clear();
            PARTSUPP_PARTKEY_TREEMAP.clear();
            PARTSUPP_SUPPKEY_TREEMAP.clear();

            //colIndexMap.clear();

            oncePK = true;

            /**
             * sorting written index files
             */

            for (String file : indexFiles) {
                LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(TABLE_DIRECTORY + file));
                List<String> data = new ArrayList<String>();
                String line;
                while ((line = lineNumberReader.readLine()) != null) {
                    data.add(line);
                }

                lineNumberReader.close();
                lineNumberReader = null;

                long start = System.currentTimeMillis();

                Collections.sort(data, new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {

                        String a[] = o1.split("\\|");
                        String b[] = o2.split("\\|");

                        if (isNumber(a[0])) {

                            double pv1 = Double.parseDouble(a[0]);
                            double pv2 = Double.parseDouble(b[0]);

                            if (pv1 < pv2)
                                return -1;
                            else if (pv1 > pv2)
                                return 1;

                        } else {

                            return a[0].compareTo(b[0]);
                        }

                        return 0;
                    }
                });
                // }

                long end = System.currentTimeMillis();
                long t = (end - start);
                //System.out.println("Sorting took " + t + " " + table + " ");


                /**
                 * Writing sorted index files
                 */

                start = System.currentTimeMillis();

                try {
                    //BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(CommonLib.TABLE_DIRECTORY + "INDEX_" + file + "_" + commonLib.getFileSequenceNumber()));

                    List<String> temp = new ArrayList<String>(); // TODO Might be an overhead
                    List<String> indexDataList = new ArrayList<String>();

                    int linenumber = 0;
                    String startKeyValue = "";
                    String endKeyValue = "";
                    String prev = "";

                    String[] tuple;
                    for (String val : data) {

                        if (temp.size() == 0) {
                            startKeyValue = val.substring(0, val.indexOf("|"));
                        }

                        if (temp.size() == indexBlockSize) {
                            endKeyValue = prev.substring(0, prev.indexOf("|"));

                            String splitFileName = TABLE_DIRECTORY + file + "_" + commonLib.getFileSequenceNumber();
                            writeDataDisk(temp, table, column, splitFileName);
                            //linenumber = 0;
                            temp.clear();

                            indexDataList.add(startKeyValue + "|" + endKeyValue + "|" + splitFileName);

                            startKeyValue = prev.substring(0, prev.indexOf("|"));
                        }

                        temp.add(val);
                        //linenumber++;

                        prev = val;
                    }

                    if (temp.size() != 0) {

                        //tuple = prev.split("\\|");
                        endKeyValue = prev.substring(0, prev.indexOf("|"));

                        String splitFileName = TABLE_DIRECTORY + file + "_" + commonLib.getFileSequenceNumber();
                        writeDataDisk(temp, table, column, splitFileName);
                        temp.clear();

                        indexDataList.add(startKeyValue + "|" + endKeyValue + "|" + splitFileName);
                    }

                    /**
                     * Writing list of index files.
                     */

                    String indexFileName = TABLE_DIRECTORY + "INDEX_" + file + "_" + commonLib.getFileSequenceNumber();
                    indexFileLists.add(indexFileName);

                    writeDataDisk(indexDataList, table, column, indexFileName);

                    data.clear();


                } catch (Exception e) {
                    e.printStackTrace();
                }

                end = System.currentTimeMillis();
                t = (end - start);
                //System.out.println("Writing took " + t);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static TreeMap<String, List<String>> getPkMap(String table) { // TODO : DEFINE OTHER MAPS

        if (table.equals("LINEITEM"))
            return LINEITEM_ORDERKEY_TREEMAP;
        if (table.equals("ORDERS"))
            return ORDERS_ORDERKEY_TREEMAP;
        if (table.equals("CUSTOMER"))
            return CUSTOMER_CUSTKEY_TREEMAP;
        if (table.equals("PART"))
            return PART_PARTKEY_TREEMAP;
        if (table.equals("SUPPLIER"))
            return SUPPLIER_SUPPKEY_TREEMAP;
        if (table.equals("PARTSUPP"))
            return PARTSUPP_PARTKEY_TREEMAP;
        if (table.equals("NATION"))
            return NATION_NATIONKEY_TREEMAP;
        if (table.equals("REGION"))
            return REGION_REGIONKEY_TREEMAP;

        return null;
    }

    public static String getPK(String table) {

        if (table.equals("LINEITEM"))
            return "ORDERKEY";
        if (table.equals("PART"))
            return "PARTKEY";
        if (table.equals("CUSTOMER"))
            return "CUSTKEY";
        if (table.equals("SUPPLIER"))
            return "SUPPKEY";
        if (table.equals("NATION"))
            return "NATIONKEY";
        if (table.equals("PARTSUPP"))
            return "PARTKEY";
        if (table.equals("ORDERS"))
            return "ORDERKEY";
        if (table.equals("REGION"))
            return "REGIONKEY";

        return "";
    }


    private static Integer getPrimarykeyIndex(String table) {

        List<net.sf.jsqlparser.statement.create.table.Index> indexList = schemas.get(table).getIndexes();

        int i = 0;
        for (String indexColumns : indexList.get(0).getColumnsNames()) {
            for (ColumnDefinition columnDefinition : schemas.get(table).getColumnDefinitions()) {
                if (indexColumns.equals(columnDefinition.getColumnName()))
                    return i;
                i++;
            }
        }
        return i;
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

    private static void init() {

//        indexMap.put("LINEITEM", "SHIPDATE");
        indexMap.put("LINEITEM", "ORDERKEY|LINENUMBER|RETURNFLAG|RECEIPTDATE|SHIPDATE");
        indexMap.put("ORDERS", "ORDERKEY|ORDERDATE");
        indexMap.put("PART", "PARTKEY");
        indexMap.put("CUSTOMER", "CUSTKEY");
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

    public List<String> getIndexList() {
        return indexList;
    }

    public List<String> getIndexList(String tableName, String indexColumnName) {

//
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_CUSTOMER_CUSTKEY_100002");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_SUPPLIER_SUPPKEY_100003");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_SUPPLIER_NATIONKEY_100004");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_PARTSUPP_PARTKEY_100020");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_PARTSUPP_SUPPKEY_100036");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_NATION_NATIONKEY_100037");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_REGION_REGIONKEY_100038");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_LINEITEM_ORDERKEY_100159");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_LINEITEM_LINENUMBER_100280");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_LINEITEM_RETURNFLAG_100401");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_LINEITEM_RECEIPTDATE_100522");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_LINEITEM_SHIPDATE_100643");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_ORDERS_ORDERKEY_100673");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_ORDERS_ORDERKEY_100703");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_ORDERS_ORDERDATE_100733");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_PART_PARTKEY_100737");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_ORDERS_ORDERKEY_100767");
//        indexFileLists.add("/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/INDEX_ORDERS_ORDERDATE_100797");

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


        try {
            if (indexFileLists.size() == 0) {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(TABLE_DIRECTORY + tableName + "_INDEX_LIST"));

                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    indexFileLists.add(line);
                }
//
//                indexFileLists.add("CUSTOMER_INDEX_LIST");
//                indexFileLists.add("SUPPLIER_INDEX_LIST");
//                indexFileLists.add("PARTSUPP_INDEX_LIST");
//                indexFileLists.add("NATION_INDEX_LIST");
//                indexFileLists.add("REGION_INDEX_LIST");
//                indexFileLists.add("LINEITEM_INDEX_LIST");
//                indexFileLists.add("ORDERS_INDEX_LIST");
//                indexFileLists.add("PART_INDEX_LIST");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        for (String filename : indexFileLists) {
            String file = filename.substring(filename.lastIndexOf("/") + 1, filename.lastIndexOf("_"));
            if (indexFileName.equals(file))
                list.add(filename);
        }

        BufferedReader br = null;
        String line = null;
        try {
            for (int i = 0; i < list.size(); i++) {
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

        // indexFileLists.clear();
        return filelist;
    }

}