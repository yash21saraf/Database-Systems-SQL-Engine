package helpers;

import iterators.RAIterator;
import iterators.TableIterator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;

import java.io.*;
import java.util.*;

import static builders.IteratorBuilder.schemas;
import static helpers.CommonLib.TABLE_DIRECTORY;

public class Index {

    public static List<String> indexFileLists = new ArrayList<String>();
    static IndexSort sort = null;
    public static Map<String, String> indexMap = new HashMap<String, String>();
    private static CommonLib commonLib = CommonLib.getInstance();
    private static int indexBlockSize = 0;
    private static LineNumberReader br = null;
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

        boolean sorted = true;

        for (int i = 0; i < indexes.length; i++) {
            buildIndex(createTable.getTable().getName(), indexes[i], sorted);
            sorted = false;
            //buildIndex(createTable.getTable().getName(), indexes[i], isPrimaryKey(createTable, indexes[i]));
        }
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

        System.out.println("building Index for " + table  + " " + column) ;
        String tuple[];

        int indexOfColumn = getIndexOfColumn(table, column);

        if (!sorted) {
            try {
                RAIterator child = new TableIterator(table, table, schemas.get(table.toUpperCase()).getColumnDefinitions().toArray(new ColumnDefinition[schemas.get(table.toUpperCase()).getColumnDefinitions().size()]));

                sort = new IndexSort(child, getColumnList(schemas.get(table), column), null, null, true);
                sort.sort();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            String line;

            List<String> data = new ArrayList<String>();
            List<String> indexDataList = new ArrayList<String>();

            int linenumber = 0;
            String startKeyValue = "";
            String endKeyValue = "";
            String prev = "";

            while ((line = getLine(sorted, table)) != null) {

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


            sort = null;
            if(br != null)
                br.close();
            br = null;

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
                if(value == null)
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

        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));

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
        indexMap.put("LINEITEM", "ORDERKEY|LINENUMBER|RETURNFLAG|RECEIPTDATE|SHIPDATE");
        indexMap.put("ORDERS", "ORDERKEY|ORDERDATE");
        indexMap.put("PART", "PARTKEY");
        indexMap.put("CUSTOMER", "CUSTKEY|MKTSEGMENT");
        indexMap.put("SUPPLIER", "SUPPKEY|NATIONKEY");
        indexMap.put("PARTSUPP", "PARTKEY|SUPPKEY");
        indexMap.put("NATION", "NATIONKEY");
        indexMap.put("REGION", "REGIONKEY");
    }

    private boolean hasColumnName(List<Column> columnList, Schema schema) {

        for (Column column : columnList) {
            if (column.getColumnName().equals(schema.getColumnDefinition().getColumnName()))
                return true;
        }
        return false;
    }

    private void setTableName(List<Column> columnList, String columnName, String tableName) {
        for (int i = 0; i < columnList.size(); i++) {
            if (columnList.get(i).getColumnName().equals(columnName)) {
                columnList.get(i).setTable(new Table(null, tableName));
                return;
            }
        }
    }

    public List<String> getIndexList(String tableName, String indexColumnName) {

        indexFileLists.add("INDEX_LINEITEM_SHIPDATE_100064");
        indexFileLists.add("INDEX_LINEITEM_RECEIPTDATE_100051");
        indexFileLists.add("INDEX_LINEITEM_RETURNFLAG_100038");
        indexFileLists.add("INDEX_LINEITEM_LINENUMBER_100025");
        indexFileLists.add("INDEX_LINEITEM_ORDERKEY_100012");

        String col[] = null;
        String indexFileName = "";
        List<String> list = new ArrayList<String>();

        if(indexColumnName.contains(".")){
            col = indexColumnName.split("\\.");
            indexFileName = "INDEX_" + tableName + "_" + col[1];
        } else {
            indexFileName = "INDEX_" + tableName + "_" + indexColumnName;
        }

        for(String filename : indexFileLists){
            String file = filename.substring(0, filename. lastIndexOf("_"));
            if(indexFileName.equals(file))
                list.add(filename);
        }
        BufferedReader br = null;
        String line = null;
        try {
            br = new BufferedReader(new FileReader(CommonLib.TABLE_DIRECTORY + list.get(0)));

            while((line = br.readLine()) != null)
                list.add(line);

            br.close();
            br = null;
        } catch (Exception e){
            e.printStackTrace();
        }

        list.remove(0);

        indexFileLists.clear();
        return list;
    }

    private void getRowSize(String table, String column) {

        CreateTable createTable = schemas.get(table.toUpperCase());

        int size = 0;

        for (ColumnDefinition columnDefinition : createTable.getColumnDefinitions()) {
            if (columnDefinition.getColDataType().getDataType().equals("INT")) {
                size += 8;
            } else if (columnDefinition.getColDataType().getDataType().equals("DECIMAL")) {
                size += 8;
            } else if (columnDefinition.getColDataType().getDataType().equals("CHAR")) {
                size += Integer.parseInt(columnDefinition.getColDataType().getArgumentsStringList().get(0));
            } else if (columnDefinition.getColDataType().getDataType().equals("VARCHAR")) {
                size += Integer.parseInt(columnDefinition.getColDataType().getArgumentsStringList().get(0));
            } else if (columnDefinition.getColDataType().getDataType().equals("DATE")) {
                size += 8;
            }
        }
    }

    //private PlainSelect rebuildSelect(PlainSelect plainSelect) {
    private Select rebuildSelect(Select select) {

        //Select  = select;
        SelectBody selectBody = select.getSelectBody();
        PlainSelect plainSelect;

        if ((plainSelect = (PlainSelect) CommonLib.castAs(selectBody, PlainSelect.class)) != null) {
            System.out.println(plainSelect);
        }

        List<String> list = new ArrayList<String>();

        PlainSelect ps = new PlainSelect();

        FromItem fromItem = plainSelect.getFromItem();
        if (fromItem != null)
            list.add(fromItem.toString());

        if (plainSelect.getJoins() != null) {
            if (!plainSelect.getJoins().isEmpty()) {
                for (Join join : plainSelect.getJoins())
                    list.add(join.toString());

            } else
                return select;
        } else
            return select;

        Collections.sort(list, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {

                if (hashMap.containsKey(o1) && hashMap.containsKey(o2)) {
                    if (hashMap.get(o1) < hashMap.get(o2))
                        return -1;
                    if (hashMap.get(o1) > hashMap.get(o2))
                        return 1;
                    return 0;
                } else
                    return -1;
            }
        });
        List<Join> joinList = new ArrayList<Join>();

        for (int i = 0; i < list.size(); i++) {
            if (i == 0)
                plainSelect.setFromItem(new Table(list.get(i)));
            else {
                Join join = new Join();
                Table table = new Table(list.get(i));
                join.setRightItem(table);
                join.setSimple(true);
                joinList.add(join);
            }
        }

        plainSelect.setJoins(joinList);

        SelectBody selectBody1 = (SelectBody) plainSelect;

        //Select select1 = (Select) selectBody1;
        select.setSelectBody(selectBody1);
        System.out.println(plainSelect);

        /*System.out.println(plainSelectbackup);
        System.out.println("    ");
        System.out.println(plainSelect);*/
        list.clear();
        hashMap.clear();

        return select;
    }

}
