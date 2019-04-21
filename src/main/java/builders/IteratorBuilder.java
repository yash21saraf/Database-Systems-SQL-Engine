package builders;

import helpers.CommonLib;
import helpers.Schema;
import iterators.*;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.*;

import java.io.*;
import java.util.*;


public class IteratorBuilder {

    //region Variables

    public static Map<String, Schema[]> iteratorSchemas = new HashMap();
    //    String path = TableIterator.TABLE_DIRECTORY;
//    String path = "tempfolder/";
    String path = System.getProperty("user.dir");
    String fileName = "/createTableTeam3.sql";

    //endregion

    //region Constructor
    FileReader fileReader;

    //endregion
    BufferedWriter writer = null;
    HashMap<String, Integer> hashMap = new HashMap();

    //region Parsing methods
    //private static final Logger logger = LogManager.getLogger();
    private CommonLib commonLib = CommonLib.getInstance();
    private Map<String, CreateTable> schemas = new HashMap();
    /*   private FileOutputStream fileOutputStream;
       private BufferedOutputStream bufferedOutputStream;
       private ObjectOutputStream objectOutputStream;

       private BufferedInputStream bufferedInputStream;
       private FileInputStream fileInputStream;
       private ObjectInputStream objectInputStream;*/
    private File file;

    public IteratorBuilder() {

        hashMap.put("REGION", 1);
        hashMap.put("NATION", 2);
        hashMap.put("SUPPLIER", 3);
        hashMap.put("CUSTOMER", 4);
        hashMap.put("PARTSUPP", 5);
        hashMap.put("ORDERS", 6);
        hashMap.put("LINEITEM", 7);

    }

    /**
     * Method to read in CREATE TABLE statement
     * Stores the associated List<ColumnDefinition> schema
     *
     * @param statement Statement interface object extracted from JSQLParser
     *                  Statement can be cast as CreateTable or Select
     * @return The final RAIterator object to execute SQL query in volcano-style
     */
    public RAIterator parseStatement(Statement statement) throws Exception {

        CreateTable createTable;
        Select select;

        if ((createTable = (CreateTable) CommonLib.castAs(statement, CreateTable.class)) != null) {
            buildCreateTable(createTable);
            saveCreateStatement(createTable);

            createIndex(createTable);

            return null;

        } else if ((select = (Select) CommonLib.castAs(statement, Select.class)) != null) {

            if (schemas == null || schemas.size() == 0)
                createSchema();

            //select = rebuildSelect(select);
            return buildSelect(select);

        }
        throw new Exception("Invalid statement");

    }

    private void createIndex(CreateTable createTable) {

        List<Index> indexList = createTable.getIndexes();

        for (Index index : indexList) {

            List<String> columnsNames = index.getColumnsNames();
            String table = createTable.getTable().getName();

            boolean sorted = true;
            for (String col : columnsNames) {
                buildIndex(table, col, sorted);
                sorted = false;
            }

        }
    }

    private void buildIndex(String table, String column, boolean sorted) {

        CreateTable createTable = schemas.get(table.toUpperCase());
        HashMap<Object, Object> indexMap = new HashMap<Object, Object>();
        String tuple[];
        /*for (ColumnDefinition columnDefinition : createTable.getColumnDefinitions()) {
            if (columnDefinition.getColDataType().getDataType().equals("INT"))
                indexMap = new HashMap<Integer, Integer>();
            if (columnDefinition.getColDataType().getDataType().equals("DECIMAL"))
                indexMap = new HashMap<Double, Integer>();
            if (columnDefinition.getColDataType().getDataType().equals("CHAR"))
                indexMap = new HashMap<String, Integer>();
            if (columnDefinition.getColDataType().getDataType().equals("VARCHAR"))
                indexMap = new HashMap<String, Integer>();
            if (columnDefinition.getColDataType().getDataType().equals("DATE"))
                indexMap = new HashMap<Date, Integer>();
        }*/

        //getRowSize(table, column);

        int indexOfColumn = getIndexOfColumn(table, column);

        try {
            LineNumberReader br = new LineNumberReader(new FileReader(TableIterator.TABLE_DIRECTORY + table + TableIterator.extension));

            //RandomAccessFile randomAccessFile = new RandomAccessFile(TableIterator.TABLE_DIRECTORY + table + TableIterator.extension, "rw");
            String line;

            while ((line = br.readLine()) != null) {

                tuple = line.split("\\|");

                if (!indexMap.containsKey(tuple[indexOfColumn]))
                    indexMap.put(Integer.parseInt(tuple[indexOfColumn]), br.getLineNumber());

            }

            writeDataDisk(indexMap, table, column);

            br.close();
            br = null;

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void writeDataDisk(HashMap<Object, Object> indexMap, String table, String column) throws Exception {

        File file = new File(TableIterator.TABLE_DIRECTORY + "INDEX_" + table + "_" + column);
        file.createNewFile();

        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));

        for (Map.Entry<Object, Object> entry : indexMap.entrySet()) {
            Integer key = (Integer) entry.getKey();
            Integer value = (Integer) entry.getValue();

            bufferedWriter.write(key + "|" + value + "\n");
        }

        bufferedWriter.close();
        bufferedWriter = null;

    }


    private int getIndexOfColumn(String table, String column) {
        int index = 0;
        CreateTable createTable = schemas.get(table.toUpperCase());

        for (ColumnDefinition columnDefinition : createTable.getColumnDefinitions()) {
            if (columnDefinition.getColumnName().equals(column))
                return index;
            index++;
        }

        return index;
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

    private void createSchema() {

        BufferedReader reader = null;
        String line;
        String in = "";
        CreateTable createTable = null;

        try {

            fileReader = new FileReader(path + fileName);
            reader = new BufferedReader(fileReader);

            while ((line = reader.readLine()) != null) {
                in += line + " ";
                if (line.charAt(line.length() - 1) == ';') {

                    StringReader input = new StringReader(in);
                    CCJSqlParser parser = new CCJSqlParser(input);

                    createTable = (CreateTable) parser.Statement();

                    buildCreateTable(createTable);

                    in = "";
                }
                //System.out.println(in);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private synchronized void saveCreateStatement(CreateTable createTable) {

        try {

            file = new File(path + fileName);
            file.createNewFile();

            if (writer == null)
                writer = new BufferedWriter(new FileWriter(path + fileName, true));


            CreateTable ct = new CreateTable();

            ct.setTable(createTable.getTable());
            ct.setColumnDefinitions(createTable.getColumnDefinitions());


            writer.write(ct.toString() + ";");
            writer.write("\n");

            writer.close();
            writer = null;

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to parse the SelectBody interface which can be a PlainSelect object or Union object
     *
     * @param selectBody   Interface containing the body of a SELECT statement
     * @param rootIterator RAIterator object upon which the SelectBody's iterators will be added
     * @return RAIterator object containing the SelecyBody's iterators
     */
    private RAIterator parseSelectBody(SelectBody selectBody, String selectAlias, RAIterator rootIterator) throws Exception {

        Union union;
        PlainSelect plainSelect;

        if ((union = (Union) CommonLib.castAs(selectBody, Union.class)) != null) {
            rootIterator = buildUnion(union, rootIterator);
        } else if ((plainSelect = (PlainSelect) CommonLib.castAs(selectBody, PlainSelect.class)) != null) {
            //plainSelect = rebuildSelect(plainSelect);
            rootIterator = buildPlainSelect(plainSelect, selectAlias, rootIterator);
        }

        return rootIterator;
    }


    private RAIterator parseFromItem(FromItem fromItem, RAIterator rootIterator) throws Exception {

        Table table;
        SubSelect subSelect;
        SubJoin subJoin;

        if ((table = (Table) CommonLib.castAs(fromItem, Table.class)) != null) {
            rootIterator = buildTable(table, rootIterator);
        } else if ((subSelect = (SubSelect) CommonLib.castAs(fromItem, SubSelect.class)) != null) {
            rootIterator = buildSubSelect(subSelect, rootIterator);
        } else if ((subJoin = (SubJoin) CommonLib.castAs(fromItem, SubJoin.class)) != null) {
            rootIterator = buildSubJoin(subJoin, rootIterator);
        }

        return rootIterator;

    }

    //endregion

    //region Building methods

    /**
     * Method to read in CREATE TABLE statement
     * Stores the associated List<ColumnDefinition> schema
     * }
     *
     * @param createTable CreateTable object containing the schema and relevant details
     *                    of the base table to be read from filesystem
     */
    private void buildCreateTable(CreateTable createTable) {

      /*if (createTable.getTable().getAlias() != null)
         schemas.put(createTable.getTable().getAlias(),createTable);
      else*/
        schemas.put(createTable.getTable().getName().toUpperCase(), createTable);

    }

    /**
     * Method to read in SELECT statement and begin building Iterator object
     * Calls buildWithItem() and parseSelectBody() methods
     *
     * @param select Select object which is any valid SQL query
     * @return The final RAIterator object to execute SQL query in volcano-style
     */
    private RAIterator buildSelect(Select select) throws Exception {

        RAIterator rootIterator = null;

        if (select.getWithItemsList() != null) {
            for (WithItem withItem : select.getWithItemsList())
                rootIterator = buildWithItem(withItem, rootIterator);
        }

        rootIterator = parseSelectBody(select.getSelectBody(), null, rootIterator);

        return rootIterator;

    }

    /**
     * Method to parse WITH q AS... clause of SQL Statement
     *
     * @param withItem     The WithItem object containing various parts of the WITH statement
     * @param rootIterator RAIterator object upon which the WITH statement's iterators will be added
     * @return RAIterator object containing the underlying iterators of the WITH statement
     */
    private RAIterator buildWithItem(WithItem withItem, RAIterator rootIterator) throws Exception {

        rootIterator = parseSelectBody(withItem.getSelectBody(), withItem.getName(), rootIterator);

        return rootIterator;

    }


    private RAIterator buildUnion(Union union, RAIterator rootIterator) throws Exception {

        RAIterator[] plainSelectIterators = new RAIterator[union.getPlainSelects().size()];
        for (int index = 0; index < plainSelectIterators.length; index++) {
            RAIterator plainSelectIterator = null;
            plainSelectIterators[index] = buildPlainSelect(union.getPlainSelects().get(index), null, plainSelectIterator);
        }

        return new UnionIterator(union, plainSelectIterators);

    }

    /**
     * Method to build iterator on the PlainSelect object, which can contain
     * the various modifiers in a SQL statement
     *
     * @param plainSelect  PlainSelect object representing the SELECT targetlist FROM tables WHERE clauses
     * @param rootIterator RAIterator object upon which the PlainSelect's iterators will be added
     * @return RAIterator object containing the PlainSelect's iterators
     */
    private RAIterator buildPlainSelect(PlainSelect plainSelect, String selectAlias, RAIterator rootIterator) throws Exception {

        if (plainSelect.getFromItem() != null)
            rootIterator = parseFromItem(plainSelect.getFromItem(), rootIterator);

        if (plainSelect.getJoins() != null)
            if (!plainSelect.getJoins().isEmpty())
                for (Join join : plainSelect.getJoins())
                    rootIterator = buildJoin(join, rootIterator);

        if (plainSelect.getWhere() != null)
            rootIterator = new FilterIterator(rootIterator, plainSelect.getWhere());

        rootIterator = new MapIterator(rootIterator, plainSelect.getSelectItems(), selectAlias);

        if (plainSelect.getGroupByColumnReferences() != null) {
            rootIterator = new GroupByIterator(rootIterator, plainSelect.getSelectItems(), selectAlias, plainSelect.getGroupByColumnReferences());
        } else if (plainSelect.getGroupByColumnReferences() == null && isAggregateQuery(plainSelect.getSelectItems())) { // Aggregate Iterator
            rootIterator = new aggregateIterator(rootIterator, plainSelect.getSelectItems(), selectAlias);
        }


        if (plainSelect.getHaving() != null) {
            rootIterator = new HavingIterator(rootIterator, plainSelect.getSelectItems(), plainSelect.getHaving());
        }

        if (plainSelect.getOrderByElements() != null) {
            rootIterator = new OrderByIterator(rootIterator, plainSelect.getOrderByElements(), plainSelect);
        }

        if (plainSelect.getLimit() != null) {
            rootIterator = new LimitIterator(rootIterator, plainSelect.getLimit());
        }

        return rootIterator;

    }

    /**
     * @param table        Table object using which the TableIterator will be created
     * @param rootIterator RAIterator object upon which the TableIterator will be created
     * @return RAIterator object containing a TableIterator
     */
    private RAIterator buildTable(Table table, RAIterator rootIterator) throws Exception {

        String tableAlias;
        if (table.getAlias() != null)
            tableAlias = table.getAlias();
        else
            tableAlias = table.getName();

        ColumnDefinition[] columnDefinitions = schemas.get(table.getName().toUpperCase()).getColumnDefinitions().toArray(new ColumnDefinition[schemas.get(table.getName().toUpperCase()).getColumnDefinitions().size()]);
        rootIterator = new TableIterator(table.getName(), tableAlias, columnDefinitions);
        return rootIterator;

    }

    /**
     * @param subSelect    SELECT statement which is used as a FROM clause
     * @param rootIterator RAIterator upon which the SubSelect's iterators will be added
     * @return RAIterator object containing the SubSelect's iterators
     */
    private RAIterator buildSubSelect(SubSelect subSelect, RAIterator rootIterator) throws Exception {

        rootIterator = parseSelectBody(subSelect.getSelectBody(), subSelect.getAlias(), rootIterator);
        return rootIterator;

    }

    /**
     * @param subJoin      Table created by JOIN
     * @param rootIterator RAIterator object upon which the SubJoin's iterators will be added
     * @return RAIterator object containing the SubJoin's iterators
     */
    private RAIterator buildSubJoin(SubJoin subJoin, RAIterator rootIterator) throws Exception {

        RAIterator leftIterator = null;
        leftIterator = parseFromItem(subJoin.getLeft(), leftIterator);
        rootIterator = buildJoin(subJoin.getJoin(), leftIterator);
        return rootIterator;

    }

    /**
     * @param join         JOIN part in a SQL statement
     * @param rootIterator RAIterator object upon which the JoinIterator will be added
     * @return RAIterator object containing the JoinIterator
     */
    private RAIterator buildJoin(Join join, RAIterator rootIterator) throws Exception {

        RAIterator rightIterator = null;
        rightIterator = parseFromItem(join.getRightItem(), rightIterator);
        rootIterator = new JoinIterator(rootIterator, rightIterator, join.getOnExpression());
        return rootIterator;

    }

    //endregion

    private boolean isAggregateQuery(List<SelectItem> selectItems) {
        Function function;

        for (int index = 0; index < selectItems.size(); index++) {
            if (selectItems.get(index) instanceof AllColumns) {
                continue;
            } else if (selectItems.get(index) instanceof AllTableColumns) {
                continue;
            } else if (((SelectExpressionItem) selectItems.get(index)).getExpression() instanceof Function) {
                return true;
            }
        }
        return false;
    }
}
