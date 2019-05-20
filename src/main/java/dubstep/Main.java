package dubstep;

import builders.IteratorBuilder;
import helpers.CommonLib;
import iterators.*;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;


import java.io.*;
import java.util.*;

import static helpers.CommonLib.castAs;

public class Main {

    public static boolean inMem = true ;
    private static boolean debugEnabled = true;

    ///////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public static Map<String, ArrayList<String>> globalIndex = new HashMap<String, ArrayList<String>>();
    public static Map<String, String> globalPrimaryIndex = new HashMap<String, String>() ;
    public static Map<String, ArrayList<PrimitiveValue[]>> masterIndex = new HashMap<String, ArrayList<PrimitiveValue[]>>(); // TODO: Change back to string[]
    public static boolean isPhase1 = true ;
    public static String currentQuery ;
    public static List<Column> columnList = null ;


    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public static HashMap<String, Set<Column>> updatesColumns = new HashMap<>() ;

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void main(String[] args) throws Exception
    {

//        System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
        File f = new File(CommonLib.INDEX_DIRECTORY + "GlobalIndex");

        if (f.exists() && !f.isDirectory()) {
            isPhase1 = false;
            BufferedReader globalIndexReader = new BufferedReader(new FileReader(CommonLib.INDEX_DIRECTORY + "GlobalIndex"));
            String currentLine;
            while ((currentLine = globalIndexReader.readLine()) != null) {
                String[] currentLineAsStringArray = currentLine.split("\\|");
                globalPrimaryIndex.put(currentLineAsStringArray[0], currentLineAsStringArray[currentLineAsStringArray.length - 1]);
                ArrayList<String> indexColumns = new ArrayList<String>();
                for (int i = 1; i < currentLineAsStringArray.length - 1; i++) {
                    indexColumns.add(currentLineAsStringArray[i]);
                }
                globalIndex.put(currentLineAsStringArray[0], indexColumns);

                currentLine = globalIndexReader.readLine();
                StringReader input = new StringReader(currentLine);
                CCJSqlParser parser = new CCJSqlParser(input);
                Statement query = parser.Statement();
                RAIterator createStatementIterator = null;
                IteratorBuilder iteratorBuilderInstance = new IteratorBuilder();
                try {
                    createStatementIterator = iteratorBuilderInstance.parseStatement(query);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
// region queries
        String a[] = null;
        String a1 = "CREATE TABLE LINEITEM (ORDERKEY INT , PARTKEY INT , SUPPKEY INT , LINENUMBER INT , QUANTITY DECIMAL , EXTENDEDPRICE DECIMAL , DISCOUNT DECIMAL , TAX DECIMAL , RETURNFLAG CHAR (1) , LINESTATUS CHAR (1) , SHIPDATE DATE , COMMITDATE DATE , RECEIPTDATE DATE , SHIPINSTRUCT CHAR (25) , SHIPMODE CHAR (10) , COMMENT VARCHAR (44) , PRIMARY KEY (ORDERKEY, LINENUMBER), INDEX SHIPDATE (SHIPDATE), INDEX RECEIPTDATE (RECEIPTDATE), INDEX RETURNFLAG (RETURNFLAG)) ;";
        String a2 = "CREATE TABLE ORDERS (ORDERKEY INT , CUSTKEY INT , ORDERSTATUS CHAR (1) , TOTALPRICE DECIMAL , ORDERDATE DATE , ORDERPRIORITY CHAR (15) , CLERK CHAR (15) , SHIPPRIORITY INT , COMMENT VARCHAR (79) , PRIMARY KEY (ORDERKEY), INDEX ORDERDATE (ORDERDATE)) ;";
        String a3 = "CREATE TABLE PART (PARTKEY INT , NAME VARCHAR (55) , MFGR CHAR (25) , BRAND CHAR (10) , TYPE VARCHAR (25) , SIZE INT , CONTAINER CHAR (10) , RETAILPRICE DECIMAL , COMMENT VARCHAR (23) , PRIMARY KEY (PARTKEY)) ;";
        String a4 = "CREATE TABLE CUSTOMER (CUSTKEY INT , NAME VARCHAR (25) , ADDRESS VARCHAR (40) , NATIONKEY INT , PHONE CHAR (15) , ACCTBAL DECIMAL , MKTSEGMENT CHAR (10) , COMMENT VARCHAR (117) , PRIMARY KEY (CUSTKEY), INDEX MKTSEGMENT (MKTSEGMENT)) ;";
        String a5 = "CREATE TABLE SUPPLIER (SUPPKEY INT , NAME CHAR (25) , ADDRESS VARCHAR (40) , NATIONKEY INT , PHONE CHAR (15) , ACCTBAL DECIMAL , COMMENT VARCHAR (101) , PRIMARY KEY (SUPPKEY), INDEX NATIONKEY (NATIONKEY)) ;";
        String a6 = "CREATE TABLE PARTSUPP (PARTKEY INT , SUPPKEY INT , AVAILQTY INT , SUPPLYCOST DECIMAL , COMMENT VARCHAR (199) , PRIMARY KEY (PARTKEY, SUPPKEY)) ;" ;
        String a7 = "CREATE TABLE NATION (NATIONKEY INT , NAME CHAR (25) , REGIONKEY INT , COMMENT VARCHAR (152) , PRIMARY KEY (NATIONKEY)) ;" ;
        String a8 = "CREATE TABLE REGION (REGIONKEY INT , NAME CHAR (25) , COMMENT VARCHAR (152) , PRIMARY KEY (REGIONKEY)) ;" ;
        String q22 ="SELECT LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS, SUM(LINEITEM.QUANTITY) AS SUM_QTY, SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS SUM_DISC_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)*(1+LINEITEM.TAX)) AS SUM_CHARGE, AVG(LINEITEM.QUANTITY) AS AVG_QTY, AVG(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE, AVG(LINEITEM.DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER FROM LINEITEM WHERE LINEITEM.SHIPDATE <= DATE('1999-03-21') GROUP BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS ORDER BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS; ";
        String q30 = "SELECT\n" +
                "LINEITEM.ORDERKEY,\n" +
                "SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS REVENUE, \n" +
                "ORDERS.ORDERDATE,\n" +
                "ORDERS.SHIPPRIORITY\n" +
                "FROM\n" +
                "CUSTOMER,\n" +
                "ORDERS,\n" +
                "LINEITEM \n" +
                "WHERE\n" +
                "CUSTOMER.MKTSEGMENT = 'BUILDING' AND CUSTOMER.CUSTKEY = ORDERS.CUSTKEY\n" +
                "AND LINEITEM.ORDERKEY = ORDERS.ORDERKEY \n" +
                "AND ORDERS.ORDERDATE < DATE('1995-03-29')\n" +
                "AND LINEITEM.SHIPDATE > DATE('1995-03-29')\n" +
                "GROUP BY LINEITEM.ORDERKEY, ORDERS.ORDERDATE, ORDERS.SHIPPRIORITY \n" +
                "ORDER BY REVENUE DESC, ORDERDATE\n" +
                "LIMIT 10;";
        String  q44 = "SELECT \n" +
                "SUM(LINEITEM.EXTENDEDPRICE*LINEITEM.DISCOUNT) AS REVENUE\n" +
                "FROM\n" +
                "LINEITEM\n" +
                "WHERE\n" +
                "LINEITEM.SHIPDATE >= DATE('1994-01-01')\n" +
                "AND LINEITEM.SHIPDATE < DATE ('1995-01-01')\n" +
                "AND LINEITEM.DISCOUNT > 0.08 AND LINEITEM.DISCOUNT < 0.1 \n" +
                "AND LINEITEM.QUANTITY < 24;";
        String i1 = "INSERT INTO ORDERS(ORDERKEY, CUSTKEY, ORDERSTATUS, TOTALPRICE, ORDERDATE, ORDERPRIORITY, CLERK, SHIPPRIORITY, COMMENT) VALUES (6000001, 8606, 'O', 173905.43678800002, {d'1998-12-31'}, '5-LOW', 'Clerk#000000392', 0, 'platelets unwind-- always quick attainments through the sauternes was b');";
        String d1 = "DELETE FROM ORDERS WHERE ORDERKEY = 2704999;";
        String d2 = "DELETE FROM ORDERS WHERE CUSTKEY = 1000;";
        String u1 = "UPDATE ORDERS\n" +
                "SET ORDERKEY = ORDERKEY + 1, CUSTKEY = CUSTKEY - 1\n" +
                "WHERE ORDERKEY = 60000010000;";

        if(isPhase1){
            a = new String[]{a1, a2, a3, a4, a5, a6, a7, a8, q44, q22, q30};
        }else{
//            String q22 ="SELECT LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS, SUM(LINEITEM.QUANTITY) AS SUM_QTY, SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS SUM_DISC_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)*(1+LINEITEM.TAX)) AS SUM_CHARGE, AVG(LINEITEM.QUANTITY) AS AVG_QTY, AVG(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE, AVG(LINEITEM.DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER FROM LINEITEM WHERE LINEITEM.SHIPDATE <= DATE('1999-03-21') GROUP BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS ORDER BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS; ";
//            String q30 = "SELECT\n" +
//                    "LINEITEM.ORDERKEY,\n" +
//                    "SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS REVENUE, \n" +
//                    "ORDERS.ORDERDATE,\n" +
//                    "ORDERS.SHIPPRIORITY\n" +
//                    "FROM\n" +
//                    "CUSTOMER,\n" +
//                    "ORDERS,\n" +
//                    "LINEITEM \n" +
//                    "WHERE\n" +
//                    "CUSTOMER.MKTSEGMENT = 'BUILDING' AND CUSTOMER.CUSTKEY = ORDERS.CUSTKEY\n" +
//                    "AND LINEITEM.ORDERKEY = ORDERS.ORDERKEY \n" +
//                    "AND ORDERS.ORDERDATE < DATE('1995-03-29')\n" +
//                    "AND LINEITEM.SHIPDATE > DATE('1995-03-29')\n" +
//                    "GROUP BY LINEITEM.ORDERKEY, ORDERS.ORDERDATE, ORDERS.SHIPPRIORITY \n" +
//                    "ORDER BY REVENUE DESC, ORDERDATE\n" +
//                    "LIMIT 10;";
            a = new String[]{i1, d1, d2, u1, d1, q44, i1, q30, i1};
        }

// endregion queries
        int i = 0;

        IteratorBuilder iteratorBuilder = new IteratorBuilder();
        RAIterator rootIterator = null;

        while (i < a.length) {
            currentQuery = a[i] ;
            StringReader input = new StringReader(a[i]);
            CCJSqlParser parser = new CCJSqlParser(input);
            Statement query = parser.Statement();

            if((query instanceof Select)){
                columnList = getColumnList(query);
            }
            else if((query instanceof Delete)){
                Set<Column> updatedList;
                Set<Column> currentList = updatesColumns.get(((Delete) query).getTable().getName());
                updatedList = (currentList != null) ? currentList : new HashSet<Column>();
                for (Column column : CommonLib.getInstance().getColumnList(((Delete) query).getWhere())){
                    column.setTable(((Delete) query).getTable());
                    updatedList.add(column) ;
                }
                updatesColumns.put(((Delete) query).getTable().getName(), updatedList);
            }
            else if((query instanceof Update)){
                Set<Column> updatedList;
                Set<Column> currentList = updatesColumns.get(((Update) query).getTable().getName());
                updatedList = (currentList != null) ? currentList : new HashSet<Column>();
                for (Column column : CommonLib.getInstance().getColumnList(((Update) query).getWhere())){
                    column.setTable(((Update) query).getTable());
                    updatedList.add(column) ;
                }
                for(Expression exp : ((Update) query).getExpressions()){
                    for(Column column : CommonLib.getInstance().getColumnList(exp)){
                        column.setTable(((Update) query).getTable());
                        updatedList.add(column) ;
                    }
                }
                for(Column column : ((Update) query).getColumns()){
                    column.setTable(((Update) query).getTable());
                    updatedList.add(column) ;
                }
                updatesColumns.put(((Update) query).getTable().getName(), updatedList);
            }
            try {
                rootIterator = iteratorBuilder.parseStatement(query);
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (rootIterator != null) {
                long startTime = System.currentTimeMillis();
                rootIterator = rootIterator.optimize(rootIterator);
                rootIterator = rootIterator.optimize(rootIterator);
                rootIterator = updateRAIterator(rootIterator);
                while (rootIterator.hasNext()) {
                    PrimitiveValue[] tuple = rootIterator.next();
                    if (tuple != null) {
                        for (int index = 0; index < tuple.length; index++) {
                            System.out.print(tuple[index].toRawString());
                            if (index != (tuple.length - 1))
                                System.out.print("|");
                        }
                        System.out.print("\n");
                    }
                }
                if (debugEnabled) {
                    long endTime = System.currentTimeMillis();
                    System.out.println(endTime - startTime);
                }
            }
            i++;
        }
    }

    private static List<Column> getColumnList(Statement query) {
        CommonLib commonLib = CommonLib.getInstance();

        SelectBody selectBody = ((Select) query).getSelectBody();

        List<Column> columnList = new ArrayList<Column>();

        Set<Column> columnSet = new HashSet<Column>();

        List<SelectItem> selectItemList = ((PlainSelect) selectBody).getSelectItems();
        List<OrderByElement> orderByElementList = ((PlainSelect) selectBody).getOrderByElements();
        List<Column> groupByColumnReferences = ((PlainSelect) selectBody).getGroupByColumnReferences();
        Expression expressionList = ((PlainSelect) selectBody).getWhere();

        if(expressionList != null){
            columnSet.addAll(commonLib.getColumnList(expressionList));
        }
        if(groupByColumnReferences != null && groupByColumnReferences.size() != 0){
            columnSet.addAll(groupByColumnReferences);
        }



        for (SelectItem selectItem : selectItemList) {
            Expression expression;
            SelectExpressionItem selectExpressionItem;
            Function function;

            if ((selectExpressionItem = (SelectExpressionItem) castAs(selectItem, SelectExpressionItem.class)) != null) {
                if ((function = (Function) castAs(selectExpressionItem.getExpression(), Function.class)) != null && !function.isAllColumns()) {
                    List<Expression> expressionList1 = function.getParameters().getExpressions();
                    for (Expression exp : expressionList1) {
                        columnSet.addAll(commonLib.getColumnList(exp));
                    }
                } else {
                    columnSet.addAll(commonLib.getColumnList(((SelectExpressionItem) selectItem).getExpression()));
                }
            }
        }

        if (orderByElementList != null && orderByElementList.size() != 0) {
            for (OrderByElement orderByElement : orderByElementList) {
                columnSet.add((Column) orderByElement.getExpression());
            }
        }
        Set<String> tableNames = new HashSet<>() ;
        for (Column column : columnSet){
            tableNames.add(column.getTable().getName()) ;
        }
        for(String name : tableNames){
            if(updatesColumns.containsKey(name)){
                columnSet.addAll(updatesColumns.get(name)) ;
            }

        }
        columnList.addAll(columnSet);
        return columnList;
    }

    private static RAIterator updateRAIterator(RAIterator rootIterator){

        if(rootIterator instanceof TableIterator){
            if(IteratorBuilder.listOfStatements.containsKey(((TableIterator) rootIterator).getTableName())){
                for(Statement statement : IteratorBuilder.listOfStatements.get(((TableIterator) rootIterator).getTableName())){
                    if(statement instanceof Delete){
                        rootIterator = new FilterIterator(rootIterator, new InverseExpression(((Delete) statement).getWhere())) ;
                    }else if(statement instanceof Update){
                        rootIterator = new updateIterator(rootIterator, (Update) statement) ;
                    }
                }
            }
        }else if(rootIterator instanceof IndexIterator){
            if(IteratorBuilder.listOfStatements.containsKey(((IndexIterator) rootIterator).getTableName())){
                for(Statement statement : IteratorBuilder.listOfStatements.get(((IndexIterator) rootIterator).getTableName())){
                    if(statement instanceof Delete){
                        rootIterator = new FilterIterator(rootIterator, new InverseExpression(((Delete) statement).getWhere())) ;
                    }else if(statement instanceof Update){
                        rootIterator = new updateIterator(rootIterator, (Update) statement) ;
                    }
                }
            }
        } else{
            if(rootIterator instanceof JoinIterator){
                ((JoinIterator) rootIterator).setRightChild(updateRAIterator(((JoinIterator) rootIterator).getRightChild()));
                rootIterator.setChild(updateRAIterator(rootIterator.getChild())) ;
                return rootIterator ;
            }else{
                rootIterator.setChild(updateRAIterator(rootIterator.getChild())) ;
                return rootIterator ;
            }
        }
        return rootIterator ;

    }

}


//package dubstep;
//
//import builders.IteratorBuilder;
//import helpers.CommonLib;
//import iterators.RAIterator;
//import net.sf.jsqlparser.expression.Expression;
//import net.sf.jsqlparser.expression.Function;
//import net.sf.jsqlparser.expression.PrimitiveValue;
//import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
//import net.sf.jsqlparser.parser.CCJSqlParser;
//import net.sf.jsqlparser.schema.Column;
//import net.sf.jsqlparser.statement.Statement;
//import net.sf.jsqlparser.statement.create.table.CreateTable;
//import net.sf.jsqlparser.statement.select.*;
//
//import java.io.*;
//import java.util.*;
//
//import static helpers.CommonLib.castAs;
//
//public class Main {
//
//    public static boolean inMem = true;
//    static public boolean create_lineitem_view = false;
//
//    public static Map<String, ArrayList<String>> globalIndex = new HashMap<String, ArrayList<String>>();
//    public static Map<String, String> globalPrimaryIndex = new HashMap<String, String>();
//    public static Map<String, ArrayList<PrimitiveValue[]>> masterIndex = new HashMap<String, ArrayList<PrimitiveValue[]>>(); // TODO: Change back to string[]
//    public static boolean isPhase1 = true;
//    public static boolean closedFlag = false;
//    public static String currentQuery;
//
//    public static List<Column> columnList = null ;
//
//
//    public static void main(String[] args) throws Exception {
//
//
//        File f = new File(CommonLib.INDEX_DIRECTORY + "GlobalIndex");
//
//        if (f.exists() && !f.isDirectory()) {
//            isPhase1 = false;
//            BufferedReader globalIndexReader = new BufferedReader(new FileReader(CommonLib.INDEX_DIRECTORY + "GlobalIndex"));
//            String currentLine;
//            while ((currentLine = globalIndexReader.readLine()) != null) {
//                String[] currentLineAsStringArray = currentLine.split("\\|");
//                globalPrimaryIndex.put(currentLineAsStringArray[0], currentLineAsStringArray[currentLineAsStringArray.length - 1]);
//                ArrayList<String> indexColumns = new ArrayList<String>();
//                for (int i = 1; i < currentLineAsStringArray.length - 1; i++) {
//                    indexColumns.add(currentLineAsStringArray[i]);
//                }
//                globalIndex.put(currentLineAsStringArray[0], indexColumns);
//
//                currentLine = globalIndexReader.readLine();
//                StringReader input = new StringReader(currentLine);
//                CCJSqlParser parser = new CCJSqlParser(input);
//                Statement query = parser.Statement();
//                RAIterator createStatementIterator = null;
//                IteratorBuilder iteratorBuilderInstance = new IteratorBuilder();
//                try {
//                    createStatementIterator = iteratorBuilderInstance.parseStatement(query);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//
//
//        System.out.println("$> ");
//
//        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
//        String in = "";
//        String line;
//
//        while ((line = reader.readLine()) != null) {
//            in += line + " ";
//            if (line.charAt(line.length() - 1) == ';')
//                break;
//        }
//        currentQuery = in;
//        StringReader input = new StringReader(in);
//
//        CCJSqlParser parser = new CCJSqlParser(input);
//
//        Statement query;
//        IteratorBuilder iteratorBuilder = new IteratorBuilder();
//        RAIterator rootIterator = null;
//
//        while ((query = parser.Statement()) != null) {
//
//            if(!(query instanceof CreateTable)){
//                columnList = getColumnList(query);
//            }
//
//
//            try {
//                rootIterator = iteratorBuilder.parseStatement(query);
//
//                File file = new File(CommonLib.INDEX_DIRECTORY + "LINEITEM_VIEW.csv");
//                if (rootIterator == null && create_lineitem_view && !file.exists()) {
//                    create_lineitem_view = false;
//
//                    try {
//                        String view = "SELECT LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS, SUM(LINEITEM.QUANTITY) AS SUM_QTY, SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS SUM_DISC_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)*(1+LINEITEM.TAX)) AS SUM_CHARGE, AVG(LINEITEM.QUANTITY) AS AVG_QTY, AVG(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE, AVG(LINEITEM.DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER FROM LINEITEM GROUP BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS ORDER BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS;";
//
//                        input = new StringReader(view);
//                        parser = new CCJSqlParser(input);
//                        query = parser.Statement();
//                        if(!(query instanceof CreateTable)){
//                            columnList = getColumnList(query);
//                        }
//                        RAIterator viewIterator = iteratorBuilder.parseStatement(query);
//
//                        file.createNewFile();
//                        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
//
//                        while (viewIterator.hasNext()) {
//                            PrimitiveValue[] tuple = viewIterator.next();
//                            if (tuple != null) {
//                                for (int index = 0; index < tuple.length; index++) {
//                                    bufferedWriter.write(tuple[index].toRawString());
//                                    if (index != (tuple.length - 1))
//                                        bufferedWriter.write("|");
//                                }
//                                bufferedWriter.write("\n");
//                            }
//                        }
//                        bufferedWriter.close();
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//
//
//                if (currentQuery.contains("LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)*(1+LINEITEM.TAX)")) {
//
//                    LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(CommonLib.INDEX_DIRECTORY + "LINEITEM_VIEW.csv"));
//
//                    String out = "";
//                    while ((out = lineNumberReader.readLine()) != null) {
//                        System.out.println(out);
//                    }
//                    rootIterator = null;
//                }
//
//
//                if (rootIterator != null) {
//
//                    rootIterator = rootIterator.optimize(rootIterator);
//                    rootIterator = rootIterator.optimize(rootIterator);
//
//
//                    while (rootIterator.hasNext()) {
//                        PrimitiveValue[] tuple = rootIterator.next();
//                        if (tuple != null) {
//                            for (int index = 0; index < tuple.length; index++) {
//                                System.out.print(tuple[index].toRawString());
//                                if (index != (tuple.length - 1)) {
//                                    System.out.print("|");
//                                }
//                            }
//                            System.out.print("\n");
//                        }
//                    }
//                }
//
//
//                System.out.println("$> ");
//                reader = new BufferedReader(new InputStreamReader(System.in));
//
//                in = "";
//                while ((line = reader.readLine()) != null) {
//                    in += line + " ";
//                    if (line.charAt(line.length() - 1) == ';')
//                        break;
//                }
//
//                currentQuery = in;
//                input = new StringReader(in);
//                parser = new CCJSqlParser(input);
//
//            } catch (Exception e) {
//                e.printStackTrace();
//                System.out.println("$> ");
//                reader = new BufferedReader(new InputStreamReader(System.in));
//                while ((line = reader.readLine()) != null) {
//                    in += line + " ";
//                    if (line.charAt(line.length() - 1) == ';')
//                        break;
//                }
//                currentQuery = in;
//                input = new StringReader(in);
//                parser = new CCJSqlParser(input);
//            }
//        }
//    }
//
//    private static List<Column> getColumnList(Statement query) {
//        CommonLib commonLib = CommonLib.getInstance();
//
//        SelectBody selectBody = ((Select) query).getSelectBody();
//
//        List<Column> columnList = new ArrayList<Column>();
//
//        Set<Column> columnSet = new HashSet<Column>();
//
//        List<SelectItem> selectItemList = ((PlainSelect) selectBody).getSelectItems();
//        List<OrderByElement> orderByElementList = ((PlainSelect) selectBody).getOrderByElements();
//        List<Column> groupByColumnReferences = ((PlainSelect) selectBody).getGroupByColumnReferences();
//        Expression expressionList = ((PlainSelect) selectBody).getWhere();
//
//        if(expressionList != null){
//            columnSet.addAll(commonLib.getColumnList(expressionList));
//        }
//        if(groupByColumnReferences != null && groupByColumnReferences.size() != 0){
//            columnSet.addAll(groupByColumnReferences);
//        }
//
//        for (SelectItem selectItem : selectItemList) {
//            Expression expression;
//            SelectExpressionItem selectExpressionItem;
//            Function function;
//
//            if ((selectExpressionItem = (SelectExpressionItem) castAs(selectItem, SelectExpressionItem.class)) != null) {
//                if ((function = (Function) castAs(selectExpressionItem.getExpression(), Function.class)) != null && !function.isAllColumns()) {
//                    List<Expression> expressionList1 = function.getParameters().getExpressions();
//                    for (Expression exp : expressionList1) {
//                        columnSet.addAll(commonLib.getColumnList(exp));
//                    }
//                } else {
//                    columnSet.addAll(commonLib.getColumnList(((SelectExpressionItem) selectItem).getExpression()));
//                }
//            }
//        }
//
//        if (orderByElementList != null && orderByElementList.size() != 0) {
//            for (OrderByElement orderByElement : orderByElementList) {
//                columnSet.add((Column) orderByElement.getExpression());
//            }
//        }
//
//        columnList.addAll(columnSet);
//        return columnList;
//    }
//
//}
