package dubstep;

import builders.IteratorBuilder;
import helpers.CommonLib;
import iterators.RAIterator;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;


import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static boolean inMem = true ;
    static boolean debugEnabled = true;

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public static Map<String, ArrayList<String>> globalIndex = new HashMap<String, ArrayList<String>>();
    public static Map<String, String> globalPrimaryIndex = new HashMap<String, String>() ;
    public static Map<String, ArrayList<PrimitiveValue[]>> masterIndex = new HashMap<String, ArrayList<PrimitiveValue[]>>(); // TODO: Change back to string[]
    public static boolean isPhase1 = true ;
    public static BufferedWriter globalIndexWriter = null ;
    public static boolean closedFlag = false ;
    public static String currentQuery ;


    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    static Runtime r = Runtime.getRuntime();


    public static void main(String[] args) throws Exception
    {

        System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
        File f = new File(CommonLib.INDEX_DIRECTORY + "GlobalIndex");


        if(f.exists() && !f.isDirectory()) {
            isPhase1 = false ;
            BufferedReader globalIndexReader = new BufferedReader(new FileReader(CommonLib.INDEX_DIRECTORY + "GlobalIndex"));
            String currentLine ;
            while ((currentLine = globalIndexReader.readLine()) != null) {
                String[] currentLineAsStringArray = currentLine.split("\\|") ;
                globalPrimaryIndex.put(currentLineAsStringArray[0], currentLineAsStringArray[currentLineAsStringArray.length-1]);
                ArrayList<String> indexColumns = new ArrayList<String>();
                for(int i = 1; i <currentLineAsStringArray.length-1; i++){
                    indexColumns.add(currentLineAsStringArray[i]);
                }
                globalIndex.put(currentLineAsStringArray[0], indexColumns);

                currentLine = globalIndexReader.readLine() ;
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
        }else{
            globalIndexWriter = new BufferedWriter(new FileWriter(CommonLib.INDEX_DIRECTORY + "GlobalIndex", true));
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
//        String q22 ="SELECT LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS, SUM(LINEITEM.QUANTITY) AS SUM_QTY, SUM(LINEITEM.EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS SUM_DISC_PRICE, SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)*(1+LINEITEM.TAX)) AS SUM_CHARGE, AVG(LINEITEM.QUANTITY) AS AVG_QTY, AVG(LINEITEM.EXTENDEDPRICE) AS AVG_PRICE, AVG(LINEITEM.DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER FROM LINEITEM WHERE LINEITEM.SHIPDATE <= DATE('1999-03-21') GROUP BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS ORDER BY LINEITEM.RETURNFLAG, LINEITEM.LINESTATUS; ";
        String q22 = "SELECT COUNT(*) FROM LINEITEM WHERE LINEITEM.SHIPDATE <= DATE('1999-03-21');" ;
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
            a = new String[]{q44, q22, q30};
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
            try {
                rootIterator = iteratorBuilder.parseStatement(query);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (rootIterator != null) {
                if(isPhase1 && !closedFlag){
                    globalIndexWriter.close();
                    globalIndexWriter = null ;
                    closedFlag = true ;
                }
                long startTime = System.currentTimeMillis();
                rootIterator = rootIterator.optimize(rootIterator);
                rootIterator = rootIterator.optimize(rootIterator);

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
}







//
//
//
//
//package dubstep;
//
//import builders.IteratorBuilder;
//import helpers.CommonLib;
//import iterators.RAIterator;
//import net.sf.jsqlparser.expression.PrimitiveValue;
//import net.sf.jsqlparser.parser.CCJSqlParser;
//import net.sf.jsqlparser.statement.Statement;
//
//
//import java.io.*;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Map;
//
//public class Main {
//
//    public static boolean inMem = true;
//
//    public static Map<String, ArrayList<String>> globalIndex = new HashMap<String, ArrayList<String>>();
//    public static Map<String, String> globalPrimaryIndex = new HashMap<String, String>() ;
//    public static Map<String, ArrayList<PrimitiveValue[]>> masterIndex = new HashMap<String, ArrayList<PrimitiveValue[]>>(); // TODO: Change back to string[]
//    public static boolean isPhase1 = true ;
//    public static BufferedWriter globalIndexWriter = null ;
//    public static boolean closedFlag = false ;
//    public static String currentQuery ;
//
//    static private int k = 0;
//    static private String select = "";
//    public static void main(String[] args) throws Exception {
//
//        //System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
//
//        File f = new File(CommonLib.INDEX_DIRECTORY + "GlobalIndex");
//
//
//        if(f.exists() && !f.isDirectory()) {
//            isPhase1 = false ;
//            BufferedReader globalIndexReader = new BufferedReader(new FileReader(CommonLib.INDEX_DIRECTORY + "GlobalIndex"));
//            String currentLine ;
//            while ((currentLine = globalIndexReader.readLine()) != null) {
//                String[] currentLineAsStringArray = currentLine.split("\\|") ;
//                globalPrimaryIndex.put(currentLineAsStringArray[0], currentLineAsStringArray[currentLineAsStringArray.length-1]);
//                ArrayList<String> indexColumns = new ArrayList<String>();
//                for(int i = 1; i <currentLineAsStringArray.length-1; i++){
//                    indexColumns.add(currentLineAsStringArray[i]);
//                }
//                globalIndex.put(currentLineAsStringArray[0], indexColumns);
//
//                currentLine = globalIndexReader.readLine() ;
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
//        }else{
//            globalIndexWriter = new BufferedWriter(new FileWriter(CommonLib.INDEX_DIRECTORY + "GlobalIndex", true));
//        }
//
//        for (int j = 0; j < args.length; j++) {
//            if (args[j].equals("--on-disk")) {
//                inMem = false;
//                break;
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
//            if(line.charAt(line.length()-1) == ';')
//                break;
//        }
//        currentQuery = in ;
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
//            try {
//                rootIterator = iteratorBuilder.parseStatement(query);
//
//                if (rootIterator != null) {
//                    if(isPhase1 && !closedFlag){
//                        globalIndexWriter.close();
//                        globalIndexWriter = null ;
//                        closedFlag = true ;
//                    }
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
//                                    //cnt++;
//                                    System.out.print("|");
//                                }
//                            }
//                            System.out.print("\n");
//                        }
//                    }
//
////                    if(k == 4)
////                        System.out.println(select);
//                }
//
//
//                System.out.println("$> ");
//                reader = new BufferedReader(new InputStreamReader(System.in));
//
//                in ="";
//                while ((line = reader.readLine()) != null) {
//                    in += line+ " ";
//                    if(line.charAt(line.length()-1) == ';')
//                        break;
//                }
//
//
//                input = new StringReader(in);
//                parser = new CCJSqlParser(input);
//
//            } catch (Exception e) {
//                e.printStackTrace();
//                System.out.println("$> ");
//                reader = new BufferedReader(new InputStreamReader(System.in));
//                while ((line = reader.readLine()) != null) {
//                    in += line+ " ";
//                    if(line.charAt(line.length()-1) == ';')
//                        break;
//                }
//
//                input = new StringReader(in);
//                parser = new CCJSqlParser(input);
//            }
//        }
//    }
//
//}
