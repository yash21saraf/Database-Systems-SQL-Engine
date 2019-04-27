package dubstep;

import builders.IteratorBuilder;
import iterators.RAIterator;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.io.StringReader;
import java.util.Date;

public class Main {

    public static ColDataType colDataTypes[];
    public static boolean inMem = true ;
    static boolean debugEnabled = true;

    static Runtime r = Runtime.getRuntime();


    public static void main(String[] args) throws Exception
    {
/*
         Stream<String> lines = Files.lines(Paths.get("file.txt"));
            String line32 = lines.skip(31).findFirst().get();
*/
        System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
        String q1 = "CREATE TABLE R(a int NOT NULL, b int, c int)";
        String q2 = "CREATE TABLE S(d int NOT NULL, e int, f int)";
        String q3 = "CREATE TABLE T(d int NOT NULL, e int, f int)" ;
////        String q3 = "select * from R UNION ALL select a from R";
////        String q3 = "select A.a,b,c from R as A";
////        String q3 = "select a , sum(b+c), count(c), min(b) from R where a != 170 group by a";
////        String q3 = "select R.a, R.b "|"from R,S as TT where R.a = 11";
////        String q3 = "select count(1), avg(b+d), sum(a+c) from R, S";
////        String q3 = "select a,b,* from R";
////        String q3 = "select c, d, sum(a+b) from R, S where c != 78 group by c, d order by c, d desc";
////        String q3 = "select c, d, sum(a+b) from R, S where c != 9 group by c, d having sum(a+b) <> 35 order by c desc";
////        String q3 = "select a, a+b from R,S" ;
////        String q3 = "select * from r where a = (select a from s where c>5)" ; //TODO: Manage in expressions, Manage subqueries in where clause
////        String q3 = "select a from r, (select d from s where e>7)";
////        String q3 ="select tt.a from (select a, b from R, (select d from S where e < 5) order by b desc) as tt";
////        String q3 = "select p from (select a, sum(b+c) as p from r group by a)";
////        String q3 = "select p.a, s.d from (select a,b,count(*) as q from r group by a,b order by b asc, a asc) as p ,s " ;
////        String q3 = "select min(a + c), max(b), sum(a+b), avg(b+c),sum(a+b+c) from R" ;
////        String q3 = "select a, b, c from R order by a asc";
////        String q3 = "select a from (select a from R) where a > 3 AND b < 7 AND c > 1";
////        String q3 = "select a from (select a,b,sum(b+c) as q from R,S where a>d group by a,b having sum(b+c) > 3 order by a desc) where a < 5";
////        String q4 = "select a, b from R group by a";
////        String q4 = "select r.a, s.d , t.d from R,S,T where r.a = s.d and r.a = t.d ";
//        String q4 = "select RA.a, RB.c, SA.d from R as RA, S as SA, R as RB, T where RA.a = RB.a and SA.d = RB.a and RA.a < T.d" ;
//        String q4 = "select a,b,c,s.d from r,s,t where s.d < 7 and r.a = t.d and r.b = s.e";

        String q4 = "CREATE TABLE CUSTOMER(CUSTKEY INT,NAME VARCHAR(25),ADDRESS VARCHAR(40),NATIONKEY INT,PHONE CHAR(15),ACCTBAL DECIMAL,MKTSEGMENT CHAR(10),COMMENT VARCHAR(117),PRIMARY KEY (CUSTKEY));" ;
        String q5 =  "CREATE TABLE SUPPLIER(SUPPKEY INT,NAME CHAR(25),ADDRESS VARCHAR(40),NATIONKEY INT,PHONE CHAR(15),ACCTBAL DECIMAL,COMMENT VARCHAR(101),PRIMARY KEY (SUPPKEY));" ;
        String q6 = "CREATE TABLE PARTSUPP(PARTKEY INT,SUPPKEY INT,AVAILQTY INT,SUPPLYCOST DECIMAL,COMMENT VARCHAR(199),PRIMARY KEY (PARTKEY,SUPPKEY));" ;
        String q7 = "CREATE TABLE NATION(NATIONKEY INT,NAME CHAR(25),REGIONKEY INT,COMMENT VARCHAR(152),PRIMARY KEY (NATIONKEY));";
        String q8 = " CREATE TABLE REGION(REGIONKEY INT,NAME CHAR(25),COMMENT VARCHAR(152),PRIMARY KEY (REGIONKEY));" ;
        String q9 = "CREATE TABLE LINEITEM(ORDERKEY INT,PARTKEY INT,SUPPKEY INT,LINENUMBER INT,QUANTITY DECIMAL,EXTENDEDPRICE DECIMAL,DISCOUNT DECIMAL,TAX DECIMAL,RETURNFLAG CHAR(1),LINESTATUS CHAR(1),SHIPDATE DATE,COMMITDATE DATE,RECEIPTDATE DATE,SHIPINSTRUCT CHAR(25),SHIPMODE CHAR(10),COMMENT VARCHAR(44),PRIMARY KEY (ORDERKEY,LINENUMBER));";
        String q10 = "CREATE TABLE ORDERS(ORDERKEY INT,CUSTKEY INT,ORDERSTATUS CHAR(1),TOTALPRICE DECIMAL,ORDERDATE DATE,ORDERPRIORITY CHAR(15),CLERK CHAR(15),SHIPPRIORITY INT,COMMENT VARCHAR(79),PRIMARY KEY (ORDERKEY));" ;
        String q11 = "CREATE TABLE PART(PARTKEY INT,NAME VARCHAR(55),MFGR CHAR(25),BRAND CHAR(10),TYPE VARCHAR(25),SIZE INT,CONTAINER CHAR(10),RETAILPRICE DECIMAL,COMMENT VARCHAR(23),PRIMARY KEY (PARTKEY));";

//        String q12 = "SELECT\n" +
//                "  LINEITEM.ORDERKEY,\n" +
//                "  SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS REVENUE, \n" +
//                "  ORDERS.ORDERDATE,\n" +
//                "  ORDERS.SHIPPRIORITY\n" +
//                "FROM\n" +
//                "  CUSTOMER,\n" +
//                "  ORDERS,\n" +
//                "  LINEITEM \n" +
//                "WHERE\n" +
//                "  CUSTOMER.MKTSEGMENT = 'HOUSEHOLD' AND CUSTOMER.CUSTKEY = ORDERS.CUSTKEY\n" +
//                "  AND LINEITEM.ORDERKEY = ORDERS.ORDERKEY \n" +
//                "  AND ORDERS.ORDERDATE < DATE('1995-03-26')\n" +
//                "  AND LINEITEM.SHIPDATE > DATE('1995-03-26')\n" +
//                "GROUP BY LINEITEM.ORDERKEY, ORDERS.ORDERDATE, ORDERS.SHIPPRIORITY \n" +
//                "ORDER BY REVENUE DESC, ORDERDATE\n" +
//                "LIMIT 10;";
//        String q12 = "SELECT\n" +
//                "  LINEITEM.ORDERKEY,\n" +
//                "  ORDERS.ORDERDATE,\n" +
//                "  ORDERS.SHIPPRIORITY\n" +
//                "FROM\n" +
//                "  CUSTOMER,\n" +
//                "  ORDERS,\n" +
//                "  LINEITEM \n" +
//                "WHERE\n" +
//                "  CUSTOMER.MKTSEGMENT = 'HOUSEHOLD' AND CUSTOMER.CUSTKEY = ORDERS.CUSTKEY\n" +
//                "  AND LINEITEM.ORDERKEY = ORDERS.ORDERKEY \n" +
//                "  AND ORDERS.ORDERDATE < DATE('1995-03-26')\n" +
//                "  AND LINEITEM.SHIPDATE > DATE('1995-03-26');";
//
//        String q12 = "SELECT\n" +
//                "  LINEITEM.ORDERKEY,\n" +
//                "  ORDERS.ORDERDATE,\n" +
//                "  ORDERS.SHIPPRIORITY\n" +
//                "FROM\n" +
//                "  ORDERS,\n" +
//                "  LINEITEM \n" +
//                "WHERE\n" +
//                "  LINEITEM.ORDERKEY = ORDERS.ORDERKEY \n" +
//                "  AND ORDERS.ORDERDATE < DATE('1995-03-26')\n" +
//                "  AND LINEITEM.SHIPDATE > DATE('1995-03-26');";
      /*  String q12 = "SELECT\n" +
                "  SUM(LINEITEM.EXTENDEDPRICE * (1 - LINEITEM.DISCOUNT)) AS REVENUE, \n" +
                "  AVG(LINEITEM.EXTENDEDPRICE * (1 - LINEITEM.DISCOUNT)) AS A, \n" +
                "  MIN(LINEITEM.EXTENDEDPRICE) AS B, \n" +
                "  AVG(LINEITEM.EXTENDEDPRICE) AS C \n" +
                "FROM\n" +
                "  REGION, NATION, CUSTOMER, ORDERS, LINEITEM, SUPPLIER\n" +
                "WHERE\n" +
                "  CUSTOMER.CUSTKEY = ORDERS.CUSTKEY\n" +
                "  AND LINEITEM.ORDERKEY = ORDERS.ORDERKEY\n" +
                "  AND LINEITEM.SUPPKEY = SUPPLIER.SUPPKEY\n" +
                "  AND CUSTOMER.NATIONKEY = NATION.NATIONKEY \n" +
                "  AND SUPPLIER.NATIONKEY = NATION.NATIONKEY\n" +
                "  AND NATION.REGIONKEY = REGION.REGIONKEY\n" +
                "  AND REGION.NAME = 'ASIA'\n" +
                "  AND ORDERS.ORDERDATE >= DATE('1994-01-01')\n" +
                "  AND ORDERS.ORDERDATE < DATE('1995-01-01')\n" +
                "ORDER BY REVENUE DESC;" ;
        String q13 = "SELECT\n" +
                " COUNT(*)\n" +
                "FROM\n" +
                "  REGION, NATION, CUSTOMER, ORDERS, LINEITEM, SUPPLIER\n" +
                "WHERE\n" +
                "  CUSTOMER.CUSTKEY = ORDERS.CUSTKEY\n" +
                "  AND LINEITEM.ORDERKEY = ORDERS.ORDERKEY\n" +
                "  AND LINEITEM.SUPPKEY = SUPPLIER.SUPPKEY\n" +
                "  AND CUSTOMER.NATIONKEY = NATION.NATIONKEY \n" +
                "  AND SUPPLIER.NATIONKEY = NATION.NATIONKEY\n" +
                "  AND NATION.REGIONKEY = REGION.REGIONKEY\n" +
                "  AND REGION.NAME = 'ASIA'\n" +
                "  AND ORDERS.ORDERDATE >= DATE('1994-01-01')\n" +
                "  AND ORDERS.ORDERDATE < DATE('1995-01-01')" ;
        String q14 = " SELECT ORDERDATE FROM (SELECT\n" +
                "  LINEITEM.ORDERKEY,\n" +
                "  SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS REVENUE, \n" +
                "  ORDERS.ORDERDATE,\n" +
                "  ORDERS.SHIPPRIORITY\n" +
                "FROM\n" +
                "  CUSTOMER,\n" +
                "  ORDERS,\n" +
                "  LINEITEM \n" +
                "WHERE\n" +
                "  CUSTOMER.MKTSEGMENT = 'HOUSEHOLD' AND CUSTOMER.CUSTKEY = ORDERS.CUSTKEY\n" +
                "  AND LINEITEM.ORDERKEY = ORDERS.ORDERKEY \n" +
                "  AND ORDERS.ORDERDATE < DATE('1995-03-26')\n" +
                "  AND LINEITEM.SHIPDATE > DATE('1995-03-26')\n" +
                "GROUP BY LINEITEM.ORDERKEY, ORDERS.ORDERDATE, ORDERS.SHIPPRIORITY \n" +
                "ORDER BY ORDERDATE\n" +
                "LIMIT 10);" ;

        String q15 = "SELECT LINEITEM.ORDERKEY, ORDERS.ORDERKEY  FROM LINEITEM, ORDERS WHERE LINEITEM.ORDERKEY = ORDERS.ORDERKEY";
        String q16 = "SELECT\n" +
                "NATION.NAME,\n" +
                "SUM(LINEITEM.EXTENDEDPRICE * (1 - LINEITEM.DISCOUNT)) AS REVENUE \n" +
                "FROM\n" +
                "REGION, NATION, CUSTOMER, ORDERS, LINEITEM, SUPPLIER\n" +
                "WHERE\n" +
                "CUSTOMER.CUSTKEY = ORDERS.CUSTKEY\n" +
                "AND LINEITEM.ORDERKEY = ORDERS.ORDERKEY\n" +
                "AND LINEITEM.SUPPKEY = SUPPLIER.SUPPKEY\n" +
                "AND CUSTOMER.NATIONKEY = NATION.NATIONKEY \n" +
                "AND SUPPLIER.NATIONKEY = NATION.NATIONKEY\n" +
                "AND NATION.REGIONKEY = REGION.REGIONKEY\n" +
                "AND REGION.NAME = 'EUROPE'\n" +
                "AND ORDERS.ORDERDATE >= DATE('1995-01-01')\n" +
                "AND ORDERS.ORDERDATE < DATE('1996-01-01')\n" +
                "GROUP BY NATION.NAME\n" +
                "ORDER BY REVENUE DESC;" ;
        String q17 = "SELECT\n" +
                "LINEITEM.ORDERKEY,\n" +
                "SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS REVENUE, \n" +
                "ORDERS.ORDERDATE,\n" +
                "ORDERS.SHIPPRIORITY\n" +
                "FROM\n" +
                "CUSTOMER,\n" +
                "ORDERS,\n" +
                "LINEITEM \n" +
                "WHERE\n" +
                "CUSTOMER.MKTSEGMENT = 'AUTOMOBILE' AND CUSTOMER.CUSTKEY = ORDERS.CUSTKEY\n" +
                "AND LINEITEM.ORDERKEY = ORDERS.ORDERKEY \n" +
                "AND ORDERS.ORDERDATE < DATE('1995-03-12')\n" +
                "AND LINEITEM.SHIPDATE > DATE('1995-03-12')\n" +
                "GROUP BY LINEITEM.ORDERKEY, ORDERS.ORDERDATE, ORDERS.SHIPPRIORITY \n" +
                "ORDER BY REVENUE DESC, ORDERDATE\n" +
                "LIMIT 10;";*/
        String q20 = "SELECT\n" +
                "LINEITEM.ORDERKEY,\n" +
                "SUM(LINEITEM.EXTENDEDPRICE*(1-LINEITEM.DISCOUNT)) AS REVENUE, \n" +
                "ORDERS.ORDERDATE,\n" +
                "ORDERS.SHIPPRIORITY\n" +
                "FROM\n" +
                "LINEITEM, \n" +
                "CUSTOMER,\n" +
                "ORDERS \n" +
                "WHERE\n" +
                "CUSTOMER.MKTSEGMENT = 'AUTOMOBILE' AND CUSTOMER.CUSTKEY = ORDERS.CUSTKEY\n" +
                "AND LINEITEM.ORDERKEY = ORDERS.ORDERKEY \n" +
                "AND ORDERS.ORDERDATE < DATE('1995-03-12')\n" +
                "AND LINEITEM.SHIPDATE > DATE('1995-03-12')\n" +
                "GROUP BY LINEITEM.ORDERKEY, ORDERS.ORDERDATE, ORDERS.SHIPPRIORITY \n" +
                "ORDER BY REVENUE DESC, ORDERDATE\n" +
                "LIMIT 10;";

        String q21 = "SELECT\n" +
                "NATION.NAME,\n" +
                "SUM(LINEITEM.EXTENDEDPRICE * (1 - LINEITEM.DISCOUNT)) AS REVENUE \n" +
                "FROM\n" +
                "REGION, NATION, CUSTOMER, ORDERS, LINEITEM, SUPPLIER\n" +
                "WHERE\n" +
                "CUSTOMER.CUSTKEY = ORDERS.CUSTKEY\n" +
                "AND LINEITEM.ORDERKEY = ORDERS.ORDERKEY\n" +
                "AND LINEITEM.SUPPKEY = SUPPLIER.SUPPKEY\n" +
                "AND CUSTOMER.NATIONKEY = NATION.NATIONKEY \n" +
                "AND SUPPLIER.NATIONKEY = NATION.NATIONKEY\n" +
                "AND NATION.REGIONKEY = REGION.REGIONKEY\n" +
                "AND REGION.NAME = 'EUROPE'\n" +
                "AND ORDERS.ORDERDATE >= DATE('1995-01-01')\n" +
                "AND ORDERS.ORDERDATE < DATE('1996-01-01')\n" +
                "GROUP BY NATION.NAME\n" +
                "ORDER BY REVENUE DESC;" ;
      /*  String q18 = "select r.a,s.d,t.d from r,s,t where r.a = s.d and r.a = t.d;" ;
        String q19 = "SELECT ORDERS.ORDERDATE FROM ORDERS WHERE ORDERS.ORDERDATE < DATE('1995-03-12') AND ORDERS.ORDERDATE > DATE('1995-02-29');" ;

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
                "LIMIT 10;";*/

        String a = "SELECT * FROM REGION, NATION WHERE NATION.REGIONKEY = REGION.REGIONKEY AND REGION.NAME = 'ASIA';";
//            String a = "SELECT * FROM NATION ORDER BY NATION.REGIONKEY;" ;
        for (int j = 0; j < args.length; j++) {
            if (args[j].equals("--on-disk")) {
                //inMem = false;
                break;
            }
        }

//
//        Thread thread = new Thread(){
//            public void run(){
//                while (true) {
//                    try {
//                        sleep(300);
//                        r.gc();
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        };
//
//        thread.start();

        //-Djava.util.Arrays.useLegacyMergeSort=true

        String q33 = "CREATE TABLE LINEITEM (ORDERKEY INT , PARTKEY INT , SUPPKEY INT , LINENUMBER INT , QUANTITY DECIMAL , EXTENDEDPRICE DECIMAL , DISCOUNT DECIMAL , TAX DECIMAL , RETURNFLAG CHAR (1) , LINESTATUS CHAR (1) , SHIPDATE DATE , COMMITDATE DATE , RECEIPTDATE DATE , SHIPINSTRUCT CHAR (25) , SHIPMODE CHAR (10) , COMMENT VARCHAR (44) , PRIMARY KEY (ORDERKEY, LINENUMBER));";

        String  q44 = "SELECT \n" +
                "SUM(LINEITEM.EXTENDEDPRICE*LINEITEM.DISCOUNT) AS REVENUE\n" +
                "FROM\n" +
                "LINEITEM\n" +
                "WHERE\n" +
                "LINEITEM.SHIPDATE >= DATE('1994-01-01')\n" +
                "AND LINEITEM.SHIPDATE < DATE ('1995-01-01')\n" +
                "AND LINEITEM.DISCOUNT > 0.08 AND LINEITEM.DISCOUNT < 0.1 \n" +
                "AND LINEITEM.QUANTITY < 24;";
        String q[] = {q4, q5, q6, q7, q8, q9, q10, q11, q44};
//        String  q45 = "SELECT * from LINEITEM";
//        String q[] = {q9 ,q44};
//        String q[] = {q44};
        int i = 0;

        IteratorBuilder iteratorBuilder = new IteratorBuilder();
        RAIterator rootIterator = null;

        while (i < q.length) {
            StringReader input = new StringReader(q[i]);
            CCJSqlParser parser = new CCJSqlParser(input);
            Statement query = parser.Statement();
            try {
                rootIterator = iteratorBuilder.parseStatement(query);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (rootIterator != null) {
//                long startTime = System.nanoTime();
                long startTime = System.currentTimeMillis();
                rootIterator = rootIterator.optimize(rootIterator);

                while (rootIterator.hasNext()) {
                    PrimitiveValue[] tuple = rootIterator.next();
                    if (tuple != null) {
                        for (int index = 0; index < tuple.length; index++) {
//                            printResult(tuple, index);
                            System.out.print(tuple[index].toRawString());
                            if (index != (tuple.length - 1))
                                System.out.print("|");
                        }
                        System.out.print("\n");

                    }
                }
                if (debugEnabled) {
                    long endTime = System.currentTimeMillis();
                    //System.out.println(startTime);
                    System.out.println(endTime - startTime);
                    //long freemem = Runtime.getRuntime().freeMemory();
                    //System.out.println(freemem);
                }
            }
            i++;
        }
    }
}