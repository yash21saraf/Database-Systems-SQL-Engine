package dubstep;

import builders.IteratorBuilder;
import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import helpers.Schema;
import iterators.RAIterator;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.io.StringReader;

public class Main {

    public static ColDataType colDataTypes[];
    public static boolean inMem = false;
    static boolean debugEnabled = false;

    public static void main(String[] args) throws Exception
    {

        String q1 = "CREATE TABLE R(a int NOT NULL, b int, c int)";
        String q2 = "CREATE TABLE S(d int NOT NULL, e int, f int)";
//        String q3 = "select * from R UNION ALL select a from R";
//        String q3 = "select A.a,b,c from R as A";
//        String q3 = "select a , sum(b+c), count(c), min(b) from R where a != 170 group by a";
//        String q3 = "select R.a, R.b "|"from R,S as TT where R.a = 11";
//        String q3 = "select count(1), avg(b+d), sum(a+c) from R, S";
//        String q3 = "select a,b,* from R";
//        String q3 = "select c, d, sum(a+b) from R, S where c != 78 group by c, d order by c, d desc";
//        String q3 = "select c, d, sum(a+b) from R, S where c != 9 group by c, d having sum(a+b) <> 35 order by c desc";
//        String q3 = "select a, a+b from R,S" ;
//        String q3 = "select * from r where a = (select a from s where c>5)" ; //TODO: Manage in expressions, Manage subqueries in where clause
//        String q3 = "select a from r, (select d from s where e>7)";
//        String q3 ="select tt.a from (select a, b from R, (select d from S where e < 5) order by b desc) as tt";
//        String q3 = "select p from (select a, sum(b+c) as p from r group by a)";
        String q3 = "select p.a, s.d from (select a,b,count(*) as q from r group by a,b order by b asc, a asc) as p ,s " ;
//        String q3 = "select min(a + c), max(b), sum(a+b), avg(b+c),sum(a+b+c) from R" ;
//        String q3 = "select a, b, c from R order by a asc";
//        String q3 = "select a from (select a from R) where a > 3 AND b < 7 AND c > 1";

        for (int j = 0; j < args.length; j++) {
            if (args[j].equals("--on-disk")) {
                //inMem = false;
                break;
            }
        }

        String q[] = {q1, q2, q3};
        int i = 0;

        IteratorBuilder iteratorBuilder = new IteratorBuilder();
        RAIterator rootIterator = null;

        while (i < 3) {
            StringReader input = new StringReader(q[i].toLowerCase());
            CCJSqlParser parser = new CCJSqlParser(input);
            Statement query = parser.Statement();
            try {
                rootIterator = iteratorBuilder.parseStatement(query);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (rootIterator != null) {
                long startTime = System.nanoTime();
                //rootIterator = rootIterator.optimize(rootIterator);
                setDataType(rootIterator);

                while (rootIterator.hasNext()) {
                    PrimitiveValue[] tuple = rootIterator.next();
                    if (tuple != null) {
                        for (int index = 0; index < tuple.length; index++) {
                            printResult(tuple, index);
                            //System.out.print(tuple[index].toRawString());
                            if (index != (tuple.length - 1))
                                System.out.print("|");
                        }
                        System.out.print("\n");
                    }
                }
                if (debugEnabled) {
                    long endTime = System.nanoTime();
                    System.out.println(endTime - startTime);
                    long freemem = Runtime.getRuntime().freeMemory();
                    System.out.println(freemem);
                }
            }
            i++;
        }
    }

    private static void printResult(PrimitiveValue[] tuple, int index) {

        String value = tuple[index].toRawString();
        String datatype = colDataTypes[index].getDataType();
        String val = "";
        if (datatype.equals("int")) {
            try {
                val = value.substring(0, value.indexOf("."));
            } catch (Exception e) {
                val = value;
            }
        }
        System.out.print(val);
    }

    private static void setDataType(RAIterator rootIterator) {

        Schema[] schemas = rootIterator.getSchema();
        colDataTypes = new ColDataType[schemas.length];

        for (int i = 0; i < schemas.length; i++) {
            colDataTypes[i] = schemas[i].getColumnDefinition().getColDataType();
        }
    }

}