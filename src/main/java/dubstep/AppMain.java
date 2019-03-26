package dubstep;

import builders.IteratorBuilder;
import helpers.PrimitiveValueWrapper;
import iterators.RAIterator;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;

import java.io.StringReader;

public class AppMain
{

    public static boolean inMem = false;

    public static void main(String[] args) throws Exception
    {

        String q1 = "CREATE TABLE R(a int NOT NULL, b int, c int)";
        String q2 = "CREATE TABLE S(d int NOT NULL, e int, f int)";
        //String q3 = "select * from R UNION ALL select a from R";
//        String q3 = "select a , sum(b+c), count(c), min(b) as d from R where a != 170 group by a";
//        String q3 = "select R.a, R.b "|"from R,S as TT where R.a = 11";
//        String q3 = "select count(1), avg(b+d), sum(a+c) from R, S";
//        String q3 = "select c, d, sum(a+b) from R, S where c != 78 group by c, d order by c, d desc";
//        String q3 = "select c, d, sum(a+b) from R, S where c != 78 group by c, d order by c, d desc";
        String q3 = "select c,a+b from R" ;


        for(int j = 0; j < args.length; j++){
            if(args[j].equals("--in-mem")){
                inMem = true;
                break;
            }
        }

        String q[] = {q1,q2,q3};
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
            }

            i++;
        }

    }

}