import builders.IteratorBuilder;
import helpers.PrimitiveValueWrapper;
import iterators.RAIterator;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;

import java.io.StringReader;

public class AppMain
{

   public static void main(String[] args) throws Exception
   {

      String q1 = "CREATE TABLE R(a int NOT NULL, b int, c int)";
      String q2 = "CREATE TABLE S(a int NOT NULL, e int, f int)";
      String q3 = "select q.firstA from (select first.a as firstA, second.a AS secondA, b from R as first join S as second on first.b = second.e) as q";
//      String q3 = "select a+b from R";

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
               PrimitiveValueWrapper[] tuple = rootIterator.next();
               if (tuple != null) {
                  for (int index = 0; index < tuple.length; index++) {
                     System.out.print(tuple[index].getPrimitiveValue().toRawString());
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
