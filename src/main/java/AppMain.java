import builders.IteratorBuilder;
import helpers.PrimitiveValueWrapper;
import iterators.RAIterator;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;

public class AppMain
{

   public static void main(String[] args) throws Exception
   {
      //long start = System.currentTimeMillis();




         //region Manual logic

     /* String q1 = "CREATE TABLE PLAYERS(ID string, FIRSTNAME string, LASTNAME string, FIRSTSEASON int, LASTSEASON int, WEIGHT int, BIRTHDATE date);";
      String q2 = "CREATE TABLE S(a int NOT NULL, e int, f int)";
//      String q3 = "select b from R UNION ALL select a from R";
//      String q3 = "select a+b from R join S on R.a = S.a where R.a > 13";

 CREATE TABLE R(a int NOT NULL, b int, c int);
 CREATE TABLE S(d int NOT NULL, e int, f int);

      String q3 = "SELECT P1_FIRSTNAME, P1_LASTNAME, \n" +
              "       P2_FIRSTNAME, P2_LASTNAME \n" +
              "FROM (\n" +
              "  SELECT P1.FIRSTNAME AS P1_FIRSTNAME, P1.LASTNAME AS P1_LASTNAME,\n" +
              "         P2.FIRSTNAME AS P2_FIRSTNAME, P2.LASTNAME AS P2_LASTNAME,\n" +
              "         P1.FIRSTSEASON AS P1_FIRSTSEASON, P1.LASTSEASON AS P1_LASTSEASON,\n" +
              "         P2.FIRSTSEASON AS P2_FIRSTSEASON, P2.LASTSEASON AS P2_LASTSEASON\n" +
              "    FROM PLAYERS P1, PLAYERS P2 \n" +
              "  WHERE P1.ID<>P2.ID\n" +
              "  ) SUB_Q \n" +
              "WHERE P1_FIRSTSEASON<P2_FIRSTSEASON \n" +
              "  AND P1_LASTSEASON>P2_LASTSEASON;\n";

      String q[] = {q1,q2,q3};*/
      //endregion
      /*int i = 0;
      int cnt = 0;*/


      System.out.print("$> ");

      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
      StringReader input = new StringReader(reader.readLine().toLowerCase());

      CCJSqlParser parser = new CCJSqlParser(input);

      //System.out.println("$> "); // print a prompt
      Statement query;
      IteratorBuilder iteratorBuilder = new IteratorBuilder();
      RAIterator rootIterator = null;

      while((query = parser.Statement()) != null){

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
                     if (index != (tuple.length - 1)) {
                        //cnt++;
                        System.out.print("|");
                     }
                  }
                  System.out.print("\n");
               }

            }
         }

         //System.out.print("$> ");
         //reader = new BufferedReader(new InputStreamReader(System.in));
         //input = new StringReader(reader.readLine().toLowerCase());
         input = new StringReader("select a from R");
         parser = new CCJSqlParser(input);

         //i++;
      }

     /* long end = System.currentTimeMillis();

      System.out.println(end - start);
      System.out.println(cnt);*/
   }

}
