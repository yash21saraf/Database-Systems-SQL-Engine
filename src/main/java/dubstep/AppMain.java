package dubstep;

import builders.IteratorBuilder;
import helpers.PrimitiveValueWrapper;
import iterators.RAIterator;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;

public class AppMain {

    public static boolean inMem = true;

    public static void main(String[] args) throws Exception {


        System.out.print("$> ");

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String in = "";
        String line;

        while ((line = reader.readLine()) != null) {
            in += line;
            if(line.charAt(line.length()-1) == ';')
                break;
        }

        //System.out.println(in);

        StringReader input = new StringReader(in);

        CCJSqlParser parser = new CCJSqlParser(input);

        //System.out.println("$> "); // print a prompt
        Statement query;
        IteratorBuilder iteratorBuilder = new IteratorBuilder();
        RAIterator rootIterator = null;

        while ((query = parser.Statement()) != null) {

            try {
                rootIterator = iteratorBuilder.parseStatement(query);

                if (rootIterator != null) {
                    while (rootIterator.hasNext()) {
                        PrimitiveValue[] tuple = rootIterator.next();
                        if (tuple != null) {
                            for (int index = 0; index < tuple.length; index++) {
                                System.out.print(tuple[index].toRawString());
                                if (index != (tuple.length - 1)) {
                                    //cnt++;
                                    System.out.print("|");
                                }
                            }
                            System.out.print("\n");
                        }

                    }
                }

                System.out.print("$> ");
                reader = new BufferedReader(new InputStreamReader(System.in));

                in ="";
                while ((line = reader.readLine()) != null) {
                    in += line+ " ";
                    if(line.charAt(line.length()-1) == ';')
                        break;
                }


                input = new StringReader(in);
                parser = new CCJSqlParser(input);

            } catch (Exception e) {
                e.printStackTrace();
                System.out.print("$> ");
                reader = new BufferedReader(new InputStreamReader(System.in));
                while ((line = reader.readLine()) != null) {
                    in += line;
                    if(line.charAt(line.length()-1) == ';')
                        break;
                }

                input = new StringReader(in);

                parser = new CCJSqlParser(input);
            }
        }
    }

}
