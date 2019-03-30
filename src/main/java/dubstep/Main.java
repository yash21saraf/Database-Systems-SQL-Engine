package dubstep;

import builders.IteratorBuilder;
import helpers.Schema;
import iterators.RAIterator;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;

public class Main {

    public static ColDataType colDataTypes[];
    public static boolean inMem = true;

    public static void main(String[] args) throws Exception {

        for (int j = 0; j < args.length; j++) {
            if (args[j].equals("--on-disk")) {
                inMem = false;
                break;
            }
        }


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
                    rootIterator = rootIterator.optimize(rootIterator);
                    setDataType(rootIterator);
                    while (rootIterator.hasNext()) {
                        PrimitiveValue[] tuple = rootIterator.next();
                        if (tuple != null) {
                            for (int index = 0; index < tuple.length; index++) {
                                printResult(tuple, index);
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
