package iterators;

import builders.IteratorBuilder;
import dubstep.Main;
import helpers.CommonLib;
import helpers.Schema;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.io.*;

public class TableIterator implements RAIterator
{

   //region Variables

   //private static final Logger logger = LogManager.getLogger();
   private CommonLib commonLib = CommonLib.getInstance();

//   public static final String TABLE_DIRECTORY = "/home/yash/Desktop/Databases/data/";
   public static final String TABLE_DIRECTORY = "/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/";
//   public static final String TABLE_DIRECTORY = "/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/";
   public static String extension = ".csv" ;
//   public static final String TABLE_DIRECTORY = "data/";

   private ColumnDefinition[] columnDefinitions;
   private String tableName;
   private String tableAlias;
   private BufferedReader br;
   private FileReader fileReader;
   private PrimitiveValue[] currentLine;
   private PrimitiveValue[] nextLine;
   private Schema[] schema ;
   private boolean hasNextChecked = false;
   private boolean hasNextValue = false;
   private int cnter = 0;


   //endregion

   //region Constructor

   public TableIterator(String tableName,String tableAlias,ColumnDefinition[] columnDefinitions) throws Exception
   {
      this.columnDefinitions = columnDefinitions;
      this.tableName = tableName;
      this.tableAlias = tableAlias;

      if(this.tableAlias == null)
         this.schema = createSchema(columnDefinitions, tableName);
      else{
         this.schema = createSchema(columnDefinitions,this.tableAlias);
         addOriginalSchema(columnDefinitions, tableName);
      }

      try {
         File file = new File(TABLE_DIRECTORY + tableName + extension) ;
         fileReader = new FileReader(TABLE_DIRECTORY + tableName + extension);
         br = new BufferedReader(fileReader, 1000);


      } catch (FileNotFoundException e) {
         throw e;
      }
   }

   private void addOriginalSchema(ColumnDefinition[] columnDefinitions, String tableName) throws Exception
   {

      Schema[] convertedTuple = new Schema[columnDefinitions.length];

      for (int index = 0; index < columnDefinitions.length; index++) {
         Schema convertedValue = new Schema();
         convertedValue.setColumnDefinition(columnDefinitions[index]);
         convertedValue.setTableName(tableName);
         convertedTuple[index] = convertedValue;

      }
      IteratorBuilder.iteratorSchemas.put(tableName, convertedTuple) ;
   }

   private Schema[] createSchema(ColumnDefinition[] columnDefinitions, String tableName) throws Exception
   {

      if (columnDefinitions == null)
         return null;

      Schema[] convertedTuple = new Schema[columnDefinitions.length];

      for (int index = 0; index < columnDefinitions.length; index++) {
         Schema convertedValue = new Schema();
         convertedValue.setColumnDefinition(columnDefinitions[index]);
         convertedValue.setTableName(tableName);
         convertedTuple[index] = convertedValue;

      }
      IteratorBuilder.iteratorSchemas.put(tableName, convertedTuple);
      return convertedTuple;
   }


   //endregion

   //region Iterator methods

   @Override
   public boolean hasNext() throws Exception
   {
//      if((cnter % 20000) == 0)
//         System.out.println("asdfasf");

      try {
         if (!hasNextChecked) {
            hasNextChecked = true;
            if ((nextLine = commonLib.covertTupleToPrimitiveValue(br.readLine(),columnDefinitions)) != null) {
               cnter++;
               hasNextValue = true;
               return true;
            }
            hasNextValue = false;
            fileReader.close();
            br.close();
            return false;
         } else {
            return hasNextValue;
         }
      }
      catch (IOException e) {
         throw e;
      } catch (Exception e) {
         throw e;
      }
   }

   @Override
   public PrimitiveValue[] next()
   {
      currentLine = nextLine;
      nextLine = null;
      hasNextChecked = false;
      return currentLine;
   }

   @Override
   public void reset() throws Exception
   {
      nextLine = null;
      currentLine = null;
      cnter = 0;
      try {
         br.close();
         fileReader.close();
         fileReader = new FileReader(TABLE_DIRECTORY + tableName + extension);
         br = new BufferedReader(fileReader, 1000);
         cnter++;
         nextLine = commonLib.covertTupleToPrimitiveValue(br.readLine(),columnDefinitions);
      } catch (FileNotFoundException e) {
         //logger.error("Exception in reading from file for table: {}.",tableName);
         throw e;
      } catch (IOException e) {
         //logger.error("Exception in reading line from file for table: {}.",tableName);
         throw e;
      } catch (Exception e) {
         throw e;
      }
   }

   @Override
   public RAIterator getChild() {
      return null;
   }

   @Override
   public void setChild(RAIterator child) {
   }

   @Override
   public Schema[] getSchema() {
      return this.schema;
   }

   @Override
   public void setSchema(Schema[] schema) {
      this.schema = schema;
   }

   @Override
   public RAIterator optimize(RAIterator iterator)
   {
      return iterator;
   }

   //endregion

}
