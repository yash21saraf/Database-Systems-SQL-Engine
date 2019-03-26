package iterators;

import com.sun.xml.internal.ws.api.server.SDDocument;
import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import helpers.Schema;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
/*import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;*/

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class TableIterator implements RAIterator
{

   //region Variables

   //private static final Logger logger = LogManager.getLogger();
   private CommonLib commonLib = CommonLib.getInstance();

   private static final String TABLE_DIRECTORY = "/home/yash/Desktop/Databases/data/";


   private ColumnDefinition[] columnDefinitions;
   private String tableName;
   private String tableAlias;
   private BufferedReader br;
   private PrimitiveValue[] currentLine;
   private PrimitiveValue[] nextLine;
   private Schema[] schema ;
   private boolean hasNextChecked = false;
   private boolean hasNextValue = false;

   //endregion

   //region Constructor

   public TableIterator(String tableName,String tableAlias,ColumnDefinition[] columnDefinitions) throws Exception
   {
      this.columnDefinitions = columnDefinitions;
      this.tableName = tableName;
      this.tableAlias = tableAlias;
      this.schema = createSchema(columnDefinitions, tableAlias);

      try {
         br = new BufferedReader(new FileReader(TABLE_DIRECTORY + tableName + ".csv"));

      } catch (FileNotFoundException e) {
         //logger.error("Exception in reading from file for table: {}.",tableName);
         throw e;
      }
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
      return convertedTuple;
   }


   //endregion

   //region Iterator methods

   @Override
   public boolean hasNext() throws Exception
   {
      try {
         if (!hasNextChecked) {
            hasNextChecked = true;
            if ((nextLine = commonLib.covertTupleToPrimitiveValue(br.readLine(),columnDefinitions)) != null) {
               hasNextValue = true;
               return true;
            }
            hasNextValue = false;
            return false;
         } else {
            return hasNextValue;
         }
      }
      catch (IOException e) {
         //logger.error("IOException in hasNext() of table: {}.", tableName);
         throw e;
      } catch (Exception e) {
         //logger.error("Error in table: {} " + e.getMessage(), tableName);
         throw e;
      }
   }

   @Override
   public PrimitiveValue[] next()
   {
      currentLine = nextLine;
      hasNextChecked = false;
      return currentLine;
   }

   @Override
   public void reset() throws Exception
   {
      nextLine = null;
      currentLine = null;
      try {
         br.close();
         br = new BufferedReader(new FileReader(TABLE_DIRECTORY + tableName + ".csv"));
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


   //endregion

}
