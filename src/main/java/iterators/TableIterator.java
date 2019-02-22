package iterators;

import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class TableIterator implements RAIterator
{

   //region Variables

   private static final Logger logger = LogManager.getLogger();
   private CommonLib commonLib = CommonLib.getInstance();

   private static final String TABLE_DIRECTORY = "D:/";

   private ColumnDefinition[] columnDefinitions;
   private String tableName;
   private String tableAlias;
   private BufferedReader br;
   private PrimitiveValueWrapper[] currentLine;
   private PrimitiveValueWrapper[] nextLine;
   private boolean hasNextChecked = false;
   private boolean hasNextValue = false;

   //endregion

   //region Constructor

   public TableIterator(String tableName,String tableAlias,ColumnDefinition[] columnDefinitions) throws Exception
   {
      this.columnDefinitions = columnDefinitions;
      this.tableName = tableName;
      this.tableAlias = tableAlias;

      try {
         br = new BufferedReader(new FileReader(TABLE_DIRECTORY + tableName + ".csv"));
      } catch (FileNotFoundException e) {
         logger.error("Exception in reading from file for table: {}.",tableName);
         throw e;
      }
   }

   //endregion

   //region Iterator methods

   @Override
   public boolean hasNext() throws Exception
   {
      try {
         if (!hasNextChecked) {
            hasNextChecked = true;
            if ((nextLine = commonLib.convertTupleStringToPrimitiveValueWrapperArray(br.readLine(),columnDefinitions,tableAlias)) != null) {
               hasNextValue = true;
               return true;
            }
            hasNextValue = false;
            return false;
         } else {
            return hasNextValue;
         }
      } catch (IOException e) {
         logger.error("IOException in hasNext() of table: {}.", tableName);
         throw e;
      } catch (Exception e) {
         logger.error("Error in table: {} " + e.getMessage(), tableName);
         throw e;
      }
   }

   @Override
   public PrimitiveValueWrapper[] next()
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
         nextLine = commonLib.convertTupleStringToPrimitiveValueWrapperArray(br.readLine(),columnDefinitions,tableAlias);
      } catch (FileNotFoundException e) {
         logger.error("Exception in reading from file for table: {}.",tableName);
         throw e;
      } catch (IOException e) {
         logger.error("Exception in reading line from file for table: {}.",tableName);
         throw e;
      } catch (Exception e) {
         throw e;
      }
   }

   //endregion

}
