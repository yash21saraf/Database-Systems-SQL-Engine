package helpers;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

public class CommonLib
{

   //region Variables

   private static final Logger logger = LogManager.getLogger();

   private Eval eval = new Eval()
   {
      @Override
      public PrimitiveValue eval(Column column) throws SQLException
      {
         for (PrimitiveValueWrapper[] tuple : tuples) {
            for (PrimitiveValueWrapper value : tuple) {
               if (column.getWholeColumnName().equals(value.getWholeColumnName()))
                  return value.getPrimitiveValue();
            }
         }
         logger.error("No column with name: {}",column.getColumnName());
         throw new SQLException("No column with name: " + column.getColumnName() + ".");
      }
   };

   private PrimitiveValueWrapper[] tuple;
   private ArrayList<PrimitiveValueWrapper[]> tuples = new ArrayList<>();

   //endregion

   //region Singleton implementation

   private static CommonLib instance;

   private CommonLib()
   {
   }

   public static CommonLib getInstance()
   {

      if (instance == null) {
         synchronized (CommonLib.class) {
            if (instance == null) {
               instance = new CommonLib();
            }
         }
      }

      return instance;

   }

   //endregion

   //region PrimitiveValue conversion methods

   public PrimitiveValueWrapper[] convertTupleStringToPrimitiveValueWrapperArray(String tupleString,ColumnDefinition[] columnDefinitions,String tableName) throws Exception
   {

      if (tupleString == null)
         return null;

      String[] tupleArray = tupleString.split("\\|");
      PrimitiveValueWrapper[] convertedTuple = new PrimitiveValueWrapper[tupleArray.length];


      for (int index = 0; index < tupleArray.length; index++) {
         PrimitiveValueWrapper convertedValue = new PrimitiveValueWrapper();
         convertedValue.setPrimitiveValue(convertToPrimitiveValue(tupleArray[index],columnDefinitions[index].getColDataType().getDataType()));
         if (convertedValue.getPrimitiveValue() != null) {
            convertedValue.setColumnDefinition(columnDefinitions[index]);
            convertedValue.setTableName(tableName);
            convertedTuple[index] = convertedValue;
         } else {
            logger.error("Invalid columnType: {} at columnName: {}.",columnDefinitions[index].getColDataType().getDataType(),columnDefinitions[index].getColumnName());
            throw new Exception("Invalid columnType.");
         }
      }
      return convertedTuple;

   }

   public PrimitiveValue convertToPrimitiveValue(String value,String dataType)
   {
      if (("STRING").equals(dataType.toUpperCase()))
         return new StringValue(value);
      if (("VARCHAR").equals(dataType.toUpperCase()))
         return new StringValue(value);
      if (("CHAR").equals(dataType.toUpperCase()))
         return new StringValue(value);
      if (("INT").equals(dataType.toUpperCase()))
         return new LongValue(value);
      if (("DECIMAL").equals(dataType.toUpperCase()))
         return new DoubleValue(value);
      if (("DATE").equals(dataType.toUpperCase()))
         return new DateValue(value);
      return null;
   }

   //endregion

   //region Eval methods

   public PrimitiveValueWrapper eval(Expression expression,PrimitiveValueWrapper[] tuple) throws SQLException
   {
      try {
         PrimitiveValueWrapper evaluatedExpression = new PrimitiveValueWrapper();
         this.tuples.add(tuple);
         evaluatedExpression.setPrimitiveValue(eval.eval(expression));
         this.tuples.remove(tuple);
         return evaluatedExpression;
      } catch (SQLException e) {
         logger.error("Exception in eval() call.");
         throw e;
      }
   }

   public PrimitiveValueWrapper eval(Expression expression,PrimitiveValueWrapper[]... tuples) throws SQLException
   {
      try {
         PrimitiveValueWrapper evaluatedExpression = new PrimitiveValueWrapper();
         this.tuples.addAll(Arrays.asList(tuples));
         evaluatedExpression.setPrimitiveValue(eval.eval(expression));
         this.tuples.removeAll(Arrays.asList(tuples));
         return evaluatedExpression;
      } catch (SQLException e) {
         logger.error("Exception in eval() call.");
         throw e;
      }
   }

   //endregion

   //region Helper methods

   public static <T> Object castAs(T queryPart,Class<? extends T> clazz)
   {
      if (clazz.isInstance(queryPart))
         return clazz.cast(queryPart);
      return null;
   }

   public static <T> T[] concatArrays(T[] first, T[]... rest) {
      int totalLength = first.length;
      for (T[] array : rest) {
         totalLength += array.length;
      }
      T[] result = Arrays.copyOf(first, totalLength);
      int offset = first.length;
      for (T[] array : rest) {
         System.arraycopy(array, 0, result, offset, array.length);
         offset += array.length;
      }
      return result;
   }

   //endregion

}
