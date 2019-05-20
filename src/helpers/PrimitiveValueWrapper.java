package helpers;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class PrimitiveValueWrapper
{

   //region Variables

   private PrimitiveValue primitiveValue;
   private ColumnDefinition columnDefinition;
   private String tableName;

   //endregion

   //region Accessor methods

   public PrimitiveValue getPrimitiveValue()
   {
      return primitiveValue;
   }

   public void setPrimitiveValue(PrimitiveValue primitiveValue)
   {
      this.primitiveValue = primitiveValue;
   }

   public ColumnDefinition getColumnDefinition()
   {
      return columnDefinition;
   }

   public void setColumnDefinition(ColumnDefinition columnDefinition)
   {
      this.columnDefinition = columnDefinition;
   }

   public String getTableName()
   {
      return tableName;
   }

   public void setTableName(String tableName)
   {
      this.tableName = tableName;
   }

   //endregion

   //region Helper methods

   public String getWholeColumnName()
   {

      String columnWholeName = null;
      if (tableName != null && tableName.length() != 0)
         columnWholeName = tableName + "." + columnDefinition.getColumnName();
      else
         columnWholeName = columnDefinition.getColumnName();

      return columnWholeName;

   }

   //endregion

}
