package iterators;

import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
/*import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;*/

import java.sql.SQLException;

public class FilterIterator implements RAIterator
{
   //region Variables

   //private static final Logger logger = LogManager.getLogger();
   private CommonLib commonLib = CommonLib.getInstance();

   private RAIterator child;
   private Expression expression;
   private ColumnDefinition[] columnDefinitions;
   private String tableName;
   private String tableAlias;

   //endregion

   //region Constructor

   public FilterIterator(RAIterator child,Expression expression)
   {

      this.child = child;
      this.expression = expression;
      this.columnDefinitions = child.getColumnDefinition() ;
      this.tableAlias = child.getTableAlias() ;
      this.tableName = child.getTableName() ;

   }

   //endregion

   //region Iterator methods

   @Override
   public boolean hasNext() throws Exception
   {
      return child.hasNext();
   }

   @Override
   public PrimitiveValue[] next() throws Exception
   {
      PrimitiveValue[] tuple = child.next();
      if (tuple == null)
         return null;
      try {
         PrimitiveValueWrapper[] wrappedTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(tuple, this.columnDefinitions, this.tableName);
         if (commonLib.eval(expression,wrappedTuple).getPrimitiveValue().toBool())
            return tuple;
         return null;
      } catch (SQLException e) {
         //logger.error("Exception in FilterIterator eval().");
         throw e;
      }
   }

   @Override
   public void reset() throws Exception
   {
      child.reset();
   }

   @Override
   public RAIterator getChild() {
      return this.child ;
   }

   @Override
   public void setChild(RAIterator child) {
      this.child = child ;
   }

   @Override
   public ColumnDefinition[] getColumnDefinition() {
      return this.columnDefinitions;
   }

   @Override
   public void setColumnDefinition(ColumnDefinition[] columnDefinition) {
      this.columnDefinitions = columnDefinition ;
   }

   @Override
   public void setTableName(String tableName) {
      this.tableName = tableName;
   }

   @Override
   public String getTableName() {
      return this.tableName;
   }

   @Override
   public void setTableAlias(String tableAlias) {
      this.tableAlias = tableAlias ;
   }

   @Override
   public String getTableAlias() {
      return this.tableAlias;
   }
   //endregion
}
