package iterators;

import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import helpers.Schema;
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
   private Schema[] schema ;

   //endregion

   //region Constructor

   public FilterIterator(RAIterator child,Expression expression)
   {
      this.child = child;
      this.expression = expression;
      this.schema = child.getSchema();
   }

   //endregion

   //region Iterator methods


   public Expression getExpression() {
      return expression;
   }

   public void setExpression(Expression expression){
      this.expression = expression;
   }

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
         PrimitiveValueWrapper[] wrappedTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(tuple, this.schema);
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
   public Schema[] getSchema() {
      return this.schema;
   }

   @Override
   public void setSchema(Schema[] schema) {
      this.schema = schema ;
   }
   //endregion
}
