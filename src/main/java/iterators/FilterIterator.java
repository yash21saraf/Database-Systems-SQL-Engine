package iterators;

import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import net.sf.jsqlparser.expression.Expression;

import java.sql.SQLException;

public class FilterIterator implements RAIterator
{
   //region Variables

   private CommonLib commonLib = CommonLib.getInstance();

   private RAIterator child;
   private Expression expression;

   //endregion

   //region Constructor

   public FilterIterator(RAIterator child,Expression expression)
   {

      this.child = child;
      this.expression = expression;

   }

   //endregion

   //region Iterator methods

   @Override
   public boolean hasNext() throws Exception
   {
      return child.hasNext();
   }

   @Override
   public PrimitiveValueWrapper[] next() throws Exception
   {
      PrimitiveValueWrapper[] tuple = child.next();
      if (tuple == null)
         return null;
      try {
         if (commonLib.eval(expression,tuple).getPrimitiveValue().toBool())
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

   //endregion
}
