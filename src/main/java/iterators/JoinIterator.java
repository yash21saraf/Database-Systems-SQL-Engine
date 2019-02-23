package iterators;

import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
/*import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;*/

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;

public class JoinIterator implements RAIterator
{
   //region Variables

   //private static final Logger logger = LogManager.getLogger();
   private CommonLib commonLib = CommonLib.getInstance();

   private RAIterator leftChild;
   private RAIterator rightChild;
   private Expression onExpression;

   private PrimitiveValueWrapper[] leftTuple;
   private PrimitiveValueWrapper[] rightTuple;

   //endregion

   //region Constructor

   public JoinIterator(RAIterator leftChild,RAIterator rightChild,Expression onExpression)
   {

      this.leftChild = leftChild;
      this.rightChild = rightChild;
      this.onExpression = onExpression;

   }

   //endregion

   //region Iterator methods

   @Override
   public boolean hasNext() throws Exception
   {
      try {
         if (!rightChild.hasNext())
            return leftChild.hasNext();
         return true;
      } catch (Exception e) {
         //logger.error("Error in reading from right table of join.");
         throw e;
      }

   }

   @Override
   public PrimitiveValueWrapper[] next() throws Exception
   {
      try {
         if (leftTuple == null)
            if (leftChild.hasNext())
               leftTuple = leftChild.next();
         if (!rightChild.hasNext()) {
            rightChild.reset();
            leftTuple = leftChild.next();
         }
         rightTuple = rightChild.next();
         if (onExpression != null) {
            if (commonLib.eval(onExpression,rightTuple,leftTuple).getPrimitiveValue().toBool()) {
               return CommonLib.concatArrays(leftTuple,rightTuple);
            }
         } else {
            return CommonLib.concatArrays(leftTuple,rightTuple);
         }
         return null;
      } catch (Exception e) {
         //logger.error("Error in JoinIterator.next() during rightChild.hasNext() check.");
         throw e;
      }
   }

   @Override
   public void reset() throws Exception
   {
      leftChild.reset();
      rightChild.reset();
   }

   //endregion
}
