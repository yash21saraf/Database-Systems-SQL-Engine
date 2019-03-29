package iterators;

import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import helpers.Schema;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
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

   private ColumnDefinition[] columnDefinitions;

   private PrimitiveValue[] leftTuple;
   private PrimitiveValue[] rightTuple;
   private Schema[] schema ;

   //endregion

   //region Constructor

   public JoinIterator(RAIterator leftChild,RAIterator rightChild,Expression onExpression)
   {

      this.leftChild = leftChild;
      this.rightChild = rightChild;
      this.onExpression = onExpression;
      this.schema = createSchema(leftChild.getSchema(), rightChild.getSchema()) ;
   }

   private Schema[] createSchema(Schema[] leftSchema, Schema[] rightSchema) {
      return CommonLib.concatArrays(leftSchema, rightSchema) ;
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
   public PrimitiveValue[] next() throws Exception
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
         if(rightTuple == null || leftTuple == null){
            return null ;
         }
         if (onExpression != null) {
            PrimitiveValueWrapper[] wrappedLeftTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(leftTuple, leftChild.getSchema());
            PrimitiveValueWrapper[] wrappedRightTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(rightTuple, rightChild.getSchema());
            if (commonLib.eval(onExpression,wrappedRightTuple, wrappedLeftTuple).getPrimitiveValue().toBool()) {
               return CommonLib.concatArrays(leftTuple,rightTuple);
            }
         } else if(rightTuple != null && leftTuple != null){
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

   @Override
   public RAIterator getChild() {
      return leftChild;
   }

   @Override
   public void setChild(RAIterator child) {
      this.leftChild = child ;
   }

   @Override
   public Schema[] getSchema() {
      return this.schema ;
   }

   @Override
   public void setSchema(Schema[] schema) {
      this.schema = schema ;
   }

   public void setRightChild(RAIterator rightChild) {
      this.rightChild = rightChild;
   }

   public RAIterator getRightChild() {
      return this.rightChild;
   }

   public Expression getOnExpression() {
      return this.onExpression;
   }

   public void setOnExpression(Expression onExpression) {
      this.onExpression = onExpression;
   }

   @Override
   public RAIterator optimize(RAIterator iterator)
   {
      JoinIterator joinIterator;

      if ((joinIterator = (JoinIterator) CommonLib.castAs(iterator,JoinIterator.class)) != null) {
         RAIterator leftChild = joinIterator.getChild();
         RAIterator rightChild = joinIterator.getRightChild();
         leftChild = leftChild.optimize(leftChild);
         rightChild = rightChild.optimize(rightChild);
         iterator = new JoinIterator(leftChild,rightChild,joinIterator.getOnExpression());
      }

      return iterator;
   }

   //endregion
}
