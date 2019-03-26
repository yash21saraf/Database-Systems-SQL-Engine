package iterators;

import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
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


//   private PrimitiveValueWrapper[] leftTuple;
//   private PrimitiveValueWrapper[] rightTuple;
   private PrimitiveValue[] leftTuple;
   private PrimitiveValue[] rightTuple;

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
         if (onExpression != null) {
            PrimitiveValueWrapper[] wrappedLeftTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(leftTuple, leftChild.getColumnDefinition(), leftChild.getTableName());
            PrimitiveValueWrapper[] wrappedRightTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(rightTuple, rightChild.getColumnDefinition(), rightChild.getTableName());
            if (commonLib.eval(onExpression,wrappedRightTuple, wrappedLeftTuple).getPrimitiveValue().toBool()) {
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

   @Override
   public RAIterator getChild() {
      return leftChild;
   }

   @Override
   public void setChild(RAIterator child) {
      this.leftChild = child ;
   }

   @Override
   public ColumnDefinition[] getColumnDefinition() {
      return new ColumnDefinition[0];
   }

   @Override
   public void setColumnDefinition(ColumnDefinition[] columnDefinition) {

   }

   @Override
   public void setTableName(String tableName) {

   }

   @Override
   public String getTableName() {
      return null;
   }

   @Override
   public void setTableAlias(String tableAlias) {

   }

   @Override
   public String getTableAlias() {
      return null;
   }

   public void setRightChild(RAIterator rightChild) {
      this.rightChild = rightChild;
   }

   public RAIterator getRightChild() {
      return this.rightChild;
   }

   //endregion
}
