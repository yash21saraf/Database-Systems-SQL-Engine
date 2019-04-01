package iterators;

import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import helpers.Schema;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
/*import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;*/

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.*;

public class JoinIterator implements RAIterator
{
   //region Variables

   private CommonLib commonLib = CommonLib.getInstance();

   private RAIterator leftChild;
   private RAIterator rightChild;
   private Expression onExpression;

   private PrimitiveValue[] leftTuple;
   private PrimitiveValue[] rightTuple;
   private Schema[] schema ;
   private boolean first = true;

   ////////////////////////////////////////////////////
   //////// Hash Join Variables ///////////////////////
   ////////////////////////////////////////////////////
   private boolean hashCreated = false ;
   private Map<String, List<PrimitiveValue[]>> leftBucket = new HashMap<String, List<PrimitiveValue[]>>() ;
   private Map<String, List<PrimitiveValue[]>> rightBucket = new HashMap<String, List<PrimitiveValue[]>>() ;
   private List<Column> leftColList = null ;
   private List<Column> rightColList = null ;
   private List<Expression> leftExpList = null ;
   private List<Expression> rightExpList = null ;
   private List<Integer> leftColIndexes = new ArrayList<Integer>() ;
   private List<Integer> rightColIndexes = new ArrayList<Integer>() ;
   private boolean onePass = false ;
   private PrimitiveValue[] currentRightTuple = null ;
   private Integer leftBucketPointer = 0 ;



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

//      if(onExpression != null){
         if(this.first){
            defineHashColumns();
         }
         return hashHasNext() ;
//      }

//      else{
//         try {
//            if (!rightChild.hasNext())
//               return leftChild.hasNext();
//            return true;
//         } catch (Exception e) {
//            //logger.error("Error in reading from right table of join.");
//            throw e;
//         }
//      }

   }

   @Override
   public PrimitiveValue[] next() throws Exception
   {
//      if(onExpression != null){
         return hashNext() ;
//      }

//      else{
         // region NLJ
         ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
         ////////////////// NESTED LOOP JOIN ////////////////////////////////////////////////////////////////////////////
         ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//      try {
//          if (this.first){
//              this.first = false ;
//              if (leftChild.hasNext())
//                  leftTuple = leftChild.next();
//          }
//              if (!rightChild.hasNext()) {
//              rightChild.reset();
//              leftTuple = leftChild.next();
//          }
//
//          rightTuple = rightChild.next();
//
//          if(rightTuple == null || leftTuple == null) {
//              return null;
//          }
//
//          if (onExpression != null) {
//            PrimitiveValueWrapper[] wrappedLeftTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(leftTuple, leftChild.getSchema());
//            PrimitiveValueWrapper[] wrappedRightTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(rightTuple, rightChild.getSchema());
//            if (commonLib.eval(onExpression,wrappedRightTuple, wrappedLeftTuple).getPrimitiveValue().toBool()) {
//               return CommonLib.concatArrays(leftTuple,rightTuple);
//            }
//         } else if(rightTuple != null && leftTuple != null){
//            return CommonLib.concatArrays(leftTuple,rightTuple);
//         }
//         return null;
//      } catch (Exception e) {
//         //logger.error("Error in JoinIterator.next() during rightChild.hasNext() check.");
//         throw e;
//      }

         ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
         ////////////////// NESTED LOOP JOIN ////////////////////////////////////////////////////////////////////////////
         ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
         // endregion
//      }


   }
   // region overridden methods
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

   // endregion

    private void defineHashColumns(){
       if(onExpression != null){
          List<Expression> expressionList = commonLib.getExpressionList(onExpression);
          Schema[] rightSchema = rightChild.getSchema();
          List<Column> leftColList = new ArrayList<Column>();
          List<Column> rightColList = new ArrayList<Column>()  ;
          List<Expression> leftExpList = new ArrayList<Expression>() ;
          List<Expression> rightExpList = new ArrayList<Expression>() ;
          EqualsTo equalsTo;
          for (Expression expressionItem : expressionList) {
             if ((equalsTo = (EqualsTo) CommonLib.castAs(expressionItem,EqualsTo.class)) != null) {
                if (commonLib.validateExpressionAgainstSchema(equalsTo.getRightExpression(),rightSchema)) {
                     if (equalsTo.getRightExpression() instanceof Column){
                        rightColList.add((Column) equalsTo.getRightExpression()) ;
                     }
                     else if (equalsTo.getRightExpression() instanceof Expression){
                        rightColList.addAll(commonLib.getColumnList(equalsTo.getRightExpression())) ;
                     }

                     if (equalsTo.getLeftExpression() instanceof Column){
                      leftColList.add((Column) equalsTo.getLeftExpression()) ;
                     }
                     else if (equalsTo.getLeftExpression() instanceof Expression){
                      leftColList.addAll(commonLib.getColumnList(equalsTo.getLeftExpression())) ;
                     }
                   rightExpList.add(equalsTo.getRightExpression()) ;
                   leftExpList.add(equalsTo.getLeftExpression()) ;
                }
                else {
                   if (equalsTo.getRightExpression() instanceof Column){
                      leftColList.add((Column) equalsTo.getRightExpression()) ;
                   }
                   else if (equalsTo.getRightExpression() instanceof Expression){
                      leftColList.addAll(commonLib.getColumnList(equalsTo.getRightExpression())) ;
                   }

                   if (equalsTo.getLeftExpression() instanceof Column){
                      rightColList.add((Column) equalsTo.getLeftExpression()) ;
                   }
                   else if (equalsTo.getLeftExpression() instanceof Expression){
                      rightColList.addAll(commonLib.getColumnList(equalsTo.getLeftExpression())) ;
                   }

                   rightExpList.add(equalsTo.getLeftExpression()) ;
                   leftExpList.add(equalsTo.getRightExpression()) ;
                }
             }
          }

          this.leftColList = leftColList ;
          this.rightColList = rightColList ;
          this.leftColIndexes = commonLib.ColToSchemaIndexes(leftColList,leftChild.getSchema()) ;
          this.rightColIndexes = commonLib.ColToSchemaIndexes(rightColList, rightSchema) ;
          this.leftExpList = leftExpList ;
          this.rightExpList = rightExpList ;
       }
   }

   private boolean hashHasNext() throws Exception {
      if(this.first){
         while(commonLib.memoryPending() && leftChild.hasNext()){
            fillBuckets();
         }
         if(leftBucket.isEmpty() && !leftChild.hasNext()){
            return false ;
         }
         else if(!leftChild.hasNext()){
            this.onePass = true ;
         }
         else if(!rightChild.hasNext()){
            return false ;
         }
         else{
            while(this.currentRightTuple == null){
               this.currentRightTuple = rightChild.next() ;
               if(this.currentRightTuple == null && !rightChild.hasNext()){
                  return false ;
               }
            }
         }
         this.first = false ;
      }
      if(this.onePass){
         return onePassHashHasNext() ;
      }
      return false ;
//      else
//         return twoPassHashNext() ;
   }

   private  Boolean onePassHashHasNext() throws Exception {

         if(rightChild.hasNext()){
            return true ;
         }
         else if (this.currentRightTuple == null){
              return false ;
         }
         else {
            String rightKey = "" ;
            for(int i = 0 ; i < this.rightColIndexes.size() ; i++){
               rightKey = rightKey + "|" + currentRightTuple[rightColIndexes.get(i)].toRawString() ;
            }
            if(leftBucket.containsKey(rightKey)){
               if(leftBucket.get(rightKey).size() > this.leftBucketPointer){
                  return true ;
               }
            }
         }
         return false ;
      }


   private PrimitiveValue[] hashNext() throws Exception {
         if(this.onePass){
            return oneHashNext() ;
         }
         return oneHashNext() ;
   }

   private PrimitiveValue[] oneHashNext() throws Exception {

      while(this.currentRightTuple == null){
         this.currentRightTuple = rightChild.next() ;
         this.leftBucketPointer = 0 ;
         if(this.currentRightTuple == null && !rightChild.hasNext()){
            return null ;
         }
      }

      String rightKey = "" ;
      for(int i = 0 ; i < this.rightColIndexes.size() ; i++){
         rightKey = rightKey + "|" + currentRightTuple[rightColIndexes.get(i)].toRawString() ;
      }
      if(leftBucket.containsKey(rightKey)){
         if(leftBucket.get(rightKey).size() > this.leftBucketPointer){
            PrimitiveValue[] leftTuple = leftBucket.get(rightKey).get(leftBucketPointer) ;
            leftBucketPointer++ ;
            return CommonLib.concatArrays(leftTuple,this.currentRightTuple);
         }
         else{
            this.currentRightTuple = rightChild.next() ;
            this.leftBucketPointer = 0 ;
         }
      }
      else{
         this.currentRightTuple = rightChild.next() ;
         this.leftBucketPointer = 0 ;
      }
      return null ;
   }


   private void fillBuckets() throws Exception {
      PrimitiveValue[] leftTuple = null ;
      Integer leftBucketSize = 0 ;
      try{
         while (leftChild.hasNext()) {
                  leftTuple = leftChild.next() ;
                  if(leftTuple != null){

                     leftBucketSize++ ;
                     String leftKey = "" ;

                     for(int i = 0 ; i < this.leftColIndexes.size() ; i++){
                        leftKey = leftKey + "|" + leftTuple[leftColIndexes.get(i)].toRawString() ;
                     }

                     List<PrimitiveValue[]> updatedList ;
                     List<PrimitiveValue[]> currentList = leftBucket.get(leftKey);
                     updatedList = (currentList != null) ? currentList : new LinkedList<PrimitiveValue[]>();
                     updatedList.add(leftTuple);
                     leftBucket.put(leftKey, updatedList);

                     if(leftBucketSize == 1000000000) break ;leftBucketSize++ ;

                  }
            }

      }
      catch (Exception e) {
         //logger.error("Error in JoinIterator.next() during rightChild.hasNext() check.");
         throw e;
      }
   }
   //endregion
}
