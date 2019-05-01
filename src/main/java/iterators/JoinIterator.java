package iterators;

import dubstep.Main;
import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import helpers.Schema;
import helpers.Sort;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;

import java.util.*;

public class JoinIterator implements RAIterator {
    //region Variables

    private CommonLib commonLib = CommonLib.getInstance();

    private RAIterator leftChild;
    private RAIterator rightChild;
    private Expression onExpression;

    private PrimitiveValue[] leftTuple;
    private PrimitiveValue[] rightTuple;
    private Schema[] schema;
    private boolean first = true;

    ////////////////////////////////////////////////////
    //////// Hash Join Variables ///////////////////////
    ////////////////////////////////////////////////////
    private Map<String, List<PrimitiveValue[]>> leftBucket = new HashMap<String, List<PrimitiveValue[]>>();
    private List<Expression> leftExpList = null;
    private List<Expression> rightExpList = null;
    private boolean onePass = false;
    private PrimitiveValue[] currentRightTuple = null;
    private Integer leftBucketPointer = 0;

    ///////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////
    // Created functionality but not used.
    // Can be uncommented in getHashColumns Function to obtain values
    // for leftColList, rightColList, leftColIndexes, and rightColIndexes
    ///////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////

    //   private List<Integer> leftColIndexes = new ArrayList<Integer>() ;
    //   private List<Column> leftColList = null ;
    //   private List<Column> rightColList = null ;
    //   private List<Integer> rightColIndexes = new ArrayList<Integer>() ;

    ///////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////
    //////// SORTED MERGE JOIN ALGORITHM ////////////////////////
    ///////////////////////////////////////////////////////
    private Sort leftSort;
    private Sort rightSort;
    private PrimitiveValue[] smjRightTuple = null;
    private PrimitiveValue[] smjLeftTuple = null;
    private boolean smjHasNextFlag = true;
    private List<PrimitiveValue[]> smjBucket = new ArrayList<PrimitiveValue[]>();
    private Integer smjBucketPointer = 0;


    //////////////////////////////////////////////////////////////
    //////////// Global key logic /////////////////////////////////
    /////////////////////////////////////////////////////////////////

    private PrimitiveValue[] o1key = null;
    private PrimitiveValue[] o2key = null;


    //endregion

    //region Constructor

    public JoinIterator(RAIterator leftChild, RAIterator rightChild, Expression onExpression) {

        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.onExpression = onExpression;
        this.schema = createSchema(leftChild.getSchema(), rightChild.getSchema());
    }

    private Schema[] createSchema(Schema[] leftSchema, Schema[] rightSchema) {
        return CommonLib.concatArrays(leftSchema, rightSchema);
    }


    //endregion

    //region Iterator methods

    @Override
    public boolean hasNext() throws Exception {
        // One pass Hash Join for In Memory
        if (Main.inMem) {
//      if(false){
            if (this.first) {
                defineHashColumns();
            }
            return hashHasNext();
        }

        // Sorted Merge Join for On Disk
        else {
            if (this.first) {
                defineHashColumns();
            }
            return smjHasNext();
        }

        // Nested Loop Join used for Checkpoint 1
        //region Nested Loop Join Logic
//         try {
//            if (!rightChild.hasNext())
//               return leftChild.hasNext();
//            return true;
//         } catch (Exception e) {
//            //logger.error("Error in reading from right table of join.");
//            throw e;
//         }
//      }
        //endregion
    }

    @Override
    public PrimitiveValue[] next() throws Exception {
        // One pass Hash Join for In Memory Calculations
        if (Main.inMem) {
//      if(false){
            return hashNext();
        }
        // Sorted Merge Join for On Disk Calculations
        else {
            return smjNext();
        }

        // Nested Loop Join Logic used for Checkpoint 1
        // region Nested Loop Join Logic
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

    }

    // region overridden methods
    @Override
    public void reset() throws Exception {
        leftChild.reset();
        rightChild.reset();
    }

    @Override
    public RAIterator getChild() {
        return leftChild;
    }

    @Override
    public void setChild(RAIterator child) {
        this.leftChild = child;
    }

    @Override
    public Schema[] getSchema() {
        return this.schema;
    }

    @Override
    public void setSchema(Schema[] schema) {
        this.schema = schema;
    }

    public RAIterator getRightChild() {
        return this.rightChild;
    }

    public void setRightChild(RAIterator rightChild) {
        this.rightChild = rightChild;
    }

    public Expression getOnExpression() {
        return this.onExpression;
    }

    public void setOnExpression(Expression onExpression) {
        this.onExpression = onExpression;
    }

    @Override
    public RAIterator optimize(RAIterator iterator) {
        JoinIterator joinIterator;

        if ((joinIterator = (JoinIterator) CommonLib.castAs(iterator, JoinIterator.class)) != null) {
            RAIterator leftChild = joinIterator.getChild();
            RAIterator rightChild = joinIterator.getRightChild();
            leftChild = leftChild.optimize(leftChild);
            rightChild = rightChild.optimize(rightChild);
            iterator = new JoinIterator(leftChild, rightChild, joinIterator.getOnExpression());
        }

        return iterator;
    }

    // endregion


    private boolean hashHasNext() throws Exception {

        if (this.first) {
            this.first = false;
            // while (commonLib.memoryPending() && leftChild.hasNext()) {
            while (leftChild.hasNext()) {
                onePassFillBuckets();
            }
            if (leftBucket.isEmpty() && !leftChild.hasNext()) {
                return false;
            } else if (!leftChild.hasNext()) {
                this.onePass = true;
            } else if (!rightChild.hasNext()) {
                return false;
            }

            while (this.currentRightTuple == null) {
                this.currentRightTuple = rightChild.next();
                if (this.currentRightTuple == null && !rightChild.hasNext()) {
                    return false;
                }
            }
        }
        if (this.onePass) {
            return onePassHashHasNext();
        }
        return false;
    }

    private PrimitiveValue[] hashNext() throws Exception {
        if (this.onePass) {
            return onePassHashNext();
        }
        return onePassHashNext();
    }

    private Boolean onePassHashHasNext() throws Exception {

        if (rightChild.hasNext()) {
            return true;
        } else if (this.currentRightTuple == null) {
            return false;
        } else {
            String rightKey = "";
            rightKey = createKey(currentRightTuple, rightExpList, "right");

            if (leftBucket.containsKey(rightKey)) {
                if (leftBucket.get(rightKey).size() > this.leftBucketPointer) {
                    return true;
                }
            }
        }
        return false;
    }

    private PrimitiveValue[] onePassHashNext() throws Exception {

        while (this.currentRightTuple == null) {
            this.currentRightTuple = rightChild.next();
            this.leftBucketPointer = 0;
            if (this.currentRightTuple == null && !rightChild.hasNext()) {
                return null;
            }
        }

        String rightKey = "";
        rightKey = createKey(currentRightTuple, rightExpList, "right");

        if (leftBucket.containsKey(rightKey)) {
            if (leftBucket.get(rightKey).size() > this.leftBucketPointer) {
                PrimitiveValue[] leftTuple = leftBucket.get(rightKey).get(leftBucketPointer);
                leftBucketPointer++;
                return CommonLib.concatArrays(leftTuple, this.currentRightTuple);
            } else {
                this.currentRightTuple = rightChild.next();
                this.leftBucketPointer = 0;
            }
        } else {
            this.currentRightTuple = rightChild.next();
            this.leftBucketPointer = 0;
        }
        return null;
    }

    private Boolean smjHasNext() throws Exception {
        if (this.first) {
            this.first = false;
            if (leftChild.hasNext() &&
                    rightChild.hasNext()) {
                leftSort = new Sort(leftChild, leftExpList);
                rightSort = new Sort(rightChild, rightExpList);
                leftSort.newSort();
                rightSort.newSort();
                this.smjRightTuple = rightSort.getTupleNew();
                this.smjLeftTuple = leftSort.getTupleNew();
                if (this.smjRightTuple == null || this.smjLeftTuple == null) {
                    smjHasNextFlag = false;
                    leftSort = null;
                    rightSort = null;
                    Runtime r = Runtime.getRuntime();
                    r.gc();
                    return false;
                }
                SMJfillBuckets();
                return smjHasNextFlag;
            } else {
                return false;
            }
        } else {
            return smjHasNextFlag;
        }
    }


    private PrimitiveValue[] smjNext() throws Exception {
        PrimitiveValue[] returnValue = null;
        if (smjBucketPointer < smjBucket.size() - 1) {
            returnValue = commonLib.concatArrays(smjBucket.get(smjBucketPointer), smjRightTuple);
            smjBucketPointer++;
        } else if (smjBucketPointer == smjBucket.size() - 1) {
            returnValue = commonLib.concatArrays(smjBucket.get(smjBucketPointer), smjRightTuple);
            smjBucketPointer = 0;
            smjRightTuple = rightSort.getTupleNew();
            if (smjRightTuple == null) {
                leftSort = null;
                rightSort = null;
                Runtime r = Runtime.getRuntime();
                r.gc();
                smjHasNextFlag = false;
            } else if (newSortCompare(smjBucket.get(0), smjRightTuple) != 0) {
                smjBucket.clear();
                if (this.smjLeftTuple != null) {
                    SMJfillBuckets();
                } else {
                    leftSort = null;
                    rightSort = null;
                    Runtime r = Runtime.getRuntime();
                    r.gc();
                    smjHasNextFlag = false;
                }
            }
        }
        return returnValue;
    }

    private void SMJfillBuckets() throws Exception {

        while (this.smjHasNextFlag) {
            if (newSortCompare(this.smjLeftTuple, this.smjRightTuple) < 0) {
                iterateLeft();
            } else if (newSortCompare(this.smjLeftTuple, this.smjRightTuple) > 0) {
                iterateRight();
            } else {
                break;
            }
        }

        // Here we have lefttuple = righttuple or smjHasNextFlag as false.
        // Keep adding the tuple to smjbucket as it will be joined with leftTuple
        // Make sure you don't change the value for righttuple until and unless the join with arraylist is complete


        if (this.smjHasNextFlag) {
            while (newSortCompare(this.smjLeftTuple, this.smjRightTuple) == 0) {
                this.smjBucket.add(this.smjLeftTuple);
                this.smjLeftTuple = leftSort.getTupleNew();
                if (this.smjLeftTuple == null) {
                    break;
                }
            }
        }

        // If we have smjHasNextFlag as false, we are returning hasNext as false.
        // Or we have obtained a case where lefttuple = righttuple
        // So the smjBucket size is at least one.
        // Also we getting a new leftTuple here. So we need to make sure that we only
        // call rightSort.getTuple before calling the SMJfillBuckets() again.
    }

    // While lefttuple < righttuple. Keep getting tuple from left until either
    // lefttuple > righttuple OR lefttuple = righttuple.
    private void iterateLeft() throws Exception {
        while (newSortCompare(this.smjLeftTuple, this.smjRightTuple) < 0) {
            this.smjLeftTuple = leftSort.getTupleNew();
            if (this.smjLeftTuple == null) {
                leftSort = null;
                rightSort = null;
                Runtime r = Runtime.getRuntime();
                r.gc();
                this.smjHasNextFlag = false;
                break;
            }
        }
    }

    // While lefttuple > righttuple. Keep getting tuple from left until either
    // lefttuple < righttuple OR lefttuple = righttuple.
    private void iterateRight() throws Exception {
        while (newSortCompare(this.smjLeftTuple, this.smjRightTuple) > 0) {
            this.smjRightTuple = rightSort.getTupleNew();
            if (this.smjRightTuple == null) {
                leftSort = null;
                rightSort = null;
                Runtime r = Runtime.getRuntime();
                r.gc();
                this.smjHasNextFlag = false;
                break;
            }
        }
    }


    private void onePassFillBuckets() throws Exception {

        PrimitiveValue[] leftTuple;

        try {
            while (leftChild.hasNext()) {
                leftTuple = leftChild.next();
                if (leftTuple != null) {

                    String leftKey;

                    leftKey = createKey(leftTuple, leftExpList, "left");

                    List<PrimitiveValue[]> updatedList;
                    List<PrimitiveValue[]> currentList = leftBucket.get(leftKey);
                    updatedList = (currentList != null) ? currentList : new LinkedList<PrimitiveValue[]>();
                    updatedList.add(leftTuple);
                    leftBucket.put(leftKey, updatedList);


                }
            }

        } catch (Exception e) {
            //logger.error("Error in JoinIterator.next() during rightChild.hasNext() check.");
            throw e;
        }
    }

    private void defineHashColumns() {
        if (onExpression != null) {
            List<Expression> expressionList = commonLib.getExpressionList(onExpression);
            Schema[] rightSchema = rightChild.getSchema();

//         List<Column> leftColList = new ArrayList<Column>();
//         List<Column> rightColList = new ArrayList<Column>()  ;
            List<Expression> leftExpList = new ArrayList<Expression>();
            List<Expression> rightExpList = new ArrayList<Expression>();
            EqualsTo equalsTo;
            for (Expression expressionItem : expressionList) {
                if ((equalsTo = (EqualsTo) CommonLib.castAs(expressionItem, EqualsTo.class)) != null) {
                    if (commonLib.validateExpressionAgainstSchema(equalsTo.getRightExpression(), rightSchema)) {
                        // Functionality for getting Column Lists and Columns Indexes
                        // Uncomment later if required
//                  if (equalsTo.getRightExpression() instanceof Column){
//                     rightColList.add((Column) equalsTo.getRightExpression()) ;
//                  }
//                  else if (equalsTo.getRightExpression() instanceof Expression){
//                     rightColList.addAll(commonLib.getColumnList(equalsTo.getRightExpression())) ;
//                  }
//
//                  if (equalsTo.getLeftExpression() instanceof Column){
//                     leftColList.add((Column) equalsTo.getLeftExpression()) ;
//                  }
//                  else if (equalsTo.getLeftExpression() instanceof Expression){
//                     leftColList.addAll(commonLib.getColumnList(equalsTo.getLeftExpression())) ;
//                  }
                        rightExpList.add(equalsTo.getRightExpression());
                        leftExpList.add(equalsTo.getLeftExpression());
                    } else {
//                  if (equalsTo.getRightExpression() instanceof Column){
//                     leftColList.add((Column) equalsTo.getRightExpression()) ;
//                  }
//                  else if (equalsTo.getRightExpression() instanceof Expression){
//                     leftColList.addAll(commonLib.getColumnList(equalsTo.getRightExpression())) ;
//                  }
//
//                  if (equalsTo.getLeftExpression() instanceof Column){
//                     rightColList.add((Column) equalsTo.getLeftExpression()) ;
//                  }
//                  else if (equalsTo.getLeftExpression() instanceof Expression){
//                     rightColList.addAll(commonLib.getColumnList(equalsTo.getLeftExpression())) ;
//                  }

                        rightExpList.add(equalsTo.getLeftExpression());
                        leftExpList.add(equalsTo.getRightExpression());
                    }
                }
            }

//         this.leftColList = leftColList ;
//         this.rightColList = rightColList ;
//         this.leftColIndexes = commonLib.ColToSchemaIndexes(leftColList,leftChild.getSchema()) ;
//         this.rightColIndexes = commonLib.ColToSchemaIndexes(rightColList, rightSchema) ;
            this.leftExpList = leftExpList;
            this.rightExpList = rightExpList;
            this.o1key = new PrimitiveValue[leftExpList.size()];
            this.o2key = new PrimitiveValue[rightExpList.size()];
        }
    }

    private String createKey(PrimitiveValue[] tuple, List<Expression> expression, String side) throws Exception {
        StringBuilder key = new StringBuilder();
        if (expression == null) {
            return "crossJoin";
        }
        for (int i = 0; i < expression.size(); i++) {
            PrimitiveValueWrapper[] wrappedTuple;
            PrimitiveValue result;

            if (side.equals("left")) {
                wrappedTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(tuple, leftChild.getSchema());
            } else {
                wrappedTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(tuple, rightChild.getSchema());
            }
            result = commonLib.eval(expression.get(i), wrappedTuple).getPrimitiveValue();
            key.append("|").append(result);
        }
        return key.toString();
    }

    private PrimitiveValue[] createKeyPrimitive(PrimitiveValue[] tuple, List<Expression> expression, String side) throws Exception {
        if (expression == null) {
            PrimitiveValue[] key = new PrimitiveValue[1];
            key[1] = new DoubleValue(1111);
            return key;
        }
        PrimitiveValue[] key = new PrimitiveValue[expression.size()];
        for (int i = 0; i < expression.size(); i++) {
            PrimitiveValueWrapper[] wrappedTuple;
            if (side.equals("left")) {
                wrappedTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(tuple, leftChild.getSchema());
            } else {
                wrappedTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(tuple, rightChild.getSchema());
            }
            key[i] = commonLib.eval(expression.get(i), wrappedTuple).getPrimitiveValue();
        }
        return key;
    }

    // Made both o1
    private void createKeyPrim(PrimitiveValue[] a, PrimitiveValue[] b) throws Exception {
//      this.o1key = null ;
//      this.o2key = null ;
        for (int i = 0; i < leftExpList.size(); i++) {
            PrimitiveValueWrapper[] wrappedTuple;
            wrappedTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(a, leftChild.getSchema());
            this.o1key[i] = commonLib.eval(leftExpList.get(i), wrappedTuple).getPrimitiveValue();
            wrappedTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(b, rightChild.getSchema());
            this.o2key[i] = commonLib.eval(rightExpList.get(i), wrappedTuple).getPrimitiveValue();
        }
    }

    private int newSortCompare(PrimitiveValue[] a, PrimitiveValue[] b) throws Exception {

//      PrimitiveValue[] o1key = null ;
//      PrimitiveValue[] o2key = null;

//      try {
//         o1key = createKeyPrimitive(a, leftExpList, "left");
//         o2key = createKeyPrimitive(b, rightExpList, "right") ;
        createKeyPrim(a, b);

        for (int i = 0; i < leftExpList.size(); i++) {

            if (o1key[i] instanceof StringValue) {
                int comp = o1key[i].toString().compareTo(o2key[i].toString());
                if (comp != 0)
                    return comp;
                else
                    continue;
            } else if (o1key[i] instanceof LongValue) {
                int comp = Long.valueOf(o1key[i].toLong()).compareTo(o2key[i].toLong());
                if (comp != 0)
                    return comp;
                else
                    continue;
            } else if (o1key[i] instanceof DoubleValue) {
                int comp = Double.compare(o1key[i].toDouble(), o2key[i].toDouble());
                if (comp != 0)
                    return comp;
                else
                    continue;
            } else if (o1key[i] instanceof DateValue) {
                int comp = ((DateValue) o1key[i]).getValue().compareTo(((DateValue) o2key[i]).getValue());
                if (comp != 0)
                    return comp;
                else
                    continue;
            }
        }

//      catch (Exception e) {
//         e.printStackTrace();
//      }
        return 0;
    }
    //endregion
}