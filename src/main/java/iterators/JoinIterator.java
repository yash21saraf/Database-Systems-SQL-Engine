package iterators;

import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import helpers.Schema;
import helpers.Sort;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.List;

import static helpers.CommonLib.listOfSortedFiles;
import static helpers.CommonLib.mapOfSortedFileObjects;


public class JoinIterator implements RAIterator {
    //region Variables

    Sort leftSort;
    Sort rightSort;


    //private static final Logger logger = LogManager.getLogger();
    private CommonLib commonLib = CommonLib.getInstance();
    private RAIterator leftChild;
    private RAIterator rightChild;
    private Expression onExpression;
    private ColumnDefinition[] columnDefinitions;
    private PrimitiveValue[] leftTuple;
    private PrimitiveValue[] rightTuple;
    private Schema[] schema;
    private boolean noRowFound = false;
    private boolean mergeSortJoin = true;
    private boolean sorted = false;
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

        if (mergeSortJoin) {
            if (sorted)
                return !noRowFound;
        }

        if (!rightChild.hasNext())
            return leftChild.hasNext();
        return true;

    }

    @Override
    public PrimitiveValue[] next() throws Exception {
        try {

            if (mergeSortJoin && onExpression != null) {
                if (!sorted)
                    SortFiles();
                PrimitiveValue[] mergedTuple = getNext(leftSort, rightSort);
                if (mergedTuple == null)
                    noRowFound = true;
                return mergedTuple;
            }

            if (leftTuple == null)
                if (leftChild.hasNext())
                    leftTuple = leftChild.next();
            if (!rightChild.hasNext()) {
                rightChild.reset();
                leftTuple = leftChild.next();
            }

            rightTuple = rightChild.next();
            if (rightTuple == null || leftTuple == null) {
                return null;
            }

            if (onExpression != null) { // TODO: Eval usage can be removed here if we are only dealing with equality.
                PrimitiveValueWrapper[] wrappedLeftTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(leftTuple, leftChild.getSchema());
                PrimitiveValueWrapper[] wrappedRightTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(rightTuple, rightChild.getSchema());
                if (commonLib.eval(onExpression, wrappedRightTuple, wrappedLeftTuple).getPrimitiveValue().toBool()) {
                    return CommonLib.concatArrays(leftTuple, rightTuple);
                }
            } else if (rightTuple != null && leftTuple != null) {
                return CommonLib.concatArrays(leftTuple, rightTuple);
            }
            return null;
        } catch (Exception e) {
            throw e;
        }
    }

    private void SortFiles() throws Exception {

        if (leftChild.hasNext() && rightChild.hasNext()) {

            String leftFileName = getFileName(leftChild);
            String rightFileName = getFileName(rightChild);

            if (listOfSortedFiles.size() > 0) {
                if (!listOfSortedFiles.contains(leftFileName)) {
                    leftSort = new Sort(leftChild, onExpression);
                    leftSort.sort();
                    listOfSortedFiles.add(leftFileName.toLowerCase());
                    mapOfSortedFileObjects.put(leftFileName, leftSort);
                } else {
                    leftSort = mapOfSortedFileObjects.get(leftFileName);
                    leftSort.reset();
                }
                if (!listOfSortedFiles.contains(rightFileName)) {
                    rightSort = new Sort(rightChild, onExpression);
                    rightSort.sort();
                    listOfSortedFiles.add(rightFileName.toLowerCase());
                    mapOfSortedFileObjects.put(rightFileName, rightSort);
                } else {
                    rightSort = mapOfSortedFileObjects.get(rightFileName);
                    rightSort.reset();
                }
            } else {
                leftSort = new Sort(leftChild, onExpression);
                rightSort = new Sort(rightChild, onExpression);

                leftSort.sort();
                rightSort.sort();

                listOfSortedFiles.add(leftFileName.toLowerCase());
                listOfSortedFiles.add(rightFileName.toLowerCase());

                mapOfSortedFileObjects.put(leftFileName.toLowerCase(), leftSort);
                mapOfSortedFileObjects.put(rightFileName.toLowerCase(), rightSort);
            }

            sorted = true;
        }
    }

    private String getFileName(RAIterator child) {
        Schema[] schemas = child.getSchema();

        for (int i = 0; i < schemas.length; i++) {
            return schemas[i].getTableName().toLowerCase();
        }
        return "dummy";
    }

    private PrimitiveValue[] getNext(Sort leftSort, Sort rightSort) throws Exception { // TODO: need to do key grouping

        leftTuple = leftSort.getTuple();
        rightTuple = rightSort.getTuple();

        if (leftTuple == null || rightTuple == null)
            return null;

        // TODO: Assuming that there is only one equality condition in onExpression.

        List<Integer> leftIndexList = leftSort.getIndexOfSortKey();
        List<Integer> rightIndexList = rightSort.getIndexOfSortKey();

        int leftIndex = leftIndexList.get(0);
        int rightIndex = rightIndexList.get(0);

        int sign = 0;
        while (leftTuple != null && rightTuple != null) {
            if ((sign = isEqual(leftTuple[leftIndex], rightTuple[rightIndex])) == 0) {
                return CommonLib.concatArrays(leftTuple, rightTuple);
            } else if (sign < 0) {
                leftTuple = leftSort.getTuple();
            } else if (sign > 0) {
                rightTuple = rightSort.getTuple();
            }
        }
        return null;
    }

    private int isEqual(PrimitiveValue a, PrimitiveValue b) throws Exception {
        if (a instanceof StringValue) {
            return a.toString().compareTo(b.toString());
        } else if (a instanceof LongValue) {
            return Long.valueOf(a.toLong()).compareTo(Long.valueOf(b.toLong()));
        } else if (a instanceof DoubleValue) {
            return Double.valueOf(a.toDouble()).compareTo(Double.valueOf(b.toDouble()));
        } else if (a instanceof DateValue) {
            return ((DateValue) a).getValue().compareTo(((DateValue) b).getValue());
        }
        return 0;
    }

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

    //endregion
}
