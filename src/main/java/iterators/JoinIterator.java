package iterators;

import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import helpers.Schema;
import helpers.Sort;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static builders.IteratorBuilder.tableAliasToTableName;
import static helpers.CommonLib.listOfSortedFiles;
import static helpers.CommonLib.mapOfSortedFileObjects;


public class JoinIterator implements RAIterator {
    //region Variables

    Sort leftSort;
    Sort rightSort;

    private boolean mergeSortJoin = true;


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
                    SortFiles(leftChild, rightChild, onExpression);
                PrimitiveValue[] mergedTuple = getNext(leftChild, rightChild);
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


    private PrimitiveValue[] getNext(RAIterator leftChild, RAIterator rightChild) throws Exception { // TODO: need to do key grouping

        if (!(leftChild instanceof JoinIterator) && !(rightChild instanceof JoinIterator)) {
            return getNextFromTwoSort(leftChild, rightChild);

        } else if ((leftChild instanceof JoinIterator) && !(rightChild instanceof JoinIterator)) {
            PrimitiveValue[] tuple = getNext(((JoinIterator) leftChild).leftChild, ((JoinIterator) leftChild).rightChild);

            PrimitiveValue res[] = mergeAndReturnTuple(tuple, rightChild, leftChild.getSchema(), onExpression);

        }

        return null;

    }

    private PrimitiveValue[] mergeAndReturnTuple(PrimitiveValue[] tuple, RAIterator child, Schema[] tupleSchema) throws Exception {

        Sort sort = getSort(child);

        PrimitiveValue[] childTuple = sort.getTuple();

        if (childTuple == null || tuple == null)
            return null;


        List<Integer> leftIndexList = sort.getIndexOfSortKey();

        List<Integer> rightIndexList = sort.getIndexOfSortKey();

        int leftIndex = sortIndexList.get(0);
        int rightIndex = rightIndexList.get(0);


    }

    private PrimitiveValue[] mergeTuples(RAIterator next, RAIterator child) throws Exception {
        Sort rightSort = null;
        rightSort = getSort(child);

        PrimitiveValue[] childTuple = rightSort.getTuple();
        PrimitiveValue[] nextTuple = next.next();


        return null;

    }

    private Sort getSort(RAIterator child) {
        Schema[] schemas = child.getSchema();
        for (Map.Entry<String, Sort> entry : mapOfSortedFileObjects.entrySet()) {
            for (int i = 0; i < schemas.length; i++) {
                String tablename = schemas[i].getTableName();
                if (tableAliasToTableName.containsKey(tablename) || tableAliasToTableName.containsValue(tablename) || entry.getKey().equals(tablename)) { // TODO: validate the logic
                    return mapOfSortedFileObjects.get(tablename);
                }
            }
        }
        return null;
    }

    private HashMap<String, Sort> SortFiles(RAIterator leftChild, RAIterator rightChild, Expression onExpression) throws Exception {

        if (leftChild instanceof JoinIterator) {
            SortFiles(((JoinIterator) leftChild).leftChild, ((JoinIterator) leftChild).rightChild, ((JoinIterator) leftChild).onExpression);
            //leftChild = ((JoinIterator) leftChild).leftChild;
        }

        if (rightChild instanceof JoinIterator) {
            SortFiles(((JoinIterator) rightChild).leftChild, ((JoinIterator) rightChild).rightChild, ((JoinIterator) rightChild).onExpression);
            //rightChild = ((JoinIterator) rightChild).rightChild;
        }

        List<Column> columnsList = commonLib.getColumnList(onExpression);
        //HashMap<String, Sort> mapOfSortedFileObjects = new HashMap<String, Sort>();

        if (leftChild.hasNext() && rightChild.hasNext()) {

            String leftFileName = getFileName(leftChild);
            String rightFileName = getFileName(rightChild);

            if (listOfSortedFiles.size() > 0) {
                if (!listOfSortedFiles.contains(leftFileName)) {
                    leftSort = new Sort(leftChild, columnsList, null, null, (leftChild instanceof TableIterator));
                    leftSort.sort();
                    listOfSortedFiles.add(leftFileName.toLowerCase());
                    mapOfSortedFileObjects.put(leftFileName, leftSort);
                } else {
                    leftSort = mapOfSortedFileObjects.get(leftFileName);
                    leftSort.reset();
                }
                if (!listOfSortedFiles.contains(rightFileName)) {
                    rightSort = new Sort(rightChild, columnsList, null, null, (leftChild instanceof TableIterator));
                    rightSort.sort();
                    listOfSortedFiles.add(rightFileName.toLowerCase());
                    mapOfSortedFileObjects.put(rightFileName, rightSort);
                } else {
                    rightSort = mapOfSortedFileObjects.get(rightFileName);
                    rightSort.reset();
                }
            } else {
                leftSort = new Sort(leftChild, columnsList, null, null, (leftChild instanceof TableIterator));
                rightSort = new Sort(rightChild, columnsList, null, null, (leftChild instanceof TableIterator));

                leftSort.sort();
                rightSort.sort();

                listOfSortedFiles.add(leftFileName.toLowerCase());
                listOfSortedFiles.add(rightFileName.toLowerCase());

                mapOfSortedFileObjects.put(leftFileName.toLowerCase(), leftSort);
                mapOfSortedFileObjects.put(rightFileName.toLowerCase(), rightSort);
            }

            sorted = true;
        }
        return mapOfSortedFileObjects;
    }

    private String getFileName(RAIterator child) {
        Schema[] schemas = child.getSchema();

        for (int i = 0; i < schemas.length; i++) {
            return schemas[i].getTableName().toLowerCase();
        }
        return "dummy";
    }


    private PrimitiveValue[] getNextFromTwoSort(RAIterator leftChild, RAIterator rightChild) throws Exception {

        Sort leftSort = null;
        Sort rightSort = null;
        leftSort = getSort(leftChild);
        rightSort = getSort(rightChild);
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
