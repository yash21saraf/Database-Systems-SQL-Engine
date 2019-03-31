package iterators;

import helpers.CommonLib;
import helpers.Schema;
import helpers.Sort;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;

import java.util.Random;

public class SortMergeIterator implements RAIterator{

    Sort leftSort;
    Sort rightSort;
    private CommonLib commonLib = CommonLib.getInstance();
    private RAIterator leftChild;
    private RAIterator rightChild;
    private  Expression onExpression;

    private Schema[] schema;
    private boolean noRowFound = false;
    private boolean sorted = false;


    SortMergeIterator(RAIterator leftChild, RAIterator rightChild, Expression onExpression) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.onExpression = onExpression;
        //this.schema = createSchema(leftChild.getSchema(), rightChild.getSchema());
    }



    @Override
    public boolean hasNext() throws Exception {

        if (sorted)
            return !noRowFound;
        else{
            leftSort = new Sort(leftChild, commonLib.getColumnList(onExpression), null, null, (leftChild instanceof TableIterator));
            rightSort = new Sort(rightChild, commonLib.getColumnList(onExpression), null, null, (rightChild instanceof TableIterator));

            if (!rightChild.hasNext())
                return leftChild.hasNext();
            return true;
        }
    }

    @Override
    public PrimitiveValue[] next() throws Exception {
        PrimitiveValue[] mergedTuple =null;// = getNext(leftChild, rightChild);
        if (mergedTuple == null)
            noRowFound = true;
        return mergedTuple;
    }

    @Override
    public void reset() throws Exception {

    }

    @Override
    public RAIterator getChild() {
        return null;
    }

    @Override
    public void setChild(RAIterator child) {

    }

    @Override
    public Schema[] getSchema() {
        return new Schema[0];
    }

    @Override
    public void setSchema(Schema[] schema) {

    }

    @Override
    public RAIterator optimize(RAIterator iterator) {
        return null;
    }
}
