package iterators;

import helpers.PrimitiveValueWrapper;
import helpers.Schema;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.*;

public class LimitIterator implements RAIterator {

    //region Variables

    private RAIterator child;
    private Limit limit;
    private long currentIndex = 0;
    private Schema[] schema ;
    //endregion

    //region Constructor

    public LimitIterator(RAIterator rootIterator, Limit limit) {

        this.child = rootIterator;
        this.limit = limit;
        this.schema = child.getSchema() ;
    }

    //endregion

    //region Iterator methods

    @Override
    public boolean hasNext() throws Exception {

        return child.hasNext() && currentIndex < limit.getRowCount();

    }

    @Override
    public PrimitiveValue[] next() throws Exception {

        PrimitiveValue[] primitiveValueWrappers = child.next();
        currentIndex++;
        return primitiveValueWrappers;

    }

    @Override
    public void reset() throws Exception {
        child.reset();
    }

    @Override
    public RAIterator getChild() {
        return null;
    }

    @Override
    public void setChild(RAIterator child) {
        this.child = child ;
    }

    @Override
    public Schema[] getSchema() {
        return this.schema ;
    }

    @Override
    public void setSchema(Schema[] schema) {
        this.schema = schema ;
    }


    //endregion
}
