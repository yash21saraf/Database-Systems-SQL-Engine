package iterators;

import helpers.PrimitiveValueWrapper;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.*;

public class LimitIterator implements RAIterator {

    //region Variables

    private RAIterator currentIterator;
    private Limit limit;
    private long currentIndex = 0;
    //endregion

    //region Constructor

    public LimitIterator(RAIterator rootIterator, Limit limit) {

        this.currentIterator = rootIterator;
        this.limit = limit;
    }

    //endregion

    //region Iterator methods

    @Override
    public boolean hasNext() throws Exception {

        return currentIterator.hasNext() && currentIndex < limit.getRowCount();

    }

    @Override
    public PrimitiveValueWrapper[] next() throws Exception {

        PrimitiveValueWrapper[] primitiveValueWrappers = currentIterator.next();
        currentIndex++;
        return primitiveValueWrappers;

    }

    @Override
    public void reset() throws Exception {
        currentIterator.reset();
    }

    //endregion
}
