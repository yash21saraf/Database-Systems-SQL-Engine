package iterators;

import dubstep.AppMain;
import helpers.PrimitiveValueWrapper;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.*;

import dubstep.AppMain.*;


public class OrderByIterator implements RAIterator {

    //region Variables

    boolean sorted = false;
    private RAIterator child;
    private List<OrderByElement> orderByElementsList;
    private List<List<PrimitiveValue>> sortedList = new ArrayList<List<PrimitiveValue>>();
    private List<Integer> indexOfOrderByElements;
    private List<Boolean> orderOfOrderByElements; // asc : true, desc : false
    private int currentIndex = 0;
    //endregion

    //region Constructor

    public OrderByIterator(RAIterator rootIterator, List<OrderByElement> orderByElementsList, List<Integer> indexOfOrderByElements, List<Boolean> orderOfOrderByElements) {

        this.child = rootIterator;
        this.orderByElementsList = orderByElementsList;
        this.indexOfOrderByElements = indexOfOrderByElements;
        this.orderOfOrderByElements = orderOfOrderByElements;
    }

    //endregion

    //region Iterator methods

    @Override
    public boolean hasNext() throws Exception {


        if (sorted)
            if (sortedList.size() > currentIndex)
                return true;
            else
                return false;

        return child.hasNext();

    }

    @Override
    public PrimitiveValue[] next() throws Exception {

        if (AppMain.inMem) {

            if (sorted) {
                PrimitiveValue[] primitiveValueWrappers = sortedList.get(currentIndex).toArray(new PrimitiveValue[sortedList.get(0).size()]);
                currentIndex++;
                return primitiveValueWrappers;
            }

            while (child.hasNext()) {
                sortedList.add(Arrays.asList(child.next()));
            }

            Collections.sort(sortedList, new Comparator<List<PrimitiveValue>>() {
                @Override
                public int compare(List<PrimitiveValue> first, List<PrimitiveValue> second) {

                    int i = 0;

                    for (Integer index : indexOfOrderByElements) {
                        String primitiveValue1 = first.get(index).toRawString();
                        String primitiveValue2 = second.get(index).toRawString();

                        if (orderOfOrderByElements.get(i++)) {

                            if (primitiveValue1.compareTo(primitiveValue2) != 0)
                                return primitiveValue1.compareTo(primitiveValue2);
                            else {
                                continue;
                            }

                        } else {

                            if (primitiveValue1.compareTo(primitiveValue2) != 0)
                                return -1 * primitiveValue1.compareTo(primitiveValue2);
                            else {
                                continue;
                            }
                        }

                    }
                    return 1;
                }
            });

            sorted = true;

            PrimitiveValue[] primitiveValueWrappers = sortedList.get(currentIndex).toArray(new PrimitiveValue[sortedList.get(0).size()]);
            currentIndex++;

            return primitiveValueWrappers;

        } else {

            if (sorted) {
                PrimitiveValue[] primitiveValueWrappers = sortedList.get(currentIndex).toArray(new PrimitiveValue[sortedList.get(0).size()]);
                currentIndex++;
                return primitiveValueWrappers;
            }

            while (child.hasNext()) {
                sortedList.add(Arrays.asList(child.next()));
            }

            Collections.sort(sortedList, new Comparator<List<PrimitiveValue>>() {
                @Override
                public int compare(List<PrimitiveValue> first, List<PrimitiveValue> second) {

                    int i = 0;

                    for (Integer index : indexOfOrderByElements) {
                        String primitiveValue1 = first.get(index).toRawString();
                        String primitiveValue2 = second.get(index).toRawString();

                        if (orderOfOrderByElements.get(i++)) {

                            if (primitiveValue1.compareTo(primitiveValue2) != 0)
                                return primitiveValue1.compareTo(primitiveValue2);
                            else {
                                continue;
                            }

                        } else {

                            if (primitiveValue1.compareTo(primitiveValue2) != 0)
                                return -1 * primitiveValue1.compareTo(primitiveValue2);
                            else {
                                continue;
                            }
                        }

                    }
                    return 1;
                }
            });

            sorted = true;

            PrimitiveValue[] primitiveValueWrappers = sortedList.get(currentIndex).toArray(new PrimitiveValue[sortedList.get(0).size()]);
            currentIndex++;
            return primitiveValueWrappers;

        }

    }

    @Override
    public void reset() throws Exception {
        child.reset();
    }

    @Override
    public RAIterator getChild() {
        return this.child ;
    }

    @Override
    public void setChild(RAIterator child) {
        this.child = child ;
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

    //endregion
}
