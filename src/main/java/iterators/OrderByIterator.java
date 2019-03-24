package iterators;

import dubstep.AppMain;
import helpers.PrimitiveValueWrapper;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.*;

import dubstep.AppMain.*;

public class OrderByIterator implements RAIterator {

    //region Variables


    boolean sorted = false;
    private RAIterator currentIterator;
    private List<OrderByElement> orderByElementsList;
    private List<List<PrimitiveValueWrapper>> sortedList = new ArrayList<List<PrimitiveValueWrapper>>();
    private List<Integer> indexOfOrderByElements;
    private List<Boolean> orderOfOrderByElements; // asc : true, desc : false
    private int currentIndex = 0;
    //endregion

    //region Constructor

    public OrderByIterator(RAIterator rootIterator, List<OrderByElement> orderByElementsList, List<Integer> indexOfOrderByElements, List<Boolean> orderOfOrderByElements) {

        this.currentIterator = rootIterator;
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

        return currentIterator.hasNext();

    }

    @Override
    public PrimitiveValueWrapper[] next() throws Exception {

        //if (AppMain.inMem) {

            if (sorted) {
                PrimitiveValueWrapper[] primitiveValueWrappers = sortedList.get(currentIndex).toArray(new PrimitiveValueWrapper[sortedList.get(0).size()]);
                currentIndex++;
                return primitiveValueWrappers;
            }

            while (currentIterator.hasNext()) {
                sortedList.add(Arrays.asList(currentIterator.next()));
            }

            Collections.sort(sortedList, new Comparator<List<PrimitiveValueWrapper>>() {
                @Override
                public int compare(List<PrimitiveValueWrapper> first, List<PrimitiveValueWrapper> second) {

                    int i = 0;

                    for (Integer index : indexOfOrderByElements) {
                        String primitiveValue1 = first.get(index).getPrimitiveValue().toRawString();
                        String primitiveValue2 = second.get(index).getPrimitiveValue().toRawString();

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

            PrimitiveValueWrapper[] primitiveValueWrappers = sortedList.get(currentIndex).toArray(new PrimitiveValueWrapper[sortedList.get(0).size()]);
            currentIndex++;
            return primitiveValueWrappers;

//        } else {
//
//        }
//
//        return ;
    }

    @Override
    public void reset() throws Exception {

        currentIterator.reset();

    }

    //endregion
}
