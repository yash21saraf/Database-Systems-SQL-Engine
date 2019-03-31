package helpers;

import iterators.RAIterator;
import iterators.TableIterator;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class Sort {

    private List<Column> allColumns;
    private List<String> sortKeyColumns;
    private CommonLib commonLib = CommonLib.getInstance();
    private RAIterator child;
    private Expression condition;
    private List<PrimitiveValue> sortKey;
    private List<Integer> indexOfSortKey;
    private String fileName = "";
    private String path = TableIterator.TABLE_DIRECTORY;
    private String writtenFileName;

    private ColumnDefinition[] columnDefinitions;
    private int rowCounter = 0;

    private FileIterator[] fileIterators;
    private volatile int currFileIndex = 0;

    private int N = commonLib.getN();
    private long blockSize = commonLib.blockSize;
    private List<Boolean> orderOfOrderByElements;
    private List<Integer> indexOfOrderByElements;

    private Boolean isSourceFile;

    private int initialCapacity = 10000; // TODO: what's good number?

    //region Priority Queue
    private PriorityQueue<HashMap<Integer, PrimitiveValue[]>> priorityQueue = new PriorityQueue<HashMap<Integer, PrimitiveValue[]>>(initialCapacity,
            new Comparator<HashMap<Integer, PrimitiveValue[]>>() { //TODO: Keep NULL values at the end in PQ.
                @Override
                public int compare(HashMap<Integer, PrimitiveValue[]> o1, HashMap<Integer, PrimitiveValue[]> o2) {
                    PrimitiveValue[] primitiveValues1 = null;
                    PrimitiveValue[] primitiveValues2 = null;

                    for (Map.Entry<Integer, PrimitiveValue[]> entry1 : o1.entrySet()) {
                        Integer key1 = entry1.getKey();
                        primitiveValues1 = entry1.getValue();
                        break;
                    }

                    for (Map.Entry<Integer, PrimitiveValue[]> entry2 : o2.entrySet()) {
                        Integer key2 = entry2.getKey();
                        primitiveValues2 = entry2.getValue();
                        break;
                    }

                    for (int i = 0; i < indexOfSortKey.size(); i++) {
                        int index = indexOfSortKey.get(i);

                        try {

                            if (orderOfOrderByElements == null || orderOfOrderByElements.get(i)) {
                                if (primitiveValues1[index] instanceof StringValue) {
                                    int comp = primitiveValues1[index].toString().compareTo(primitiveValues2[index].toString());
                                    if (comp != 0)
                                        return comp;
                                    else
                                        continue;
                                } else if (primitiveValues1[index] instanceof LongValue) {
                                    int comp = Long.valueOf(primitiveValues1[index].toLong()).compareTo(Long.valueOf(primitiveValues2[index].toLong()));
                                    if (comp != 0)
                                        return comp;
                                    else
                                        continue;
                                } else if (primitiveValues1[index] instanceof DoubleValue) {
                                    int comp = Double.valueOf(primitiveValues1[index].toDouble()).compareTo(Double.valueOf(primitiveValues2[index].toDouble()));
                                    if (comp != 0)
                                        return comp;
                                    else
                                        continue;
                                } else if (primitiveValues1[index] instanceof DateValue) {
                                    int comp = ((DateValue) primitiveValues1[index]).getValue().compareTo(((DateValue) primitiveValues1[index]).getValue());
                                    if (comp != 0)
                                        return comp;
                                    else
                                        continue;
                                }

                            } else {
                                if (primitiveValues1[index] instanceof StringValue) {
                                    int comp = primitiveValues1[index].toString().compareTo(primitiveValues2[index].toString());
                                    if (comp != 0)
                                        return -1 * comp;
                                    else
                                        continue;
                                } else if (primitiveValues1[index] instanceof LongValue) {
                                    int comp = Long.valueOf(primitiveValues1[index].toLong()).compareTo(Long.valueOf(primitiveValues2[index].toLong()));
                                    if (comp != 0)
                                        return -1 * comp;
                                    else
                                        continue;
                                } else if (primitiveValues1[index] instanceof DoubleValue) {
                                    int comp = Double.valueOf(primitiveValues1[index].toDouble()).compareTo(Double.valueOf(primitiveValues2[index].toDouble()));
                                    if (comp != 0)
                                        return -1 * comp;
                                    else
                                        continue;
                                } else if (primitiveValues1[index] instanceof DateValue) {
                                    int comp = ((DateValue) primitiveValues1[index]).getValue().compareTo(((DateValue) primitiveValues1[index]).getValue());
                                    if (comp != 0)
                                        return -1 * comp;
                                    else
                                        continue;
                                }
                            }
                        } catch (Exception e) {

                        }

                    }
                    return -1;
                }
            });
    //endregion

    public Sort(RAIterator child, List<Column> columnList, List<Boolean> orderOfOrderByElements, List<Integer> indexOfOrderByElements, Boolean isSourceFile) {
        this.child = child;
        this.allColumns = columnList;
        this.orderOfOrderByElements = orderOfOrderByElements; // TODO: Not being used.
        this.indexOfOrderByElements = indexOfOrderByElements;
        this.isSourceFile = isSourceFile;

       /* if (onExpression == null) { // TODO: SortMerge should not be called for cross join

        } else {
            this.allColumns = commonLib.getColumnList(onExpression);
        }*/

        initializeVariables();

    }

    public List<Integer> getIndexOfSortKey() {
        return indexOfSortKey;
    }


    public void sort() throws Exception {


        if (isSourceFile) {

            List<PrimitiveValue[]> sortedList = new ArrayList<PrimitiveValue[]>();
            PrimitiveValue[] line;

            fileIterators = new FileIterator[N];
            for (int i = 0; i < N; i++) {
                writtenFileName = fileName + "_SORT_MERGE_" + commonLib.getSortMergeSeqNumber();
                fileIterators[i] = new FileIterator(fileName, writtenFileName);
            }

            BufferedReader br = new BufferedReader(new FileReader(path + fileName + ".csv"));

            while (true) {
                if (rowCounter >= blockSize) {
                    sortList(sortedList);
                    fileIterators[currFileIndex].writeDataDisk(sortedList);
                    synchronized (this) {
                        currFileIndex++;
                    }
                    rowCounter = 0;
                    sortedList.clear();
                }

                if ((line = commonLib.covertTupleToPrimitiveValue(br.readLine(), columnDefinitions)) != null) {
                    sortedList.add(line);
                    rowCounter++;
                } else {
                    if (sortedList.size() > 0) {
                        sortList(sortedList);
                        fileIterators[currFileIndex].writeDataDisk(sortedList);
                        synchronized (this) {
                            currFileIndex++;
                        }
                        rowCounter = 0;
                        sortedList.clear();
                    }
                    break;
                }
            }

            fillPriorityQueueWithFirstIndex();

        } else {

            PrimitiveValue[] line;
            List<PrimitiveValue[]> sortedList = new ArrayList<PrimitiveValue[]>();

            fileIterators = new FileIterator[N];
            for (int i = 0; i < N; i++) {
                writtenFileName = fileName + "_ORDER_BY_" + commonLib.getOrderBySeqNumber();
                fileIterators[i] = new FileIterator(fileName, writtenFileName);
            }

            while (child.hasNext()) {
                if (rowCounter >= blockSize) {
                    sortList(sortedList);
                    fileIterators[currFileIndex].writeDataDisk(sortedList);
                    synchronized (this) {
                        currFileIndex++;
                    }
                    rowCounter = 0;
                    sortedList.clear();
                }

                if ((line = child.next()) != null) {
                    sortedList.add(line);
                    rowCounter++;
                } else {
                    if (sortedList.size() > 0) {
                        sortList(sortedList);
                        fileIterators[currFileIndex].writeDataDisk(sortedList);
                        synchronized (this) {
                            currFileIndex++;
                        }
                        rowCounter = 0;
                        sortedList.clear();
                    }
                    break;
                }
            }
            if(sortedList.size() > 0){
                sortList(sortedList);
                fileIterators[currFileIndex].writeDataDisk(sortedList);
                synchronized (this) {
                    currFileIndex++;
                }
                rowCounter = 0;
                sortedList.clear();
            }
            fillPriorityQueueWithFirstIndex();
        }

    }

    private void fillPriorityQueueWithFirstIndex() {
        HashMap<Integer, PrimitiveValue[]> hashMap;
        for (int i = 0; i < currFileIndex; i++) {
            PrimitiveValue[] primitiveValues = fileIterators[i].getNext();
            hashMap = new HashMap<Integer, PrimitiveValue[]>();
            hashMap.put(i, primitiveValues);
            priorityQueue.add(hashMap);
        }
    }

    private void initializeVariables() {
        Schema[] schemas = child.getSchema();
        indexOfSortKey = new ArrayList<Integer>();

        List<String> allColumnList = new ArrayList<String>();
        for (int i = 0; i < allColumns.size(); i++) {
            allColumnList.add(allColumns.get(i).getTable() + "." + allColumns.get(i).getColumnName());
        }

        /**
         *  Remove columns from allColumn list which are not part of current schema
         *  Also, extracts file name.
         */
        sortKeyColumns = new ArrayList<String>();
        for (int i = 0; i < schemas.length; i++) {
            String col = schemas[i].getTableName() + "." + schemas[i].getColumnDefinition().getColumnName();
            if (allColumnList.contains(col)) {
                sortKeyColumns.add(schemas[i].getColumnDefinition().getColumnName());

                if (fileName.equals("")) {
                    fileName = schemas[i].getTableName();
                }
            }
        }

        /**
         *  Index of sort keys in the current schema.
         */

        for (int position = 0; position < sortKeyColumns.size(); position++) {
            for (int index = 0; index < schemas.length; index++) {
                if (sortKeyColumns.get(position).equals(schemas[index].getColumnDefinition().getColumnName())) {
                    indexOfSortKey.add(index);
                    break;
                }
            }
        }

        /**
         *  Create column Definition array.
         */
        columnDefinitions = new ColumnDefinition[schemas.length];
        for (int i = 0; i < schemas.length; i++) {
            columnDefinitions[i] = schemas[i].getColumnDefinition();
        }
    }

    public PrimitiveValue[] getTuple() throws Exception {

        PrimitiveValue[] primitiveValues = null;
        Integer key = null;

        HashMap<Integer, PrimitiveValue[]> hashMap = priorityQueue.poll();
        if (hashMap == null && priorityQueue.size() > 0) {
            while (hashMap == null && priorityQueue.size() > 0)
                hashMap = priorityQueue.poll();
        }

        if (hashMap == null)
            return null;

        for (Map.Entry<Integer, PrimitiveValue[]> entry : hashMap.entrySet()) {
            key = entry.getKey();
            primitiveValues = entry.getValue();
            break;
        }

        PrimitiveValue[] newData = fileIterators[key].getNext(); //TODO: Need to handle null or does above code in while loop handles?
        if (newData != null) {
            HashMap<Integer, PrimitiveValue[]> newMap = new HashMap<Integer, PrimitiveValue[]>();
            newMap.put(key, newData);
            priorityQueue.add(newMap);
        }

        return primitiveValues;
    }

    private void sortList(List<PrimitiveValue[]> sortedData) {
        Collections.sort(sortedData, new Comparator<PrimitiveValue[]>() {
            @Override
            public int compare(PrimitiveValue[] a, PrimitiveValue[] b) {

                for (int i = 0; i < indexOfSortKey.size(); i++) {
                    int index = indexOfSortKey.get(i);

                    try {
                        if (orderOfOrderByElements == null || orderOfOrderByElements.get(i)) {

                            if (a[index] instanceof StringValue) {
                                int comp = a[index].toString().compareTo(b[index].toString());
                                if (comp != 0)
                                    return comp;
                                else
                                    continue;
                            } else if (a[index] instanceof LongValue) {
                                int comp = Long.valueOf(a[index].toLong()).compareTo(Long.valueOf(b[index].toLong()));
                                if (comp != 0)
                                    return comp;
                                else
                                    continue;
                            } else if (a[index] instanceof DoubleValue) {
                                int comp = Double.valueOf(a[index].toDouble()).compareTo(Double.valueOf(b[index].toDouble()));
                                if (comp != 0)
                                    return comp;
                                else
                                    continue;
                            } else if (a[index] instanceof DateValue) {
                                int comp = ((DateValue) a[index]).getValue().compareTo(((DateValue) b[index]).getValue());
                                if (comp != 0)
                                    return comp;
                                else
                                    continue;
                            }
                        } else {
                            if (a[index] instanceof StringValue) {
                                int comp = a[index].toString().compareTo(b[index].toString());
                                if (comp != 0)
                                    return -1 * comp;
                                else
                                    continue;
                            } else if (a[index] instanceof LongValue) {
                                int comp = Long.valueOf(a[index].toLong()).compareTo(Long.valueOf(b[index].toLong()));
                                if (comp != 0)
                                    return -1 * comp;
                                else
                                    continue;
                            } else if (a[index] instanceof DoubleValue) {
                                int comp = Double.valueOf(a[index].toDouble()).compareTo(Double.valueOf(b[index].toDouble()));
                                if (comp != 0)
                                    return -1 * comp;
                                else
                                    continue;
                            } else if (a[index] instanceof DateValue) {
                                int comp = ((DateValue) a[index]).getValue().compareTo(((DateValue) b[index]).getValue());
                                if (comp != 0)
                                    return -1 * comp;
                                else
                                    continue;
                            }
                        }

                    } catch (Exception e) {

                    }
                }
                return -1;
            }
        });
    }

    public void reset() throws Exception {

        for (int i = 0; i < currFileIndex; i++) {
            fileIterators[i].reset();
        }

        fillPriorityQueueWithFirstIndex();
    }
}
