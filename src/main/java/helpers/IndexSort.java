package helpers;

import iterators.RAIterator;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;


public class IndexSort {

    String newData = null;
    private List<Column> allColumns;
    private List<String> sortKeyColumns;
    private CommonLib commonLib = CommonLib.getInstance();
    private RAIterator child;
    private List<PrimitiveValue> sortKey;
    private List<Integer> indexOfSortKey;
    private String fileName = "";
    private String path = CommonLib.TABLE_DIRECTORY;
    private ColumnDefinition[] columnDefinitions;
    private int rowCounter = 0;
    private List<IndexFileIterator> fileIterators = new ArrayList<IndexFileIterator>();
    private volatile int currFileIndex = 0;
    private long blockSize = commonLib.blockSize;
    private List<Boolean> orderOfOrderByElements;
    private List<Integer> indexOfOrderByElements;
    private Integer temppppp = 0;
    private Boolean isSourceFile;
    private int initialCapacity = 20; // TODO: what's good number?


    //region Priority Queue
    private PriorityQueue<HashMap<Integer, String>> priorityQueue = new PriorityQueue<HashMap<Integer, String>>(initialCapacity,
            new Comparator<HashMap<Integer, String>>() {
                @Override
                public int compare(HashMap<Integer, String> o1, HashMap<Integer, String> o2) {
                    String a[] = null;
                    String b[] = null;

                    for (Map.Entry<Integer, String> entry1 : o1.entrySet()) {
                        a = entry1.getValue().split("\\|");
                        break;
                    }

                    for (Map.Entry<Integer, String> entry2 : o2.entrySet()) {
                        b = entry2.getValue().split("\\|");
                        break;
                    }

                    for (int i = 0; i < indexOfSortKey.size(); i++) {
                        int index = indexOfSortKey.get(i);

                        try {

                            if (orderOfOrderByElements == null || orderOfOrderByElements.get(i)) {

                                if (isNumber(a[index])) {

                                    double pv1 = Double.parseDouble(a[index]);
                                    double pv2 = Double.parseDouble(b[index]);

                                    if (pv1 < pv2)
                                        return -1;
                                    else if (pv1 > pv2)
                                        return 1;
                                    else {
                                        continue;
                                    }

                                } else {

                                    return a[index].compareTo(b[index]);
                                }

                            } else {

                            }
                        } catch (Exception e) {

                        }

                    }
                    return -1;
                }
            });
    //endregion

    public IndexSort(RAIterator child, List<Column> columnList, List<Boolean> orderOfOrderByElements, List<Integer> indexOfOrderByElements, Boolean isSourceFile) {
        this.child = child;
        this.allColumns = columnList;
        this.orderOfOrderByElements = orderOfOrderByElements; // TODO: Not being used.
        this.indexOfOrderByElements = indexOfOrderByElements;
        this.isSourceFile = isSourceFile;

        initializeVariables();

    }

    public static boolean isNumber(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public void sort() throws Exception {


        if (isSourceFile) {

            List<String> sortedList = new ArrayList<String>();
            String line;

            //fileIterators.add(new FileIterator(fileName, fileName + "SORT_MERGE" + commonLib.getSortMergeSeqNumber()));

            BufferedReader br = new BufferedReader(new FileReader(path + fileName + ".csv"));

            while (true) {
                if (rowCounter >= blockSize) {
                    sortList(sortedList);
                    fileIterators.add(new IndexFileIterator(fileName, fileName + "_NEW_SORT_MERGE" + commonLib.getSortMergeSeqNumber()));
                    fileIterators.get(currFileIndex).writeDataDisk(sortedList);
                    synchronized (this) {
                        currFileIndex++;
                    }
                    rowCounter = 0;
                    sortedList.clear();
                    Runtime r = Runtime.getRuntime();
                    r.gc();
                }

                if ((line = br.readLine()) != null) {
                    sortedList.add(line);
                    rowCounter++;
                } else {
                    if (sortedList.size() > 0) {
                        sortList(sortedList);
                        fileIterators.add(new IndexFileIterator(fileName, fileName + "_NEW_SORT_MERGE" + commonLib.getSortMergeSeqNumber()));
                        fileIterators.get(currFileIndex).writeDataDisk(sortedList);
                        synchronized (this) {
                            currFileIndex++;
                        }
                        rowCounter = 0;
                        sortedList.clear();
                        Runtime r = Runtime.getRuntime();
                        r.gc();
                    }
                    break;
                }
            }

            fillPriorityQueueWithFirstIndex();

        }
    }

    private void fillPriorityQueueWithFirstIndex() {
        HashMap<Integer, String> hashMap;
        for (int i = 0; i < currFileIndex; i++) {
            String nextValue = fileIterators.get(i).getNext();
            hashMap = new HashMap<Integer, String>();
            hashMap.put(i, nextValue);
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

    public String getTuple() throws Exception {

        String primitiveValues = null;
        Integer key = null;

        HashMap<Integer, String> hashMap = priorityQueue.poll();
        if (hashMap == null && priorityQueue.size() > 0) {
            while (hashMap == null && priorityQueue.size() > 0)
                hashMap = priorityQueue.poll();
        }

        if (hashMap == null) {
            if (primitiveValues == null) {
                for (int i = 0; i < currFileIndex; i++) {
                    fileIterators.get(i).clearAll();
                }
                fileIterators.clear();
            }
            return null;
        }

        for (Map.Entry<Integer, String> entry : hashMap.entrySet()) {
            key = entry.getKey();
            primitiveValues = entry.getValue();
            break;
        }

        newData = fileIterators.get(key).getNext(); //TODO: Need to handle null or does above code in while loop handles?
        if (newData != null) {
            HashMap<Integer, String> newMap = new HashMap<Integer, String>();
            newMap.put(key, newData);
            priorityQueue.add(newMap);
        } else {
            fileIterators.get(key).clearAll();
        }

        if (primitiveValues == null) {
            for (int i = 0; i < currFileIndex; i++) {
                fileIterators.get(i).clearAll();
            }
            fileIterators.clear();
        }
        return primitiveValues;
    }

    private void sortList(List<String> sortedData) {
        Collections.sort(sortedData, new Comparator<String>() {
            @Override
            public int compare(String a1, String b1) {

                String a[] = a1.split("\\|");
                String b[] = b1.split("\\|");

                for (int i = 0; i < indexOfSortKey.size(); i++) {
                    int index = indexOfSortKey.get(i);

                    try {
                        if (orderOfOrderByElements == null || orderOfOrderByElements.get(i)) {

                            if (isNumber(a[index])) {

                                double pv1 = Double.parseDouble(a[index]);
                                double pv2 = Double.parseDouble(b[index]);

                                if (pv1 < pv2)
                                    return -1;
                                else if (pv1 > pv2)
                                    return 1;
                                else {
                                    continue;
                                }

                            } else {

                                return a[index].compareTo(b[index]);
                            }

                        } else {

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
            fileIterators.get(i).reset();
        }

        fillPriorityQueueWithFirstIndex();
    }
}