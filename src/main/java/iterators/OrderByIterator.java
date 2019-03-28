package iterators;

import dubstep.AppMain;
import helpers.Schema;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.io.*;
import java.util.*;


public class OrderByIterator implements RAIterator {

    //region Variables

    boolean sorted = false;
    volatile int fileNumber = 1000;
    volatile int mergedSeqNumber = 10000;
    int blockSize = 4;
    String path = TableIterator.TABLE_DIRECTORY;
    private RAIterator child;
    private List<OrderByElement> orderByElementsList;
    private List<List<PrimitiveValue>> sortedList = new ArrayList<List<PrimitiveValue>>();
    private List<Integer> indexOfOrderByElements;
    private List<Boolean> orderOfOrderByElements; // asc : true, desc : false
    private int currentIndex = 0;
    private Schema[] schema;
    // On disk variables
    private boolean onDiskSorted = false;
    private List<String> onDiskSortedList = new ArrayList<String>();
    private boolean noDataFound = false;
    private int onDiscRowToReturn = 0;
    private BufferedReader brMergedFile1;
    private BufferedReader brMergedFile2;
    private List<String> mergedFileLists = new ArrayList<String>();

    private String leftData;
    private String rightData;

    // On disk variables ends here

    //endregion

    //region Constructor

    public OrderByIterator(RAIterator child, List<OrderByElement> orderByElementsList, List<Integer> indexOfOrderByElements, List<Boolean> orderOfOrderByElements) {

        this.child = child;
        this.orderByElementsList = orderByElementsList;
        this.indexOfOrderByElements = indexOfOrderByElements;
        this.orderOfOrderByElements = orderOfOrderByElements;
        this.schema = child.getSchema();
    }

    //endregion

    //region Iterator methods

    public static boolean isNumber(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Override
    public boolean hasNext() throws Exception {


        if (!AppMain.inMem) {
            if (onDiskSorted) {
                if (noDataFound)
                    return false;
                else
                    return true;
            }
        } else {
            if (sorted)
                if (sortedList.size() > currentIndex)
                    return true;
                else
                    return false;
        }
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
                PrimitiveValue[] tuple = child.next();
                if (tuple == null)
                    continue;
                sortedList.add(Arrays.asList(tuple));
            }

            Collections.sort(sortedList, new Comparator<List<PrimitiveValue>>() {
                @Override
                public int compare(List<PrimitiveValue> first, List<PrimitiveValue> second) {

                    int i = 0;

                    for (Integer index : indexOfOrderByElements) {
                        String primitiveValue1 = first.get(index).toRawString();
                        String primitiveValue2 = second.get(index).toRawString();

                        if (isNumber(primitiveValue1)) {

                            double pv1 = Double.parseDouble(primitiveValue1);
                            double pv2 = Double.parseDouble(primitiveValue2);

                            if (orderOfOrderByElements.get(i++)) {

                                if (pv1 < pv2)
                                    return -1;
                                else if (pv1 > pv2)
                                    return 1;
                                else {
                                    continue;
                                }

                            } else {

                                if (pv1 < pv2)
                                    return 1;
                                else if (pv1 > pv2)
                                    return -1;
                                else {
                                    continue;
                                }
                            }

                        } else {

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

                    }
                    return 1;
                }
            });

            sorted = true;

            PrimitiveValue[] primitiveValueWrappers = sortedList.get(currentIndex).toArray(new PrimitiveValue[sortedList.get(0).size()]);
            currentIndex++;

            return primitiveValueWrappers;

        } else {
            if (onDiskSorted) {

                if (onDiscRowToReturn >= blockSize) {
                    sortWithoutMerge();
                    onDiscRowToReturn = 0;
                    if (noDataFound)
                        return null;
                }

                if(onDiscRowToReturn == onDiskSortedList.size()) {
                    noDataFound = true;
                    return null;
                }

                String row[] = onDiskSortedList.get(onDiscRowToReturn).split("\\|");
                PrimitiveValue[] primitiveValue = new PrimitiveValue[row.length];
                for (int i = 0; i < row.length; i++) {
                    primitiveValue[i] = new StringValue(row[i]);
                }
                onDiscRowToReturn++;

                return primitiveValue;
            }

            int rowCount = 0;
            List<String> listOfSortedFiles = new ArrayList<String>();
            while (child.hasNext()) {
                sortedList.add(Arrays.asList(child.next()));
                rowCount++;

                if (rowCount >= blockSize || !child.hasNext()) {

                    Collections.sort(sortedList, new Comparator<List<PrimitiveValue>>() {
                        @Override
                        public int compare(List<PrimitiveValue> first, List<PrimitiveValue> second) {

                            int i = 0;

                            for (Integer index : indexOfOrderByElements) {
                                String primitiveValue1 = first.get(index).toRawString();
                                String primitiveValue2 = second.get(index).toRawString();


                                if (isNumber(primitiveValue1)) {

                                    double pv1 = Double.parseDouble(primitiveValue1);
                                    double pv2 = Double.parseDouble(primitiveValue2);

                                    if (orderOfOrderByElements.get(i++)) {

                                        if (pv1 < pv2)
                                            return -1;
                                        else if (pv1 > pv2)
                                            return 1;
                                        else {
                                            continue;
                                        }

                                    } else {

                                        if (pv1 < pv2)
                                            return 1;
                                        else if (pv1 > pv2)
                                            return -1;
                                        else {
                                            continue;
                                        }
                                    }

                                } else {


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

                            }
                            return 1;
                        }
                    });

                    String file = "SORTED_FILE_" + fileNumber++;
                    writeDataDisk(file);
                    listOfSortedFiles.add(file);
                    sortedList.clear();
                    rowCount = 0;
                }
            }


            while (listOfSortedFiles.size() > 0) {
                if (listOfSortedFiles.size() >= 2) {

                    String firstFile = listOfSortedFiles.get(0);
                    String secondFile = listOfSortedFiles.get(1);

                    listOfSortedFiles.remove(1);
                    listOfSortedFiles.remove(0);

                    String filename = "MERGED_FILE_" + mergedSeqNumber++;
                    merge(firstFile, secondFile, filename);
                    mergedFileLists.add(filename);

                    if (listOfSortedFiles.size() == 0 && mergedFileLists.size() == 2) {
                        // All data are sorted now and are in two final merged files.
                        break;
                    }
                    if(listOfSortedFiles.size() == 1 && mergedFileLists.size() == 1) {
                        mergedFileLists.add(listOfSortedFiles.get(0));
                        listOfSortedFiles.remove(0);
                        break;
                    }

                    if(listOfSortedFiles.size() == 1 && mergedFileLists.size() > 0){
                        listOfSortedFiles.addAll(mergedFileLists);
                        mergedFileLists.clear();
                    }
                    if (listOfSortedFiles.size() == 0) {
                        listOfSortedFiles.addAll(mergedFileLists);
                        mergedFileLists.clear();
                    }

                } else if(listOfSortedFiles.size() == 1) { // Not yet tested.
                    mergedFileLists.add(listOfSortedFiles.get(0));
                    listOfSortedFiles.remove(0);
                    break;
                }
            }

            onDiskSorted = true;

            if(mergedFileLists.size() == 1){
                File file = new File(path + "dummy");
                file.createNewFile();
                mergedFileLists.add("dummy");
            }

            brMergedFile1 = new BufferedReader(new FileReader(path + mergedFileLists.get(0)));
            brMergedFile2 = new BufferedReader(new FileReader(path + mergedFileLists.get(1)));

            sortWithoutMerge(); // Creates a sorted block from final two merged files
        }

        String row[] = onDiskSortedList.get(onDiscRowToReturn).split("\\|");
        PrimitiveValue[] primitiveValue = new PrimitiveValue[row.length];
        for (int i = 0; i < row.length; i++) {
            primitiveValue[i] = new StringValue(row[i]);

        }
        onDiscRowToReturn++;

        return primitiveValue;
    }

    private void sortWithoutMerge() { // TODO: Might have a bug to fix.

        int cnt = 0;
        onDiskSortedList.clear();

        try {
            if(leftData == null)
                leftData = getLeftData();
            if(rightData == null)
                rightData = getRightData();

            while (leftData != null && rightData != null) {
                if (cnt < blockSize) {
                    //onDiskSortedList.add(leftData);

                    if (isLeftGreater(leftData, rightData)) {
                        onDiskSortedList.add(leftData);
                        leftData = getLeftData();
                        cnt++;
                    } else if (!isLeftGreater(leftData, rightData)) {
                        onDiskSortedList.add(rightData);
                        rightData = getRightData();
                        cnt++;
                    } else { // TODO: Handle for Equality.
                        onDiskSortedList.add(leftData);
                        onDiskSortedList.add(rightData);
                        cnt += 2;
                    }

                    if (cnt >= blockSize) {
                        sortList(onDiskSortedList);
                        return;
                    }
                }
            }
            if (leftData != null) {
                while (leftData != null) {
                    if (cnt < blockSize) {
                        onDiskSortedList.add(leftData);
                        leftData = getLeftData();
                        cnt++;
                        if (cnt >= blockSize) {
                            sortList(onDiskSortedList);
                            return;
                        }

                    }
                }

            } else if (rightData != null) {
                while (rightData != null) {
                    if (cnt < blockSize) {
                        onDiskSortedList.add(rightData);
                        rightData = getRightData();
                        cnt++;
                        if (cnt >= blockSize) {
                            sortList(onDiskSortedList);
                            return;
                        }
                    }
                }
            }

            if (leftData == null && rightData == null && onDiskSortedList.size() > 0) {
                sortList(onDiskSortedList);
                return;
            }

            if (leftData == null && rightData == null) {
                noDataFound = true;
                return;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean isLeftGreater(String leftData, String rightData) {


        String first[] = leftData.split("\\|");
        String second[] = rightData.split("\\|");

        int i = 0;

        for (Integer index : indexOfOrderByElements) {
            String primitiveValue1 = first[index];
            String primitiveValue2 = second[index];

            if (isNumber(primitiveValue1)) {

                double pv1 = Double.parseDouble(primitiveValue1);
                double pv2 = Double.parseDouble(primitiveValue2);

                if (orderOfOrderByElements.get(i++)) {

                    if (pv1 < pv2)
                        return true;
                    else if (pv1 > pv2)
                        return false;
                    else {
                        continue;
                    }

                } else {

                    if (pv1 < pv2)
                        return false;
                    else if (pv1 > pv2)
                        return true;
                    else {
                        continue;
                    }
                }

            } else {

                if (orderOfOrderByElements.get(i++)) {

                    if (primitiveValue1.compareTo(primitiveValue2) > 0)
                        return true;
                    else if (primitiveValue1.compareTo(primitiveValue2) < 0)
                        return false;
                    else {
                        continue;
                    }

                } else {

                    if (primitiveValue1.compareTo(primitiveValue2) > 0)
                        return false;
                    else if (primitiveValue1.compareTo(primitiveValue2) < 0)
                        return true;
                    else {
                        continue;
                    }
                }
            }

        }


        return true; // TODO: DEFAULT returns true.
    }

    private String getLeftData() {

        try {
            return brMergedFile1.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null; // Should not come here.
    }

    private String getRightData() {

        try {
            return brMergedFile2.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null; // Should not come here.
    }

    private void merge(String firstFile, String secondFile, String filename) {

        int cnt = 0;

        List<String> sortedData = new ArrayList<String>();

        try {
            BufferedReader brFirstFile = new BufferedReader(new FileReader(path + firstFile));
            BufferedReader brSecondFile = new BufferedReader(new FileReader(path + secondFile));
            String line1 = brFirstFile.readLine();
            String line2 = brSecondFile.readLine();
            while (line1 != null && line2 != null) { // TODO: Should be merged after checking orderOfOrderByElements
                if (cnt < blockSize) {

                    if (isLeftGreater(line1, line2)) {
                        sortedData.add(line1);
                        line1 = brFirstFile.readLine();
                        cnt++;
                    } else if (!isLeftGreater(line1, line2)) {
                        sortedData.add(line2);
                        line2 = brSecondFile.readLine();
                        cnt++;
                    } else {
                        sortedData.add(line1);
                        sortedData.add(line2);
                        cnt += 2;
                    }

                } else {

                    sortList(sortedData);
                    writeDataDisk(filename, sortedData);
                    sortedData.clear();
                    sortedData.add(line1);
                    sortedData.add(line2);
                    line1 = brFirstFile.readLine();
                    line2 = brSecondFile.readLine();
                    cnt = 2;
                }
            }
            if (line1 != null) {
                while(line1 != null){
                    if (cnt < blockSize) {
                        sortedData.add(line1);
                        cnt++;
                        line1 = brFirstFile.readLine();
                    } else {
                        sortList(sortedData);
                        writeDataDisk(filename, sortedData);
                        sortedData.clear();
                        cnt = 0;
                    }
                }

            } else if (line2 != null) {
                while(line2 != null){
                    if (cnt < blockSize) {
                        sortedData.add(line2);
                        cnt++;
                        line2 = brSecondFile.readLine();
                    } else {
                        sortList(sortedData);
                        writeDataDisk(filename, sortedData);
                        sortedData.clear();
                        cnt = 0;
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        if(sortedData.size() > 0)
            writeDataDisk(filename, sortedData);
    }

    private void writeDataDisk(String filename, List<String> sortedData) {

        File file = new File(path + filename);

        FileWriter fileWriter = null;
        BufferedWriter bufferedWriter = null;

        try {
            bufferedWriter = new BufferedWriter(new FileWriter(file, true));
            for (String data : sortedData) {
                bufferedWriter.write(data + "\n");
            }
            bufferedWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fileWriter != null) {
                    fileWriter.close();
                }
                if (bufferedWriter != null) {
                    bufferedWriter.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void sortList(List<String> sortedData) { // TODO: Fix it. It currently sorts on string ordering. Change it to primitive values
        Collections.sort(sortedData, new Comparator<String>() {
            @Override
            public int compare(String a, String b) {

                String first[] = a.split("\\|");
                String second[] = b.split("\\|");

                int i = 0;

                for (Integer index : indexOfOrderByElements) {
                    String val1 = first[index];
                    String val2 = second[index];


                    if (isNumber(val1)) {

                        double pv1 = Double.parseDouble(val1);
                        double pv2 = Double.parseDouble(val2);

                        if (orderOfOrderByElements.get(i++)) {

                            if (pv1 < pv2)
                                return -1;
                            else if (pv1 > pv2)
                                return 1;
                            else {
                                continue;
                            }

                        } else {

                            if (pv1 < pv2)
                                return 1;
                            else if (pv1 > pv2)
                                return -1;
                            else {
                                continue;
                            }
                        }

                    } else {


                        if (orderOfOrderByElements.get(i++)) {

                            if (val1.compareTo(val2) != 0)
                                return val1.compareTo(val2);
                            else {
                                continue;
                            }

                        } else {

                            if (val1.compareTo(val2) != 0)
                                return -1 * val1.compareTo(val2);
                            else {
                                continue;
                            }
                        }
                    }
                }
                return 1;
            }
        });
    }

    private void writeDataDisk(String filename) {

        File file = new File(path + filename);

        FileWriter fileWriter = null;
        BufferedWriter bufferedWriter = null;

        StringBuilder dataToWrite = new StringBuilder();
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(file, true));
            for (List<PrimitiveValue> list : sortedList) {
                for (PrimitiveValue data : list)
                    dataToWrite.append("|" + data.toRawString());
                bufferedWriter.write(dataToWrite.toString().substring(1) + "\n");
                dataToWrite = new StringBuilder();
            }
            bufferedWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fileWriter != null) {
                    fileWriter.close();
                }
                if (bufferedWriter != null) {
                    bufferedWriter.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void reset() throws Exception {
        child.reset();
    }

    @Override
    public RAIterator getChild() {
        return this.child;
    }

    @Override
    public void setChild(RAIterator child) {
        this.child = child;
    }

    @Override
    public Schema[] getSchema() {
        return this.schema;
    }

    @Override
    public void setSchema(Schema[] schema) {
        this.schema = schema;
    }

    @Override
    public RAIterator optimize(RAIterator iterator)
    {
        RAIterator child = iterator.getChild();
        child = child.optimize(child);
        iterator.setChild(child);
        return iterator;
    }

    //endregion
}
