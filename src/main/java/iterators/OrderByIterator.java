package iterators;

import dubstep.AppMain;
import helpers.PrimitiveValueWrapper;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.io.*;
import java.util.*;

import dubstep.AppMain.*;

public class OrderByIterator implements RAIterator {

    //region Variables

    volatile int fileNumber = 1000;
    volatile int mergedSeqNumber = 10000;
    int blockSize = 4;
    boolean sorted = false;
    String path = "/Users/deepak/Desktop/Database/data/";
    private RAIterator currentIterator;
    private List<OrderByElement> orderByElementsList;
    private List<List<PrimitiveValueWrapper>> sortedList = new ArrayList<List<PrimitiveValueWrapper>>();
    private List<Integer> indexOfOrderByElements;
    private List<Boolean> orderOfOrderByElements; // asc : true, desc : false
    private int currentIndex = 0;
    private BufferedReader brMergedFile1;
    private BufferedReader brMergedFile2;
    private boolean onDiskSorted = false;
    private List<String> onDiskSortedList = new ArrayList<String>();
    private boolean noDataFound = false;
    private int onDiscRowToReturn = 0;

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


        if (!AppMain.inMem) {
            if (onDiskSorted) {
                if (noDataFound)
                    return false;
                else
                    return true;
            }
        }

        if (sorted)
            if (sortedList.size() > currentIndex)
                return true;
            else
                return false;

        return currentIterator.hasNext();

    }

    @Override
    public PrimitiveValueWrapper[] next() throws Exception {

        if (AppMain.inMem) {

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

        } else {

            if (onDiskSorted) {

                if (onDiscRowToReturn >= blockSize) {
                    sortWithoutMerge();
                    onDiscRowToReturn = 0;
                    if (noDataFound)
                        return null;
                }

                String row[] = onDiskSortedList.get(onDiscRowToReturn).split("\\|");
                PrimitiveValueWrapper[] primitiveValueWrappers = new PrimitiveValueWrapper[row.length];
                for (int i = 0; i < row.length; i++) {
                    primitiveValueWrappers[i] = new PrimitiveValueWrapper();
                    primitiveValueWrappers[i].setPrimitiveValue(new StringValue(row[i]));
                }
                onDiscRowToReturn++;

                return primitiveValueWrappers;
            }

            int rowCount = 0;
            List<String> listOfSortedFiles = new ArrayList<String>();
            while (currentIterator.hasNext()) {
                sortedList.add(Arrays.asList(currentIterator.next()));
                rowCount++;

                if (rowCount >= blockSize || !currentIterator.hasNext()) {

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

                    String file = "SORTED_FILE_" + fileNumber++;
                    writeDataDisk(file);
                    listOfSortedFiles.add(file);
                    sortedList.clear();
                    rowCount = 0;
                }
            }

            List<String> mergedFileLists = new ArrayList<String>();

            while (listOfSortedFiles.size() > 0) {
                if (listOfSortedFiles.size() % 2 == 0) {

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
                    if (listOfSortedFiles.size() == 0) {
                        listOfSortedFiles.addAll(mergedFileLists);
                        mergedFileLists.clear();
                    }

                } else {

                }
            }

            onDiskSorted = true;

            brMergedFile1 = new BufferedReader(new FileReader(path + mergedFileLists.get(0)));
            brMergedFile2 = new BufferedReader(new FileReader(path + mergedFileLists.get(1)));

            sortWithoutMerge();
        }

        String row[] = onDiskSortedList.get(onDiscRowToReturn).split("\\|");
        PrimitiveValueWrapper[] primitiveValueWrappers = new PrimitiveValueWrapper[row.length];
        for (int i = 0; i < row.length; i++) {
            primitiveValueWrappers[i] = new PrimitiveValueWrapper();
            primitiveValueWrappers[i].setPrimitiveValue(new StringValue(row[i]));
        }
        onDiscRowToReturn++;

        return primitiveValueWrappers;
    }

    private void sortWithoutMerge() {
        int cnt = 0;
        String line1 = null;
        String line2 = null;
        try {
            while ((line1 = brMergedFile1.readLine()) != null && (line2 = brMergedFile2.readLine()) != null) {
                if (cnt < blockSize) {
                    onDiskSortedList.add(line1);
                    onDiskSortedList.add(line2);

                    cnt += 2;
                    if(cnt >= blockSize){
                        sortList(onDiskSortedList);
                        return;
                    }
                }
            }
            if (line1 != null) {
                while ((line1 = brMergedFile1.readLine()) != null) {
                    if (cnt < blockSize) {
                        onDiskSortedList.add(line1);
                        cnt++;
                        if(cnt >= blockSize){
                            sortList(onDiskSortedList);
                            return;
                        }
                    }
                }

            } else if (line2 != null) {

                while ((line2 = brMergedFile2.readLine()) != null) {
                    if (cnt < blockSize) {
                        onDiskSortedList.add(line2);
                        cnt++;
                        if(cnt >= blockSize){
                            sortList(onDiskSortedList);
                            return;
                        }
                    }
                }
            }
            if (line1 == null && line2 == null) {
                noDataFound = true;
                return;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void merge(String firstFile, String secondFile, String filename) {

        int cnt = 0;

        List<String> sortedData = new ArrayList<String>();

        try {
            BufferedReader brFirstFile = new BufferedReader(new FileReader(path + firstFile));
            BufferedReader brSecondFile = new BufferedReader(new FileReader(path + secondFile));
            String line1 = null;
            String line2 = null;
            while ((line1 = brFirstFile.readLine()) != null && (line2 = brSecondFile.readLine()) != null) {
                if (cnt < blockSize) {
                    sortedData.add(line1);
                    sortedData.add(line2);

                    cnt += 2;
                } else {

                    sortList(sortedData);
                    writeDataDisk(filename, sortedData);
                    sortedData.clear();
                    sortedData.add(line1);
                    sortedData.add(line2);
                    cnt = 2;
                }
            }
            if (line1 != null) {
                do {
                    if (cnt < blockSize) {
                        sortedData.add(line1);
                        cnt++;
                    } else {
                        sortList(sortedData);
                        writeDataDisk(filename, sortedData);
                        sortedData.clear();
                        cnt = 0;
                    }
                } while ((line1 = brFirstFile.readLine()) != null);

            } else if (line2 != null) {

                do {
                    if (cnt < blockSize) {
                        sortedData.add(line2);
                        cnt++;
                    } else {
                        sortList(sortedData);
                        writeDataDisk(filename, sortedData);
                        sortedData.clear();
                        cnt = 0;
                    }
                } while ((line2 = brSecondFile.readLine()) != null);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


        //return filename;
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
            for (List<PrimitiveValueWrapper> list : sortedList) {
                for (PrimitiveValueWrapper data : list)
                    dataToWrite.append("|" + data.getPrimitiveValue().toRawString());
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

        currentIterator.reset();

    }

    //endregion
}
