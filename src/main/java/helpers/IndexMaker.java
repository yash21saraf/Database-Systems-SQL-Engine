package helpers;

import dubstep.Main;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.io.*;
import java.util.*;

public class IndexMaker {

    private static Integer indexBlockSize = 49999 ;
    private static Map<String, net.sf.jsqlparser.statement.create.table.Index> primaryIndexMap = new HashMap<String, net.sf.jsqlparser.statement.create.table.Index>();
    private static Map<String, ArrayList<net.sf.jsqlparser.statement.create.table.Index>> secondaryIndexMap = new HashMap<String, ArrayList<net.sf.jsqlparser.statement.create.table.Index>>();
    private static Map<String, ArrayList<Integer>> secondaryIndexColumnIndices = new HashMap<String, ArrayList<Integer>>();
    private static Map<String, ArrayList<Integer>> primaryIndexColumnIndices = new HashMap<String, ArrayList<Integer>>();
    private static boolean createTableCopy = false ;

    //////////////////////////////////////////////////////////////////
    private static ColumnDefinition[] columnDefinitions ;
    private static Tuple tupleClass;
    ////////////////////////////////////////////////////////////////


    public static void createIndex(CreateTable createTable) throws IOException {

        ////////////////////////////////////////////////////////////////////////////////////////
//        createTableCopy = true ;
//        columnDefinitions = createTable.getColumnDefinitions().toArray(new ColumnDefinition[0]);
//        tupleClass = new Tuple(columnDefinitions, createTable.getTable().getName());
        ////////////////////////////////////////////////////////////////////////////////////////

        if(createTable.getTable().getName().equals("LINEITEM")) {
            Main.create_lineitem_view = true;
            //return;
        }
        if(createTable.getTable().getName().equals("LINEITEM_VIEW"))
            return;

        ArrayList<net.sf.jsqlparser.statement.create.table.Index> listOfIndexes = (ArrayList<net.sf.jsqlparser.statement.create.table.Index>) createTable.getIndexes();
        String tableName = createTable.getTable().getName() ;

        indexesToSecondaryIndexes(tableName, listOfIndexes);
        indexColumnIndices(createTable, primaryIndexMap.get(tableName));
        indexColumnIndices(createTable, secondaryIndexMap.get(tableName));

        buildIndexes(createTable, tableName) ;
        writeToGlobalIndex(tableName, createTable);

    }

    private static void buildIndexes(CreateTable createTable, String tableName) throws IOException {
        ArrayList<Integer> columnIndices = new ArrayList<Integer>() ;
        ArrayList<String> columnNames = new ArrayList<String>() ;

        columnIndices.addAll(primaryIndexColumnIndices.get(tableName)) ;
        columnIndices.addAll(secondaryIndexColumnIndices.get(tableName));

        columnNames.addAll(primaryIndexMap.get(tableName).getColumnsNames()) ;

        for(net.sf.jsqlparser.statement.create.table.Index index : secondaryIndexMap.get(tableName)){
            columnNames.addAll(index.getColumnsNames()) ;
        }
        long st = System.currentTimeMillis();
        for(int i =0; i < columnIndices.size(); i++){
            String colDataType = createTable.getColumnDefinitions().get(columnIndices.get(i)).getColDataType().getDataType();
            buildColumnIndex(tableName, columnIndices.get(i), columnNames.get(i), colDataType);
        }
        long et = System.currentTimeMillis();
        System.out.println(et-st);
//
    }

    private static void buildColumnIndex(String tableName, Integer columnIndex, String columnName, String colDataType) throws IOException {

        String currentLineAsString ;
        TreeMap<String, ArrayList<String>> currentIndex ;
        if(validateDataType(colDataType)){
            currentIndex= new TreeMap<String, ArrayList<String>>(
                    new Comparator<String>() {
                        @Override
                        public int compare(String o1, String o2) {
                            double a = Double.parseDouble(o1);
                            double b = Double.parseDouble(o2);
                            if(a < b) {
                                return -1;
                            }
                            else if(a > b) {
                                return 1 ;
                            }
                            return 0;
                        }
                    }
            );
        }else{
            currentIndex= new TreeMap<String, ArrayList<String>>();
        }

        long position = 0;

        try {
            LineNumberReader br = new LineNumberReader(new FileReader(CommonLib.TABLE_DIRECTORY + tableName + CommonLib.extension), 100);

            if(createTableCopy){
                String newTableName = tableName + "_" + "NEW" ;
                File newTableFile = new File(CommonLib.INDEX_DIRECTORY + newTableName);
                FileOutputStream newTableFileOutputStream = new FileOutputStream(newTableFile);
                BufferedOutputStream newTableBufferedOutputStream = new BufferedOutputStream(newTableFileOutputStream, 5000);
                ObjectOutputStream newTableObjectOutputStream = new ObjectOutputStream(newTableFileOutputStream);
                PrimitiveValue[] currentRowPV ;

                while ((currentLineAsString = br.readLine()) != null) {
                    currentRowPV = tupleClass.covertTupleToPrimitiveValue(currentLineAsString) ;
                    newTableObjectOutputStream.writeUnshared(currentRowPV);

                    String[] currentLineAsStringArray = currentLineAsString.split("\\|") ;
                    String currentLineIndexCols = currentLineAsStringArray[columnIndex] ;


                    if(currentIndex.containsKey(currentLineIndexCols)){
                        currentIndex.get(currentLineIndexCols).add(Long.toString(position));
                    }else{
                        ArrayList<String> positionArray = new ArrayList<String>();
                        positionArray.add(Long.toString(position)) ;
                        currentIndex.put(currentLineIndexCols, positionArray) ;
                    }

                    position += currentLineAsString.length() + 1;
                }
                newTableObjectOutputStream.writeUnshared(null);
                newTableObjectOutputStream.close();
//                newTableBufferedOutputStream.close();
                newTableFileOutputStream.close() ;


                createTableCopy = false ;
            }else{
                while ((currentLineAsString = br.readLine()) != null) {

                    String[] currentLineAsStringArray = currentLineAsString.split("\\|") ;
                    String currentLineIndexCols = currentLineAsStringArray[columnIndex] ;


                    if(currentIndex.containsKey(currentLineIndexCols)){
                        currentIndex.get(currentLineIndexCols).add(Long.toString(position));
                    }else{
                        ArrayList<String> positionArray = new ArrayList<String>();
                        positionArray.add(Long.toString(position)) ;
                        currentIndex.put(currentLineIndexCols, positionArray) ;
                    }

                    position += currentLineAsString.length() + 1;
                }
            }

            br.close();
            br = null;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        writeIndexToDisk(currentIndex, tableName, columnName, colDataType);
    }

//    private static void writeIndexToDisk(TreeMap<String, ArrayList<String>> currentIndex, String tableName, String columnName) throws IOException {
//
//        String indexListName = tableName + "_" + columnName ;
//        BufferedWriter indexBW = new BufferedWriter(new FileWriter(CommonLib.TABLE_DIRECTORY + indexListName, true));
//        Integer lineNumber = 0;
//
//        String indexFileName = tableName + "_" + columnName + "_" + CommonLib.getsortFileSeqNumber();
//        BufferedWriter bw = new BufferedWriter(new FileWriter(CommonLib.TABLE_DIRECTORY + indexFileName, true));
//
//        // For master IndexMaker Writing
//        ArrayList<String[]> currentMapForMasterIndex = new ArrayList<String[]>();
//        String[] currentItemForMasterIndex = new String[3];
//        String firstKey = currentIndex.firstEntry().getKey();
//
//        for(Map.Entry<String, ArrayList<String>> entry : currentIndex.entrySet()){
//            String currentKey = entry.getKey();
//            for(String currentValue : entry.getValue()){
//                if(lineNumber < indexBlockSize) {
//                    bw.write(currentKey +"|" + currentValue + "\n");
//                }else if(lineNumber.equals(indexBlockSize)){
//                    bw.write(currentKey +"|" + currentValue + "\n");
//                    indexBW.write(firstKey + "|" + currentKey + "|" + indexFileName + "\n");
//                    currentItemForMasterIndex[0] = firstKey ;
//                    currentItemForMasterIndex[1] = currentKey ;
//                    currentItemForMasterIndex[2] = indexFileName ;
//                    currentMapForMasterIndex.add(currentItemForMasterIndex);
//                }else{
//                    bw.close() ;
//                    bw = null ;
//
//                    indexFileName = tableName + "_" + columnName + "_" + CommonLib.getsortFileSeqNumber();
//                    bw = new BufferedWriter(new FileWriter(CommonLib.TABLE_DIRECTORY + indexFileName, true));
//                    bw.write(currentKey +"|" + currentValue + "\n");
//                    firstKey = currentKey ;
//                    lineNumber = 0;
//                }
//                lineNumber +=1 ;
//            }
//        }
//        indexBW.write(firstKey + "|" + currentIndex.lastKey() + "|" + indexFileName + "\n");
//        indexBW.close();
//        currentItemForMasterIndex[0] = firstKey ;
//        currentItemForMasterIndex[1] = currentIndex.lastKey() ;
//        currentItemForMasterIndex[2] = indexFileName ;
//        currentMapForMasterIndex.add(currentItemForMasterIndex);
//        indexBW = null ;
//        bw.close();
//        bw = null ;
//        currentIndex.clear();
//        currentIndex = null ;
//        Main.masterIndex.put(tableName + "|" + columnName, currentMapForMasterIndex);
//    }

    private static void writeIndexToDisk(TreeMap<String, ArrayList<String>> currentIndex, String tableName, String columnName, String colDataType) throws IOException {

        String indexListName = tableName + "_" + columnName ;
        File indexFile = new File(CommonLib.INDEX_DIRECTORY + indexListName);
        FileOutputStream indexFileOutputStream = new FileOutputStream(indexFile);
        BufferedOutputStream indexBufferedOutputStream = new BufferedOutputStream(indexFileOutputStream);
        ObjectOutputStream indexBW = new ObjectOutputStream(indexBufferedOutputStream);

        Integer lineNumber = 0;

        String indexFileName = tableName + "_" + columnName + "_" + CommonLib.getsortFileSeqNumber();

        File file = new File(CommonLib.INDEX_DIRECTORY + indexFileName);
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
        ObjectOutputStream bw = new ObjectOutputStream(bufferedOutputStream);

        // For master IndexMaker Writing
        ArrayList<PrimitiveValue[]> currentMapForMasterIndex = new ArrayList<PrimitiveValue[]>();
        PrimitiveValue firstKey = CommonLib.convertToPrimitiveValue(currentIndex.firstEntry().getKey(), colDataType);
        PrimitiveValue[] writeValue = new PrimitiveValue[2] ;

        for(Map.Entry<String, ArrayList<String>> entry : currentIndex.entrySet()){
            PrimitiveValue currentKey = CommonLib.convertToPrimitiveValue(entry.getKey(), colDataType);
            for(String currentValue : entry.getValue()){
                if(lineNumber < indexBlockSize) {
                    writeValue[0] = currentKey;
                    writeValue[1] = CommonLib.convertToPrimitiveValue(currentValue, "INT");
                    bw.writeUnshared(writeValue);
                }else if(lineNumber.equals(indexBlockSize)){
                    writeValue[0] = currentKey;
                    writeValue[1] = CommonLib.convertToPrimitiveValue(currentValue, "INT");
                    bw.writeUnshared(writeValue);
                    PrimitiveValue[] currentItemForMasterIndex = new PrimitiveValue[3];
                    currentItemForMasterIndex[0] = firstKey ;
                    currentItemForMasterIndex[1] = currentKey ;
                    currentItemForMasterIndex[2] = CommonLib.convertToPrimitiveValue(indexFileName, "STRING") ;
                    indexBW.writeUnshared(currentItemForMasterIndex);
                    currentMapForMasterIndex.add(currentItemForMasterIndex);
                }else{
                    bw.writeUnshared(null);
                    bufferedOutputStream.close();
                    fileOutputStream.close();
                    bw.close() ;
                    bw = null ;

                    indexFileName = tableName + "_" + columnName + "_" + CommonLib.getsortFileSeqNumber();
                    file = new File(CommonLib.INDEX_DIRECTORY + indexFileName);
                    fileOutputStream = new FileOutputStream(file);
                    bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
                    bw = new ObjectOutputStream(bufferedOutputStream);

                    writeValue[0] = currentKey;
                    writeValue[1] = CommonLib.convertToPrimitiveValue(currentValue, "INT");
                    bw.writeUnshared(writeValue);
                    firstKey = currentKey ;
                    lineNumber = 0;
                }
                lineNumber +=1 ;
            }
        }

        if(!lineNumber.equals(indexBlockSize+1)){
            PrimitiveValue[] currentItemForMasterIndex = new PrimitiveValue[3];
            currentItemForMasterIndex[0] = firstKey ;
            currentItemForMasterIndex[1] = CommonLib.convertToPrimitiveValue(currentIndex.lastKey(), colDataType) ;
            currentItemForMasterIndex[2] = CommonLib.convertToPrimitiveValue(indexFileName, "STRING") ;
            indexBW.writeUnshared(currentItemForMasterIndex);

            currentMapForMasterIndex.add(currentItemForMasterIndex);
        }


        indexBW.writeUnshared(null);
        indexBW.close();
        indexBufferedOutputStream.close();
        indexFileOutputStream.close();

        indexBW = null ;
        bw.writeUnshared(null);
        bw.close();
        bufferedOutputStream.close();
        fileOutputStream.close();
        bw.close();
        bw = null ;
        currentIndex.clear();
        currentIndex = null ;
        Main.masterIndex.put(tableName + "|" + columnName, currentMapForMasterIndex);
    }

    private static void indexesToSecondaryIndexes(String tableName, ArrayList<net.sf.jsqlparser.statement.create.table.Index> listOfIndexes){
        ArrayList<net.sf.jsqlparser.statement.create.table.Index> secondaryIndexes =  new ArrayList<net.sf.jsqlparser.statement.create.table.Index>() ;
        for(net.sf.jsqlparser.statement.create.table.Index index : listOfIndexes){
            if(index.getType().equals("INDEX")){
                secondaryIndexes.add(index) ;
            }
            else{
                primaryIndexMap.put(tableName, index) ;
            }
        }
        secondaryIndexMap.put(tableName, secondaryIndexes) ;
    }

    private static void indexColumnIndices(CreateTable createTable, ArrayList<net.sf.jsqlparser.statement.create.table.Index> indexList){
        ArrayList<Integer> indexColumnIndices = new ArrayList<Integer>() ;
        for (net.sf.jsqlparser.statement.create.table.Index index : indexList){
            int i = 0;
            for (ColumnDefinition columnDefinition : createTable.getColumnDefinitions()) {
                if(index.getColumnsNames().get(0).equals(columnDefinition.getColumnName())){
                    indexColumnIndices.add(i) ;
                    break ;
                }
                i++;
            }
        }
        secondaryIndexColumnIndices.put(createTable.getTable().getName(), indexColumnIndices) ;
    }

    private static void indexColumnIndices(CreateTable createTable, net.sf.jsqlparser.statement.create.table.Index indexList){
        ArrayList<Integer> indexColumnIndices = new ArrayList<Integer>() ;
        for (String column : indexList.getColumnsNames()){
            int i = 0;
            for (ColumnDefinition columnDefinition : createTable.getColumnDefinitions()) {
                if(column.equals(columnDefinition.getColumnName())){
                    indexColumnIndices.add(i) ;
                    break ;
                }
                i++;
            }
        }
        primaryIndexColumnIndices.put(createTable.getTable().getName(), indexColumnIndices) ;
    }

    private static boolean validateDataType(String colDataType){
        return colDataType.equals("INT") || colDataType.equals("DECIMAL");
    }

    private static void writeToGlobalIndex(String tableName, CreateTable createTable) throws IOException {
        String diskWrite = tableName + "|" ;
        ArrayList<String> ColumnList = new ArrayList<String>();
        String primaryKey = "" ;
        for(net.sf.jsqlparser.statement.create.table.Index index: createTable.getIndexes()){
            if(index.getType().equals("PRIMARY KEY")){
                primaryKey = index.getColumnsNames().get(0);
                Main.globalPrimaryIndex.put(tableName, primaryKey);
            }
            ColumnList.addAll(index.getColumnsNames());
            for(String columnName : index.getColumnsNames()){
                diskWrite = diskWrite + columnName + "|" ;
            }
        }
        diskWrite += primaryKey ;
        Main.globalIndex.put(tableName, ColumnList) ;
        BufferedWriter globalIndexWriter = new BufferedWriter(new FileWriter(CommonLib.INDEX_DIRECTORY + "GlobalIndex", true));
        globalIndexWriter.write(diskWrite + "\n");
        globalIndexWriter.write(Main.currentQuery + "\n");
        globalIndexWriter.close();
        globalIndexWriter = null ;
    }
}