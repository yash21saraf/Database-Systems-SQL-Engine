package iterators;

import builders.IteratorBuilder;
import helpers.CommonLib;
import helpers.Index;
import helpers.Schema;
import helpers.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

import static helpers.Index.isNumber;

public class IndexIterator implements RAIterator {

    //region Variables

    boolean hasNext = false;
    int currentFileIndex = 0;
    List<String> tableIndexList = new ArrayList<String>();
    List<Expression> expressionList;
    int cnt = 0;
    long start = System.currentTimeMillis();
    long current = 0; //positionList.get(currentPositionToRead++);
    long next = 0;
    String val = null;
    int cntr = 1;
    private CommonLib commonLib = CommonLib.getInstance();
    private ColumnDefinition[] columnDefinitions;
    private String tableName;
    private String tableAlias;
    private LineNumberReader br;
    private PrimitiveValue[] currentLine;
    private PrimitiveValue[] nextLine;
    private Schema[] schema;
    private Expression expression;
    private List<String> finalFileNameList = new ArrayList<String>();
    private List<Long> positionList = new ArrayList<Long>();
    private int currentPositionToRead = 0;
    private RandomAccessFile randomAccessFile = null;

    private Tuple tuple;

    public IndexIterator(String tableName, String tableAlias, ColumnDefinition[] columnDefinitions, Expression expression) throws Exception {
        this.columnDefinitions = columnDefinitions;
        this.tableName = tableName;
        this.tableAlias = tableAlias;

        this.expression = expression;

        tuple = new Tuple(columnDefinitions, tableName);

        if (this.tableAlias == null)
            this.schema = createSchema(columnDefinitions, tableName);
        else {
            this.schema = createSchema(columnDefinitions, this.tableAlias);
            addOriginalSchema(columnDefinitions, tableName);
        }

        expressionList = commonLib.getExpressionList(expression);

        expressionList = getUsefulExp(expressionList);
        createFileNameList();

        finalFileList();

//        randomAccessFile = new RandomAccessFile(CommonLib.TABLE_DIRECTORY + tableName + CommonLib.extension, "r");
        br = new LineNumberReader(new FileReader(CommonLib.TABLE_DIRECTORY + tableName + CommonLib.extension));


        if(positionList.size() > 5000000){
            positionList.clear();
        }

        if(positionList.size() != 0) {
            Collections.sort(positionList);
            next = positionList.get(currentPositionToRead++);
        }


//                , new Comparator<Long>() {
//            @Override
//            public int compare(Long o1, Long o2) {
//                return o1.compareTo(o2);
//            }
//        });

    }

    private List<Expression> getUsefulExp(List<Expression> expressionList) {
        List<Expression> list = new ArrayList<Expression>();


        for(Expression expression : expressionList){

            List<Column> right = commonLib.getColumnList(expression);

            if(right.size() == 1 && (right.get(0).getColumnName().equals("SHIPDATE") || right.get(0).getColumnName().equals("RECEIPTDATE") || right.get(0).getColumnName().equals("ORDERDATE") )){
                list.add(expression);
            }
        }

        return  list;
    }

    private static boolean isPrimaryKey(String table, String columnName) {

        if (table.equals("LINEITEM") && columnName.equals("ORDERKEY"))
            return true;
        if (table.equals("PART") && columnName.equals("PARTKEY"))
            return true;
        if (table.equals("CUSTOMER") && columnName.equals("CUSTKEY"))
            return true;
        if (table.equals("SUPPLIER") && columnName.equals("SUPPKEY"))
            return true;
        if (table.equals("NATION") && columnName.equals("NATIONKEY"))
            return true;
        if (table.equals("REGION") && columnName.equals("REGIONKEY"))
            return true;
        if (table.equals("ORDERS") && columnName.equals("ORDERKEY"))
            return true;
        if (table.equals("PARTSUPP") && columnName.equals("PARTKEY"))
            return true;

        return false;

    }

    private void finalFileList() throws Exception {

        String colName = null;
        Set<String> set = new HashSet<String>();

        for (String path : finalFileNameList) {
            String filename = path.substring(path.lastIndexOf("/") + 1, path.lastIndexOf("_"));

            colName = filename.substring(filename.indexOf("_") + 1);
            //System.out.println(filename);
            if (!isPrimaryKey(tableName, colName)) {
                BufferedReader br = new BufferedReader(new FileReader(path));
                String line = null;
                while ((line = br.readLine()) != null) {
                    String tuple[] = line.split("\\|");
                    set.add(tuple[1]);
                }
            }
        }

        List<String> temp = new ArrayList<String>(set);


        //finalFileNameList = getPrimaryKeyFiles(temp, colName);
        for (String str : temp) {
            long val = Long.valueOf(str);
            positionList.add(val);  // TODO : position list can be stored and read back in second phase
        }


    }

    private List<String> getPrimaryKeyFiles(List<String> list, String colName) {

        Set<String> primaryKeyFiles = new HashSet<String>();

        Index index = new Index();

        primaryKeyFiles.addAll(index.getIndexList(tableName, index.getPK(tableName)));

        Set<String> set = new HashSet<String>();

        for (String key : list) {
            for (String str : primaryKeyFiles) {

                if (isKeyInRange(str, key)) {
                    set.add(CommonLib.TABLE_DIRECTORY + str.substring(str.indexOf("_") + 1));
                    break;
                }
            }
        }

        List<String> ret = new ArrayList(set);
        return ret;
    }

    private boolean isKeyInRange(String file, String key) {


        String start = file.substring(0, file.indexOf("|"));
        String end = file.substring(file.indexOf("|") + 1, file.lastIndexOf("|"));

        if (isNumber(key)) {

            int first = Integer.parseInt(start);
            int last = Integer.parseInt(end);

            int k = Integer.parseInt(key);

            if (k > first && k < last)
                return true;
            else
                return false;

        } else {
            if (key.compareTo(start) >= 0 && key.compareTo(end) <= 0)
                return true;
            else
                return false;
        }
    }

    private String getFiles(String secondaryIndex, String colName) {
        Index index = new Index();

        List<String> indexFileList = index.getIndexList(tableName, index.getPK(tableName));

        //System.out.println(indexFileList);

        return "";
    }

    @Override
    public boolean hasNext() throws Exception {

//        cnt++;
//
//        if(cnt >=99999)
//            System.out.println(cnt);

        try {
            if (hasNext)
                return true;

            if(positionList.size() == 0){
                if ((nextLine = tuple.covertTupleToPrimitiveValue(br.readLine())) != null) {
                    hasNext = true;
                    return true;
                }
            } else {
                if ((nextLine = getDataFromFile()) != null) {
                    hasNext = true;
                    return true;
                }
            }


            hasNext = false;
            return false;

        } catch (Exception e) {
            throw e;
        }
    }

    private PrimitiveValue[] getDataFromFile() {
        try {

            if (currentPositionToRead >= positionList.size()) {
                hasNext = false;
                return null;
            }
            //
//
//            long pos = positionList.get(currentPositionToRead++);
//            randomAccessFile.seek(pos);
//
//            String randomAccessFileLine = randomAccessFile.readUTF();
//

//            long end;
//            if(currentPositionToRead % 10000 == 0) {
//                end = System.currentTimeMillis();
//                System.out.println(currentPositionToRead);
//                long res = end - start;
//                System.out.println("time " +res);
//                start = end;
//            }

            //br = null;

            //br = new LineNumberReader(new FileReader(CommonLib.TABLE_DIRECTORY + tableName + CommonLib.extension));


            br.skip(next - current);
            val = br.readLine();
            //System.out.println("VAL " + val );
            //System.out.println("LEN " + val.length());
            if ((nextLine = tuple.covertTupleToPrimitiveValue(val)) != null) {
                if(tableName.equals("LINEITEM")){
                    nextLine[15] = new StringValue("a");
                    nextLine[13] = new StringValue("a");
                }
                // System.out.println(Arrays.toString(nextLine));
                hasNext = true;
                current = next + val.length() + 1;
                if (currentPositionToRead == positionList.size())
                    return nextLine;
                next = positionList.get(currentPositionToRead++);
                return nextLine;
            }

            hasNext = false;
            return null;

//            next = current - next;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private BufferedReader getBufferedReader() throws Exception {

        if (currentFileIndex < finalFileNameList.size()) {
            String file = finalFileNameList.get(currentFileIndex);

            br = new LineNumberReader(new FileReader(new File(file)));

            currentFileIndex++;
            return br;
        }

        return null;
    }

    @Override
    public PrimitiveValue[] next() {
        //System.out.println(cntr++);

        currentLine = nextLine;
        nextLine = null;
        hasNext = false;
        return currentLine;
    }

    @Override
    public void reset() throws Exception {
        nextLine = null;
        currentLine = null;
        hasNext = false;
        currentPositionToRead = 0;
        currentFileIndex = 0; // TODO : NOT using anymore
    }

    @Override
    public RAIterator getChild() {
        return null;
    }

    @Override
    public void setChild(RAIterator child) {
    }

    //endregion

    @Override
    public Schema[] getSchema() {
        return this.schema;
    }

    @Override
    public void setSchema(Schema[] schema) {
        this.schema = schema;
    }

    @Override
    public RAIterator optimize(RAIterator iterator) {
        return iterator;
    }

    private void createFileNameList() {

        List<String> temp = new ArrayList<String>();
        List<String> leftFiles = new ArrayList<String>();
        List<String> rightFiles = new ArrayList<String>();

        int min = Integer.MAX_VALUE;

        while (expressionList.size() > 0) {

            List<Expression> expressionsPair = getPair();

            if (expressionsPair == null || expressionsPair.size() == 0) { // tOdO check the logic
                return;
            }

            if (expressionsPair.size() == 1) {
                leftFiles = getFiles(expressionsPair.get(0));
            } else {
                leftFiles = getFiles(expressionsPair.get(0));
                rightFiles = getFiles(expressionsPair.get(1));
            }

            List<String> t= intersect(leftFiles, rightFiles);
            if(t == null || t.size() == 0)
                continue;

            temp.addAll(t);

            if (temp.size() < min) {
                min = temp.size();
                finalFileNameList.clear();
                finalFileNameList.addAll(temp);
                temp.clear();
            }
        }
    }

    private List<String> intersect(List<String> leftFiles, List<String> rightFiles) {

        if (leftFiles == null || leftFiles.size() == 0)
            return null;

        if (rightFiles == null || rightFiles.size() == 0)
            return leftFiles;

        leftFiles.retainAll(rightFiles);

        return leftFiles;
    }

    private List<Expression> getPair() {

        Index ind = new Index();

        String indexList = Index.indexMap.get(tableName);

        String indexes[] = indexList.split("\\|");

        for (int i = 0; i < indexes.length; i++) {
            tableIndexList.add(tableName + "." + indexes[i]);
        }

        //tableIndexList.add(indexList);

        List<Expression> expList = getIndexExpression();

        return expList;
    }


    private List<Expression> getIndexExpression() {

        String indexColumnName = "";

        List<Expression> returnList = new ArrayList<Expression>();

//        List<Expression> expressionList = commonLib.getExpressionList(expression);

        for (Expression expression : commonLib.getExpressionList(expression)) {

            if (expression instanceof GreaterThan) {

                indexColumnName = ((GreaterThan) expression).getLeftExpression().toString();
                if (tableIndexList.contains(indexColumnName)) {
                    expressionList.remove(expression);
                    returnList.add(expression);

                    for (Expression expressionInner : expressionList) {
                        if (tableIndexList.contains(indexColumnName)) {
                            expressionList.remove(expressionInner);
                            returnList.add(expressionInner);
                            return returnList;
                        }
                        return returnList;
                    }

                }

            } else if (expression instanceof GreaterThanEquals) {

                indexColumnName = ((GreaterThanEquals) expression).getLeftExpression().toString();
                if (tableIndexList.contains(indexColumnName)) {
                    expressionList.remove(expression);
                    returnList.add(expression);

                    for (Expression expressionInner : expressionList) {
                        if (tableIndexList.contains(indexColumnName)) {
                            expressionList.remove(expressionInner);
                            returnList.add(expressionInner);
                            return returnList;
                        }
                        return returnList;
                    }

                }

            } else if (expression instanceof MinorThan) {

                indexColumnName = ((MinorThan) expression).getLeftExpression().toString();
                if (tableIndexList.contains(indexColumnName)) {
                    expressionList.remove(expression);
                    returnList.add(expression);

                    for (Expression expressionInner : expressionList) {
                        if (tableIndexList.contains(indexColumnName)) {
                            expressionList.remove(expressionInner);
                            returnList.add(expressionInner);
                            return returnList;
                        }
                        return returnList;
                    }

                }

            } else if (expression instanceof MinorThanEquals) {

                indexColumnName = ((MinorThanEquals) expression).getLeftExpression().toString();
                if (tableIndexList.contains(indexColumnName)) {
                    expressionList.remove(expression);
                    returnList.add(expression);

                    for (Expression expressionInner : expressionList) {
                        if (tableIndexList.contains(indexColumnName)) {
                            expressionList.remove(expressionInner);
                            returnList.add(expressionInner);
                            return returnList;
                        }
                        return returnList;
                    }

                }

            } else if (expression instanceof EqualsTo) {

                indexColumnName = ((EqualsTo) expression).getLeftExpression().toString();
                if (tableIndexList.contains(indexColumnName)) {
                    expressionList.remove(expression);
                    returnList.add(expression);

                    for (Expression expressionInner : expressionList) {
                        if (tableIndexList.contains(indexColumnName)) {
                            expressionList.remove(expressionInner);
                            returnList.add(expressionInner);
                            return returnList;
                        }
                        return returnList;
                    }

                }

            } else if (expression instanceof NotEqualsTo) {

                indexColumnName = ((NotEqualsTo) expression).getLeftExpression().toString();
                if (tableIndexList.contains(indexColumnName)) {
                    expressionList.remove(expression);
                    returnList.add(expression);

                    for (Expression expressionInner : expressionList) {
                        if (tableIndexList.contains(indexColumnName)) {
                            expressionList.remove(expressionInner);
                            returnList.add(expressionInner);
                            return returnList;
                        }
                        return returnList;
                    }

                }
            }
        }

        return returnList;
    }


    private List<String> getFiles(Expression expression) {
        List<String> fileNameList = new ArrayList<String>();
        Index index = new Index();

        String indexColumnName = "";
        String indexColumnValue = "";

        if (expression instanceof GreaterThan) {

            indexColumnName = ((GreaterThan) expression).getLeftExpression().toString();

            if (((GreaterThan) expression).getRightExpression() instanceof Function) {
                indexColumnValue = ((Function) ((GreaterThan) expression).getRightExpression()).getParameters().getExpressions().get(0).toString();

                char[] ch = indexColumnValue.toCharArray();
                indexColumnValue = "";
                for (int i = 1; i < ch.length - 1; i++) // TODO: did workaround for DATE
                    indexColumnValue += ch[i];
            } else
                indexColumnValue = ((GreaterThan) expression).getRightExpression().toString();

            List<String> indexFileList = index.getIndexList(tableName, indexColumnName);

            for (String ind : indexFileList) {
                String indexRow[] = ind.split("\\|");

                if (indexRow[0].compareTo(indexColumnValue) <= 0 && indexRow[1].compareTo(indexColumnValue) <= 0)
                    continue;
                else
                    fileNameList.add(indexRow[2]);
            }

        } else if (expression instanceof GreaterThanEquals) {

            indexColumnName = ((GreaterThanEquals) expression).getLeftExpression().toString();

            if (((GreaterThanEquals) expression).getRightExpression() instanceof Function) {
                indexColumnValue = ((Function) ((GreaterThanEquals) expression).getRightExpression()).getParameters().getExpressions().get(0).toString();

                char[] ch = indexColumnValue.toCharArray();
                indexColumnValue = "";
                for (int i = 1; i < ch.length - 1; i++) // TODO: did workaround for DATE
                    indexColumnValue += ch[i];
            } else
                indexColumnValue = ((GreaterThanEquals) expression).getRightExpression().toString();

            List<String> indexFileList = index.getIndexList(tableName, indexColumnName);

            for (String ind : indexFileList) {
                String indexRow[] = ind.split("\\|");

                if (indexRow[0].compareTo(indexColumnValue) <= 0 && indexRow[1].compareTo(indexColumnValue) <= 0)
                    continue;
                else
                    fileNameList.add(indexRow[2]);
            }

        } else if (expression instanceof MinorThan) {

            indexColumnName = ((MinorThan) expression).getLeftExpression().toString();

            if (((MinorThan) expression).getRightExpression() instanceof Function) {
                indexColumnValue = ((Function) ((MinorThan) expression).getRightExpression()).getParameters().getExpressions().get(0).toString();

                char[] ch = indexColumnValue.toCharArray();
                indexColumnValue = "";
                for (int i = 1; i < ch.length - 1; i++) // TODO: did workaround for DATE
                    indexColumnValue += ch[i];
            } else
                indexColumnValue = ((MinorThan) expression).getRightExpression().toString();

            List<String> indexFileList = index.getIndexList(tableName, indexColumnName);

            for (String ind : indexFileList) {
                String indexRow[] = ind.split("\\|");

                if (indexRow[1].compareTo(indexColumnValue) <= 0)
                    fileNameList.add(indexRow[2]);

                else if (indexRow[0].compareTo(indexColumnValue) <= 0) {
                    fileNameList.add(indexRow[2]);
                    break;
                }


            }

        } else if (expression instanceof MinorThanEquals) {

            indexColumnName = ((MinorThanEquals) expression).getLeftExpression().toString();

            if (((MinorThanEquals) expression).getRightExpression() instanceof Function) {
                indexColumnValue = ((Function) ((MinorThanEquals) expression).getRightExpression()).getParameters().getExpressions().get(0).toString();

                char[] ch = indexColumnValue.toCharArray();
                indexColumnValue = "";
                for (int i = 1; i < ch.length - 1; i++) // TODO: did workaround for DATE
                    indexColumnValue += ch[i];
            } else
                indexColumnValue = ((MinorThanEquals) expression).getRightExpression().toString();

            List<String> indexFileList = index.getIndexList(tableName, indexColumnName);

            for (String ind : indexFileList) {
                String indexRow[] = ind.split("\\|");

                if (indexRow[1].compareTo(indexColumnValue) <= 0)
                    fileNameList.add(indexRow[2]);
                else if (indexRow[0].compareTo(indexColumnValue) <= 0) {
                    fileNameList.add(indexRow[2]);
                    break;
                }
            }

        } else if (expression instanceof EqualsTo) {

            indexColumnName = ((EqualsTo) expression).getLeftExpression().toString();
            indexColumnValue = ((EqualsTo) expression).getRightExpression().toString();

            List<String> indexFileList = index.getIndexList(tableName, indexColumnName);

            for (String ind : indexFileList) {
                String indexRow[] = ind.split("\\|");

                if (indexColumnValue.compareTo(indexRow[0]) >= 0 && indexColumnValue.compareTo(indexRow[0]) <= 0) // TODO: CHECK
                    fileNameList.add(indexRow[2]);
            }

        } else if (expression instanceof NotEqualsTo) {

            indexColumnName = ((NotEqualsTo) expression).getLeftExpression().toString();
            indexColumnValue = ((NotEqualsTo) expression).getRightExpression().toString();

            fileNameList = index.getIndexList(tableName, indexColumnName);

        }

        return fileNameList; //TODO
    }

    private void addOriginalSchema(ColumnDefinition[] columnDefinitions, String tableName) throws Exception {

        Schema[] convertedTuple = new Schema[columnDefinitions.length];

        for (int index = 0; index < columnDefinitions.length; index++) {
            Schema convertedValue = new Schema();
            convertedValue.setColumnDefinition(columnDefinitions[index]);
            convertedValue.setTableName(tableName);
            convertedTuple[index] = convertedValue;

        }
        IteratorBuilder.iteratorSchemas.put(tableName, convertedTuple);
    }

    private Schema[] createSchema(ColumnDefinition[] columnDefinitions, String tableName) throws Exception {

        if (columnDefinitions == null)
            return null;

        Schema[] convertedTuple = new Schema[columnDefinitions.length];

        for (int index = 0; index < columnDefinitions.length; index++) {
            Schema convertedValue = new Schema();
            convertedValue.setColumnDefinition(columnDefinitions[index]);
            convertedValue.setTableName(tableName);
            convertedTuple[index] = convertedValue;

        }
        IteratorBuilder.iteratorSchemas.put(tableName, convertedTuple);
        return convertedTuple;
    }


}
