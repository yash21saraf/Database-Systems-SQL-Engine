package iterators;

import builders.IteratorBuilder;
import helpers.CommonLib;
import helpers.Index;
import helpers.Schema;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.io.*;
import java.util.*;

import static helpers.Index.isNumber;

public class IndexIterator implements RAIterator {

    //region Variables

    boolean hasNext = false;
    int currentFileIndex = 0;
    List<String> tableIndexList = new ArrayList<String>();
    private CommonLib commonLib = CommonLib.getInstance();
    private ColumnDefinition[] columnDefinitions;
    private String tableName;
    private String tableAlias;
    private BufferedReader br;
    private PrimitiveValue[] currentLine;
    private PrimitiveValue[] nextLine;
    private Schema[] schema;
    private Expression expression;
    List<Expression> expressionList;
    private List<String> finalFileNameList = new ArrayList<String>();

    public IndexIterator(String tableName, String tableAlias, ColumnDefinition[] columnDefinitions, Expression expression) throws Exception {
        this.columnDefinitions = columnDefinitions;
        this.tableName = tableName;
        this.tableAlias = tableAlias;

        this.expression = expression;

        if (this.tableAlias == null)
            this.schema = createSchema(columnDefinitions, tableName);
        else {
            this.schema = createSchema(columnDefinitions, this.tableAlias);
            addOriginalSchema(columnDefinitions, tableName);
        }

        expressionList = commonLib.getExpressionList(expression);
        createFileNameList();

        finalFileList();

    }

    private void finalFileList() throws Exception {

        String colName = null;
        Set<String> set = new HashSet<String>();

        for(String path : finalFileNameList){
            String filename = path.substring(path.lastIndexOf("/") + 1, path.lastIndexOf("_"));

            colName = filename.substring(filename.indexOf("_") + 1);
            //System.out.println(filename);
            if(!isPrimaryKey(tableName, colName)){
                BufferedReader br = new BufferedReader(new FileReader(path));
                String line = null;
                while((line = br.readLine())!= null){
                    String tuple[] = line.split("\\|");
                    set.add(tuple[1]);
                }
            }
        }

        List<String> temp = new ArrayList<String>(set);

//        Collections.sort(temp, new Comparator<String>() {
//            @Override
//            public int compare(String o1, String o2) {
//
//                String a[] = o1.split("\\|");
//                String b[] = o2.split("\\|");
//
//                if (isNumber(a[0])) {
//
//                    double pv1 = Double.parseDouble(a[0]);
//                    double pv2 = Double.parseDouble(b[0]);
//
//                    if (pv1 < pv2)
//                        return -1;
//                    else if (pv1 > pv2)
//                        return 1;
//
//                } else {
//
//                    return a[0].compareTo(b[0]);
//                }
//
//                return 0;
//            }
//        });


        finalFileNameList = getPrimaryKeyFiles(temp, colName);

    }

    private List<String> getPrimaryKeyFiles(List<String> list, String colName) {

        Set<String> primaryKeyFiles = new HashSet<String>();

        Index index = new Index();

        primaryKeyFiles.addAll(index.getIndexList(tableName, index.getPK(tableName)));

        Set<String> set = new HashSet<String>();

        for(String key : list) {
            for (String str : primaryKeyFiles) {

                if (isKeyInRange(str, key)){
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

        if(isNumber(key)){

            int first = Integer.parseInt(start);
            int last = Integer.parseInt(end);

            int k = Integer.parseInt(key);

            if(k > first && k < last)
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

    int cnt = 0;
    @Override
    public boolean hasNext() throws Exception {

//        cnt++;
//
//        if(cnt >=99999)
//            System.out.println(cnt);

        try {
            if (hasNext)
                return true;

            if (br == null)
                br = getBufferedReader();

            if (br == null)
                return false;
            else if ((nextLine = commonLib.covertTupleToPrimitiveValue(br.readLine(), columnDefinitions)) != null) {
                hasNext = true;
                return true;
            } else if (nextLine == null){
                br = getBufferedReader();
                return this.hasNext();
            }

            hasNext = false;

            return false;

        } catch (Exception e) {
            throw e;
        }
    }

    private BufferedReader getBufferedReader() throws Exception {

        if (currentFileIndex < finalFileNameList.size()) {
            String file = finalFileNameList.get(currentFileIndex);

            br = new BufferedReader(new FileReader(new File(file)));

            currentFileIndex++;
            return br;
        }

        return null;
    }

    @Override
    public PrimitiveValue[] next() {
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
        currentFileIndex = 0;
    }

    @Override
    public RAIterator getChild() {
        return null;
    }

    @Override
    public void setChild(RAIterator child) {
    }

    @Override
    public Schema[] getSchema() {
        return this.schema;
    }

    //endregion

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

            temp.addAll(intersect(leftFiles, rightFiles));

            if (temp.size() < min) {
                min = temp.size();
                finalFileNameList.clear();
                finalFileNameList.addAll(temp);
                temp.clear();
            }
        }
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

    private List<String> intersect(List<String> leftFiles, List<String> rightFiles) {

        if(leftFiles == null || leftFiles.size() == 0)
            return null;

        if(rightFiles == null || rightFiles.size() == 0)
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

        List<Expression> expList = getIndexExpression();

        return expList;
    }

    private List<Expression> getIndexExpression() {

        String indexColumnName = "";

        List<Expression> returnList = new ArrayList<Expression>();

        for (Expression expression : expressionList) {

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

            if(((GreaterThan) expression).getRightExpression() instanceof Function) {
                indexColumnValue = ((Function) ((GreaterThan) expression).getRightExpression()).getParameters().getExpressions().get(0).toString();

                char [] ch = indexColumnValue.toCharArray();
                indexColumnValue = "";
                for(int i = 1; i < ch.length-1; i++) // TODO: did workaround for DATE
                    indexColumnValue += ch[i];
            }
            else
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

            if(((GreaterThanEquals) expression).getRightExpression() instanceof Function) {
                indexColumnValue = ((Function) ((GreaterThanEquals) expression).getRightExpression()).getParameters().getExpressions().get(0).toString();

                char [] ch = indexColumnValue.toCharArray();
                indexColumnValue = "";
                for(int i = 1; i < ch.length-1; i++) // TODO: did workaround for DATE
                    indexColumnValue += ch[i];
            }
            else
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

            if(((MinorThan) expression).getRightExpression() instanceof Function) {
                indexColumnValue = ((Function) ((MinorThan) expression).getRightExpression()).getParameters().getExpressions().get(0).toString();

                char [] ch = indexColumnValue.toCharArray();
                indexColumnValue = "";
                for(int i = 1; i < ch.length-1; i++) // TODO: did workaround for DATE
                    indexColumnValue += ch[i];
            }
            else
                indexColumnValue = ((MinorThan) expression).getRightExpression().toString();

            List<String> indexFileList = index.getIndexList(tableName, indexColumnName);

            for (String ind : indexFileList) {
                String indexRow[] = ind.split("\\|");

                if (indexRow[1].compareTo(indexColumnValue) <= 0 )
                    fileNameList.add(indexRow[2]);

                else if (indexRow[0].compareTo(indexColumnValue) <= 0 ) {
                    fileNameList.add(indexRow[2]);
                    break;
                }


            }

        } else if (expression instanceof MinorThanEquals) {

            indexColumnName = ((MinorThanEquals) expression).getLeftExpression().toString();

            if(((MinorThanEquals) expression).getRightExpression() instanceof Function) {
                indexColumnValue = ((Function) ((MinorThanEquals) expression).getRightExpression()).getParameters().getExpressions().get(0).toString();

                char [] ch = indexColumnValue.toCharArray();
                indexColumnValue = "";
                for(int i = 1; i < ch.length-1; i++) // TODO: did workaround for DATE
                    indexColumnValue += ch[i];
            }
            else
                indexColumnValue = ((MinorThanEquals) expression).getRightExpression().toString();

            List<String> indexFileList = index.getIndexList(tableName, indexColumnName);

            for (String ind : indexFileList) {
                String indexRow[] = ind.split("\\|");

                if (indexRow[1].compareTo(indexColumnValue) <= 0 )
                    fileNameList.add(indexRow[2]);
                else if (indexRow[0].compareTo(indexColumnValue) <= 0 ) {
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
