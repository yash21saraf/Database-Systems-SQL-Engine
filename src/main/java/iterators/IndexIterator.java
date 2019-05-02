package iterators;

import builders.IteratorBuilder;
import dubstep.Main;
import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import helpers.Schema;
import helpers.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import javax.swing.text.Position;
import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IndexIterator implements RAIterator {

    boolean hasNext = false;
    private CommonLib commonLib = CommonLib.getInstance();
    private ColumnDefinition[] columnDefinitions;
    private String tableName;
    private String tableAlias;
    private Schema[] schema;
    private PrimitiveValue[] currentLine;
    private PrimitiveValue[] nextLine;
    private ArrayList<Expression> expressions;
    private Expression completeExpression ;
    private ArrayList<String> columnNames ;
    private ArrayList<ColumnDefinition> columnIndices = new ArrayList<ColumnDefinition>() ;
    private ArrayList<String> conditionTypes ;
    private ArrayList<Long> positions= new ArrayList<Long>() ;

    //////////////////////////////////////////////////////////
    private int currentPositionToRead = 0;
    long currentPosition = 0;
    long nextPosition = 0;
    private LineNumberReader br;
    String val = null;

    //////////////////////////////////////////////////////////
    private Tuple tupleClass;
    private ArrayList<Integer> newColDefMapping ;

    public IndexIterator(String tableName, String tableAlias, ColumnDefinition[] columnDefinitions, ArrayList<Expression> expression, Schema[] schema, ArrayList<String> columnNames, ArrayList<String> conditionTypes, ArrayList<Integer> newColDefMapping) throws Exception {
        this.columnDefinitions = columnDefinitions;
        this.tableName = tableName;
        this.tableAlias = tableAlias;
        this.schema = schema;
        this.expressions = expression;
        this.columnNames = columnNames ;
        this.conditionTypes = conditionTypes;
        this.newColDefMapping = newColDefMapping ;

        tupleClass = new Tuple(columnDefinitions, tableName, newColDefMapping);

        completeExpression = this.expressions.get(0);
        for(int i = 1; i < this.expressions.size(); i++){
            completeExpression = new AndExpression(completeExpression, this.expressions.get(i)) ;
        }

        preProcessIndex();
        br = new LineNumberReader(new FileReader(CommonLib.TABLE_DIRECTORY + tableName + CommonLib.extension));



        if(positions.size() != 0) {
            nextPosition = positions.get(currentPositionToRead++);
        }

    }

    private void preProcessIndex() throws IOException, ClassNotFoundException, SQLException {

        for(String colName: columnNames){
            for(ColumnDefinition colDefinition : columnDefinitions){
                if(colDefinition.getColumnName().equals(colName)){
                    columnIndices.add(colDefinition);
                    break ;
                }
            }
        }
        ArrayList<ArrayList<PrimitiveValue>> listOfFinalFileList = new ArrayList<ArrayList<PrimitiveValue>>() ;


        for(int i = 0; i <expressions.size(); i++){
            if(!Main.masterIndex.containsKey(tableName + "|" + columnNames.get(i))){
                String indexListName = tableName + "_" + columnNames.get(i) ;
                File indexFile = new File(CommonLib.INDEX_DIRECTORY + indexListName);
                FileInputStream indexFileInputStream = new FileInputStream(indexFile);
                BufferedInputStream indexBufferedInputStream = new BufferedInputStream(indexFileInputStream);
                ObjectInputStream indexBW = new ObjectInputStream(indexBufferedInputStream);

                ArrayList<PrimitiveValue[]> currentMapForMasterIndex = new ArrayList<PrimitiveValue[]>();

                PrimitiveValue[] currentRow =(PrimitiveValue[]) indexBW.readUnshared() ;
                while(currentRow != null){
                    currentMapForMasterIndex.add(currentRow);
                    currentRow = (PrimitiveValue[]) indexBW.readUnshared() ;
                }
                Main.masterIndex.put(tableName + "|" + columnNames.get(i), currentMapForMasterIndex);
                indexFileInputStream.close();
                indexBufferedInputStream.close();
                indexBW.close();
                indexFileInputStream = null;
                indexBufferedInputStream = null;
                indexBW = null;

            }

            ArrayList<PrimitiveValue> listOfFileNames = new ArrayList<PrimitiveValue>();
            PrimitiveValueWrapper[] start_end = new PrimitiveValueWrapper[1];
            PrimitiveValueWrapper convertedRow = new PrimitiveValueWrapper();
            PrimitiveValueWrapper result_start ;
            PrimitiveValueWrapper result_end ;

            if(conditionTypes.get(i).equals("OTHERS")){
                for(PrimitiveValue[] currentRow: Main.masterIndex.get(tableName + "|" + columnNames.get(i))){

                    convertedRow.setPrimitiveValue(currentRow[0]);
                    convertedRow.setColumnDefinition(columnIndices.get(i));
                    convertedRow.setTableName(this.tableName);
                    start_end[0] = convertedRow ;
                    result_start = (PrimitiveValueWrapper) commonLib.eval(expressions.get(i), start_end);
                    if(result_start.getPrimitiveValue().toBool()){
                        listOfFileNames.add(currentRow[2]) ;
                        continue ;
                    }

                    convertedRow.setPrimitiveValue(currentRow[1]);
                    convertedRow.setColumnDefinition(columnIndices.get(i));
                    convertedRow.setTableName(this.tableName);
                    start_end[0] = convertedRow ;
                    result_end = (PrimitiveValueWrapper) commonLib.eval(expressions.get(i), start_end);
                    if(result_end.getPrimitiveValue().toBool()){
                        listOfFileNames.add(currentRow[2]) ;
                    }
                }

            }else if(conditionTypes.get(i).equals("EQUALS")){
                ArrayList<Expression> expBreakdown = (ArrayList<Expression>) commonLib.getExpressionList(expressions.get(i));
                for(Expression exp : expBreakdown){
                    if(exp instanceof EqualsTo){
                        if(columnIndices.get(i).getColDataType().getDataType().equals("STRING") || columnIndices.get(i).getColDataType().getDataType().equals("CHAR") ||columnIndices.get(i).getColDataType().getDataType().equals("VARCHAR")){
                            List<Column> leftCol = commonLib.getColumnList(((EqualsTo) exp).getLeftExpression());
                            List<Column> rightCol = commonLib.getColumnList(((EqualsTo) exp).getRightExpression());
                            String comparisonValue ;
                            if (leftCol == null || leftCol.size() == 0){
                                comparisonValue = (((EqualsTo) exp).getLeftExpression()).toString() ;
                            }else{
                                comparisonValue = (((EqualsTo) exp).getRightExpression()).toString() ;
                            }
                            comparisonValue = comparisonValue.substring(1, comparisonValue.length() - 1);
                            for(PrimitiveValue[] currentRow: Main.masterIndex.get(tableName + "|" + columnNames.get(i))){
                                String leftVal = currentRow[0].toRawString() ;
                                String rightVal = currentRow[1].toRawString() ;
                                if(leftVal.compareTo(comparisonValue) <= 0){
                                    if(rightVal.compareTo(comparisonValue) >= 0){
                                        listOfFileNames.add(currentRow[2]) ;
                                    }
                                }
                            }
                        }else{
                            for(PrimitiveValue[] currentRow: Main.masterIndex.get(tableName + "|" + columnNames.get(i))){
                                Expression leftEval = new GreaterThanEquals(((EqualsTo) exp).getRightExpression(), ((EqualsTo) exp).getLeftExpression());
                                convertedRow.setPrimitiveValue(currentRow[0]);
                                convertedRow.setColumnDefinition(columnIndices.get(i));
                                convertedRow.setTableName(this.tableName);
                                start_end[0] = convertedRow ;
                                result_start = (PrimitiveValueWrapper) commonLib.eval(leftEval, start_end);

                                Expression rightEval = new GreaterThanEquals(((EqualsTo) exp).getLeftExpression(), ((EqualsTo) exp).getRightExpression());
                                convertedRow.setPrimitiveValue(currentRow[1]);
                                convertedRow.setColumnDefinition(columnIndices.get(i));
                                convertedRow.setTableName(this.tableName);
                                start_end[0] = convertedRow ;
                                result_end = (PrimitiveValueWrapper) commonLib.eval(rightEval, start_end);

                                if(result_end.getPrimitiveValue().toBool() && result_start.getPrimitiveValue().toBool()) {
                                    listOfFileNames.add(currentRow[2]) ;
                                }
                            }
                        }

                    }
                }

            }
            listOfFinalFileList.add(listOfFileNames) ;
        }



        PrimitiveValueWrapper[] convertedRowForEval = new PrimitiveValueWrapper[1];
        PrimitiveValueWrapper convertedRow = new PrimitiveValueWrapper();
        PrimitiveValueWrapper result ;

        for(int i = 0; i < listOfFinalFileList.size(); i++){

            ArrayList<Long> currentPositions = new ArrayList<Long>() ;

            for(int j = 0; j < listOfFinalFileList.get(i).size(); j++){
                File File = new File(CommonLib.INDEX_DIRECTORY + listOfFinalFileList.get(i).get(j).toRawString());
                FileInputStream fileInputStream = new FileInputStream(File);
                BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                ObjectInputStream br = new ObjectInputStream(bufferedInputStream);
                PrimitiveValue[] currentRow =(PrimitiveValue[]) br.readUnshared() ;

                while(currentRow != null){
                    if(j == 0 || j == listOfFinalFileList.get(i).size()-1){
                        convertedRow.setPrimitiveValue(currentRow[0]);
                        convertedRow.setColumnDefinition(columnIndices.get(i));
                        convertedRow.setTableName(this.tableName);
                        convertedRowForEval[0] = convertedRow ;
                        result = (PrimitiveValueWrapper) commonLib.eval(this.expressions.get(i), convertedRowForEval);
                        if(result.getPrimitiveValue().toBool()){
                            currentPositions.add(currentRow[1].toLong());
                        }
                    }else{
                        currentPositions.add(currentRow[1].toLong());
                    }
                    currentRow =(PrimitiveValue[]) br.readUnshared() ;
                }
                fileInputStream.close() ;
                bufferedInputStream.close();
                br.close();
            }
            if(i == 0){
                Collections.sort(currentPositions);
                positions = currentPositions ;
            }else{
                Collections.sort(currentPositions);
                positions = intersectLists(positions, currentPositions) ;
            }
        }
        positions.add(positions.size(), 0L);
    }

    private ArrayList<Long> intersectLists(ArrayList<Long> leftFiles, ArrayList<Long> rightFiles) {
        ArrayList<Long> merged = new ArrayList<Long>() ;
        int i = 0 ;
        int j = 0 ;
        while (i < leftFiles.size() && j < rightFiles.size()){
            if(leftFiles.get(i) < rightFiles.get(j)){
                i += 1 ;
            }else if(leftFiles.get(i).equals(rightFiles.get(j))){
                merged.add(leftFiles.get(i)) ;
                i += 1 ;
                j += 1 ;
            }else{
                j += 1 ;
            }
        }
        return merged ;
    }

    @Override
    public boolean hasNext() throws Exception {
        try {
            if (hasNext)
                return true;

            if(positions.size() == 0){

                if ((nextLine = tupleClass.covertTupleToPrimitiveValuePP(br.readLine())) != null) {
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

            if (currentPositionToRead >= positions.size()) {
                hasNext = false;
                return null;
            }

            br.skip(nextPosition - currentPosition);
            val = br.readLine();
            if ((nextLine = tupleClass.covertTupleToPrimitiveValuePP(val)) != null) {
                hasNext = true;
                currentPosition = nextPosition + val.length() + 1;
                if (currentPositionToRead == positions.size())
                    return nextLine;
                nextPosition = positions.get(currentPositionToRead++);
                return nextLine;
            }

            hasNext = false;
            return null;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public PrimitiveValue[] next() throws Exception {
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

    @Override
    public void setSchema(Schema[] schema) {
        this.schema = schema;
    }
    @Override
    public RAIterator optimize(RAIterator iterator) {
        return iterator;
    }
}