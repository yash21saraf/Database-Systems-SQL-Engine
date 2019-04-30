package helpers;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class CommonLib
{

    public volatile int fileSequenceNumber = 100000;
    public static volatile int sortFileSeqNumber = 1000;
    public static volatile int mergeFileSeqNumber = 10000;
    private static CommonLib commonLib = CommonLib.getInstance();
    public static long blockSize = 15110000;

   public static final int N = 50;
   public static List<String> listOfSortedFiles = new ArrayList<String>();
   public static HashMap<String, Sort> mapOfSortedFileObjects = new HashMap<String, Sort>();
   public volatile int sortMergeSeqNumber = 50000;
   public volatile int orderBySeqNumber = 70000;

      public static final String TABLE_DIRECTORY = "/home/yash/Desktop/Databases/data/TPCHDATA/";
//      public static final String TABLE_DIRECTORY = "/Users/deepak/Desktop/Database/data/a/thcp/TPCHinmem/";
//   public static final String TABLE_DIRECTORY = "/Users/deepak/Desktop/Database/data/a/thcp/TPCHDATA/";
   public static final String extension = ".csv" ;
   //   public static final String TABLE_DIRECTORY = "data/";
   public static final String INDEX_DIRECTORY = "/home/yash/Desktop/Databases/data/TPCHDATA/Index/";


   //region Variables

   private Eval eval = new Eval()
   {
      @Override
      public PrimitiveValue eval(Column column) throws SQLException
      {
         for (PrimitiveValueWrapper[] tuple : tuples) {
            for (PrimitiveValueWrapper value : tuple) {
               if (column.getWholeColumnName().equals(value.getWholeColumnName()))
                  return value.getPrimitiveValue();
               else if (column.getTable().getName() == null && column.getTable().getAlias() == null && column.getWholeColumnName().equals(value.getColumnDefinition().getColumnName()))
                  return value.getPrimitiveValue();
            }
         }
         throw new SQLException("No column with name: " + column.getColumnName() + ".");
      }
   };

   private PrimitiveValueWrapper[] tuple;
   private ArrayList<PrimitiveValueWrapper[]> tuples = new ArrayList();

   //endregion

   //region Singleton implementation



   private static CommonLib instance;

   private CommonLib()
   {
   }

   public static CommonLib getInstance()
   {

      if (instance == null) {
         synchronized (CommonLib.class) {
            if (instance == null) {
               instance = new CommonLib();
            }
         }
      }

      return instance;

   }

   //endregion

   //region PrimitiveValue conversion methods

   public PrimitiveValueWrapper[] convertTupleStringToPrimitiveValueWrapperArray(String tupleString,ColumnDefinition[] columnDefinitions,String tableName) throws Exception
   {

      if (tupleString == null)
         return null;

      String[] tupleArray = tupleString.split("\\|");
      PrimitiveValueWrapper[] convertedTuple = new PrimitiveValueWrapper[tupleArray.length];


      for (int index = 0; index < tupleArray.length; index++) {
         PrimitiveValueWrapper convertedValue = new PrimitiveValueWrapper();
         convertedValue.setPrimitiveValue(convertToPrimitiveValue(tupleArray[index],columnDefinitions[index].getColDataType().getDataType()));
         if (convertedValue.getPrimitiveValue() != null) {
            convertedValue.setColumnDefinition(columnDefinitions[index]);
            convertedValue.setTableName(tableName);
            convertedTuple[index] = convertedValue;
         } else {
            //logger.error("Invalid columnType: {} at columnName: {}.",columnDefinitions[index].getColDataType().getDataType(),columnDefinitions[index].getColumnName());
            throw new Exception("Invalid columnType.");
         }
      }
      return convertedTuple;

   }

   public PrimitiveValueWrapper[] convertTuplePrimitiveValueToPrimitiveValueWrapperArray(PrimitiveValue[] tuple,ColumnDefinition[] columnDefinitions,String tableName) throws Exception
   {

      if (tuple == null)
         return null;

      PrimitiveValueWrapper[] convertedTuple = new PrimitiveValueWrapper[tuple.length];


      for (int index = 0; index < tuple.length; index++) {
         PrimitiveValueWrapper convertedValue = new PrimitiveValueWrapper();
         convertedValue.setPrimitiveValue(tuple[index]);
         if (convertedValue.getPrimitiveValue() != null) {
            convertedValue.setColumnDefinition(columnDefinitions[index]);
            convertedValue.setTableName(tableName);
            convertedTuple[index] = convertedValue;
         } else {
            //logger.error("Invalid columnType: {} at columnName: {}.",columnDefinitions[index].getColDataType().getDataType(),columnDefinitions[index].getColumnName());
            throw new Exception("Invalid columnType.");
         }
      }
      return convertedTuple;

   }

   public PrimitiveValueWrapper[] convertTuplePrimitiveValueToPrimitiveValueWrapperArray(PrimitiveValue[] tuple,Schema[] schema) throws Exception
   {
      PrimitiveValueWrapper[] convertedTuple;
      PrimitiveValueWrapper convertedValuein;
      if (tuple == null)
         return null;

      convertedTuple = new PrimitiveValueWrapper[tuple.length];


      for (int index = 0; index < tuple.length; index++) {
          convertedValuein = new PrimitiveValueWrapper();
          convertedValuein.setPrimitiveValue(tuple[index]);
         if (convertedValuein.getPrimitiveValue() != null) {
             convertedValuein.setColumnDefinition(schema[index].getColumnDefinition());
             convertedValuein.setTableName(schema[index].getTableName());
            convertedTuple[index] = convertedValuein;
         } else {
            //logger.error("Invalid columnType: {} at columnName: {}.",columnDefinitions[index].getColDataType().getDataType(),columnDefinitions[index].getColumnName());
            throw new Exception("Invalid columnType.");
         }
      }
      return convertedTuple;

   }


   public PrimitiveValue[] covertTupleToPrimitiveValue(String tupleString,ColumnDefinition[] columnDefinitions) throws Exception
   {
      String[] tupleArray;
      PrimitiveValue[] convertedTuple1;
      tupleArray = null;
      convertedTuple1 = null;

      if (tupleString == null)
         return null;


      tupleArray = tupleString.split("\\|");

      //StringBuilder stringBuilder = new StringBuilder();

      convertedTuple1 = new PrimitiveValue[tupleArray.length];

      for (int index = 0; index < tupleArray.length; index++) {
         PrimitiveValue convertedValue;
         convertedValue = convertToPrimitiveValue(tupleArray[index],columnDefinitions[index].getColDataType().getDataType());
         if (convertedValue != null) {
            convertedTuple1[index] = convertedValue;
         } else {
            //logger.error("Invalid columnType: {} at columnName: {}.",columnDefinitions[index].getColDataType().getDataType(),columnDefinitions[index].getColumnName());
            throw new Exception("Invalid columnType.");
         }
      }
      return convertedTuple1;

   }

   public static PrimitiveValue convertToPrimitiveValue(String value, String dataType)
   {
      if (("INT").equals(dataType.toUpperCase()))
         return new LongValue(value);
      if (("STRING").equals(dataType.toUpperCase()))
         return new StringValue(value);
      if (("VARCHAR").equals(dataType.toUpperCase()))
         return new StringValue(value);
      if (("CHAR").equals(dataType.toUpperCase()))
         return new StringValue(value);
      if (("DECIMAL").equals(dataType.toUpperCase()))
         return new DoubleValue(value);
      if (("DATE").equals(dataType.toUpperCase()))
         return new DateValue(value);
      return null;
   }

   //endregion

   //region Eval methods

   public PrimitiveValueWrapper eval(Expression expression,PrimitiveValueWrapper[] tuple) throws SQLException
   {
      try {
         PrimitiveValueWrapper evaluatedExpression = new PrimitiveValueWrapper();
         this.tuples.add(tuple);
         evaluatedExpression.setPrimitiveValue(eval.eval(expression));
         this.tuples.remove(tuple);
         return evaluatedExpression;
      } catch (SQLException e) {
         //e.printStackTrace();
         throw e;
      }
   }

   public PrimitiveValueWrapper eval(Expression expression,PrimitiveValueWrapper[]... tuples) throws SQLException
   {
      try {
         PrimitiveValueWrapper evaluatedExpression = new PrimitiveValueWrapper();
         this.tuples.addAll(Arrays.asList(tuples));
         evaluatedExpression.setPrimitiveValue(eval.eval(expression));
         this.tuples.removeAll(Arrays.asList(tuples));
         return evaluatedExpression;
      } catch (SQLException e) {
         // logger.error("Exception in eval() call.");
         throw e;
      }
   }

   public List<Expression> getExpressionList(Expression expression)
   {

      List<Expression> expressionList = new ArrayList<Expression>();
      AndExpression andExpression;

      if ((andExpression = (AndExpression) castAs(expression,AndExpression.class)) != null) {
         expressionList.addAll(getExpressionList(andExpression.getLeftExpression()));
         expressionList.addAll(getExpressionList(andExpression.getRightExpression()));
      } else {
         expressionList.add(expression);
      }

      return expressionList;

   }

   public List<Column> getColumnList(Expression expression)
   {

      List<Column> columnList = new ArrayList<Column>();
      Column column;
      OrExpression orExpression;
      EqualsTo equalsTo;
      NotEqualsTo notEqualsTo;
      GreaterThan greaterThan;
      GreaterThanEquals greaterThanEquals;
      MinorThan minorThan;
      MinorThanEquals minorThanEquals;
      AndExpression andExpression;
      List<Expression> expressionList;
      Addition addition;
      Subtraction subtraction;
      Multiplication multiplication;
      Division division;
      CaseExpression caseExpression;


      if ((column = (Column) castAs(expression,Column.class)) != null) {
         columnList.add(column);
      }
      else if ((andExpression = (AndExpression) castAs(expression,AndExpression.class)) != null) {
         expressionList = getExpressionList(expression);
         for (Expression expressionItem : expressionList) {
            columnList.addAll(getColumnList(expressionItem));
         }
      }
      else if ((orExpression = (OrExpression) castAs(expression,OrExpression.class)) != null) {
         columnList.addAll(getColumnList(orExpression.getLeftExpression()));
         columnList.addAll(getColumnList(orExpression.getRightExpression()));
      }
      else if ((equalsTo = (EqualsTo) castAs(expression,EqualsTo.class)) != null) {
         columnList.addAll(getColumnList(equalsTo.getLeftExpression()));
         columnList.addAll(getColumnList(equalsTo.getRightExpression()));
      }
      else if ((notEqualsTo = (NotEqualsTo) castAs(expression,NotEqualsTo.class)) != null) {
         columnList.addAll(getColumnList(notEqualsTo.getLeftExpression()));
         columnList.addAll(getColumnList(notEqualsTo.getRightExpression()));
      }
      else if ((greaterThan = (GreaterThan) castAs(expression,GreaterThan.class)) != null) {
         columnList.addAll(getColumnList(greaterThan.getLeftExpression()));
         columnList.addAll(getColumnList(greaterThan.getRightExpression()));
      }
      else if ((greaterThanEquals = (GreaterThanEquals) castAs(expression,GreaterThanEquals.class)) != null) {
         columnList.addAll(getColumnList(greaterThanEquals.getLeftExpression()));
         columnList.addAll(getColumnList(greaterThanEquals.getRightExpression()));
      }
      else if ((minorThan = (MinorThan) castAs(expression,MinorThan.class)) != null) {
         columnList.addAll(getColumnList(minorThan.getLeftExpression()));
         columnList.addAll(getColumnList(minorThan.getRightExpression()));
      }
      else if ((minorThanEquals = (MinorThanEquals) castAs(expression,MinorThanEquals.class)) != null) {
         columnList.addAll(getColumnList(minorThanEquals.getLeftExpression()));
         columnList.addAll(getColumnList(minorThanEquals.getRightExpression()));
      }
      else if ((addition = (Addition) castAs(expression,Addition.class)) != null) {
         columnList.addAll(getColumnList(addition.getLeftExpression()));
         columnList.addAll(getColumnList(addition.getRightExpression()));
      }
      else if ((subtraction = (Subtraction) castAs(expression,Subtraction.class)) != null) {
         columnList.addAll(getColumnList(subtraction.getLeftExpression()));
         columnList.addAll(getColumnList(subtraction.getRightExpression()));
      }
      else if ((multiplication = (Multiplication) castAs(expression,Multiplication.class)) != null) {
         columnList.addAll(getColumnList(multiplication.getLeftExpression()));
         columnList.addAll(getColumnList(multiplication.getRightExpression()));
      }
      else if ((division = (Division) castAs(expression,Division.class)) != null) {
         columnList.addAll(getColumnList(division.getLeftExpression()));
         columnList.addAll(getColumnList(division.getRightExpression()));
      }
      else if ((caseExpression = (CaseExpression) castAs(expression,CaseExpression.class)) != null) {

         //Column column1 = new Column(new Table("ORDERS"), "ORDERPRIORITY");
         //column1.setColumnName("ORDERPRIORITY");
         columnList.addAll(getColumnList(caseExpression.getWhenClauses().get(0).getWhenExpression()));
      }

      return columnList;

   }

   public synchronized int getOrderBySeqNumber() {
      return orderBySeqNumber++;
   }

   public synchronized static int getsortFileSeqNumber() {
      return sortFileSeqNumber++;
   }

   public synchronized int getFileSequenceNumber() {
       return fileSequenceNumber++;
   }

   public synchronized int getSortMergeSeqNumber() {
      return sortMergeSeqNumber++;
   }

   //endregion

   //region Singleton implementation

   public synchronized static int getmergeFileSeqNumber() {
      return mergeFileSeqNumber++;
   }

   public int getN() {
      return N;
   }

   public Boolean validateExpressionAgainstSchema(Expression expression, Schema[] schemas) {

      boolean flag = false;
      for (Column column : getColumnList(expression)) {
         for (Schema schema : schemas) {
            if (schema.getWholeColumnName().equals(column.getWholeColumnName())) {
               flag = true;
            } else if (schema.getColumnDefinition().getColumnName().equals(column.getWholeColumnName())) {
               flag = true;
            }
         }
         if (!flag) {
            return false;
         }
         flag = false;
      }
      return true;

   }

   public List<Integer> ColToSchemaIndexes(List<Column> ColoumList, Schema[] schemas) {
      List<Integer> ColIndexes = new ArrayList<Integer>() ;
      boolean flag = false;
      for (Column column : ColoumList) {
         for (int i = 0 ; i < schemas.length ; i++) {
            if (schemas[i].getWholeColumnName().equals(column.getWholeColumnName())) {
               ColIndexes.add(i) ;
            } else if (schemas[i].getColumnDefinition().getColumnName().equals(column.getWholeColumnName())) {
               ColIndexes.add(i) ;
            }
         }
      }
      return ColIndexes;
   }

   //endregion

   //region Helper methods

   public static <T> Object castAs(T queryPart,Class<? extends T> clazz)
   {
      if (clazz.isInstance(queryPart))
         return clazz.cast(queryPart);
      return null;
   }

   public static <T> T[] concatArrays(T[] first,T[]... rest)
   {
      int totalLength = first.length;
      for (T[] array : rest) {
         totalLength += array.length;
      }
      T[] result = Arrays.copyOf(first,totalLength);
      int offset = first.length;
      for (T[] array : rest) {
         System.arraycopy(array,0,result,offset,array.length);
         offset += array.length;
      }
      return result;
   }


   public PrimitiveValue PrimitiveValueComparator(PrimitiveValue first, PrimitiveValue second, String operator) throws PrimitiveValue.InvalidPrimitive {

      if(operator.equals("sum")){
         if(first instanceof LongValue){
            return new LongValue(first.toLong() + second.toLong()) ;
         }else{
            return new DoubleValue(first.toDouble() + second.toDouble()) ;
         }
      }
      else if(operator.equals("min")){
         if(first instanceof LongValue){
              return new LongValue(Math.min(first.toLong(), second.toLong()));
         }
         else if(first instanceof DoubleValue){
            return new DoubleValue(Math.min(first.toDouble(), second.toDouble())) ;
         }
         else if(first instanceof DateValue){
            int comp = ((DateValue) first).getValue().compareTo(((DateValue) second).getValue());
            if(comp < 0){
               return new DateValue(first.toRawString());
            }
            else return new DateValue(second.toRawString()) ;
         }
         else{
            int comp = first.toRawString().compareTo(second.toRawString());
            if(comp < 0 ){
               return new StringValue(first.toRawString()) ;
            }
            else return new StringValue(second.toRawString()) ;
         }
      }else if(operator.toLowerCase().equals("max")){
         if(first instanceof LongValue){
            return new LongValue(Math.max(first.toLong(), second.toLong()));
         }
         else if(first instanceof DoubleValue){
            return new DoubleValue(Math.max(first.toDouble(), second.toDouble())) ;
         }
         else if(first instanceof DateValue){
            int comp = ((DateValue) first).getValue().compareTo(((DateValue) second).getValue());
            if(comp > 0){
               return new DateValue(first.toRawString());
            }
            else return new DateValue(second.toRawString()) ;
         }
         else{
            int comp = first.toRawString().compareTo(second.toRawString());
            if(comp > 0 ){
               return new StringValue(first.toRawString()) ;
            }
            else return new StringValue(second.toRawString()) ;
         }
      }
      return null ;
   }

   public static boolean isNumber(String str) {
      try {
         Double.parseDouble(str);
         return true;
      } catch (NumberFormatException e) {
         return false;
      }
   }
   //endregion

}
