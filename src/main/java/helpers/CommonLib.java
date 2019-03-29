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
import java.util.List;

public class CommonLib
{


    public static volatile int sortFileSeqNumber = 1000;
    public static volatile int mergeFileSeqNumber = 10000;
    private static CommonLib commonLib = CommonLib.getInstance();
    public static long blockSize = 100000;

    public synchronized static int getsortFileSeqNumber() {
        return sortFileSeqNumber++;
    }

    public synchronized static int getmergeFileSeqNumber() {
        return mergeFileSeqNumber++;
    }

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

      if (tuple == null)
         return null;

      PrimitiveValueWrapper[] convertedTuple = new PrimitiveValueWrapper[tuple.length];


      for (int index = 0; index < tuple.length; index++) {
         PrimitiveValueWrapper convertedValue = new PrimitiveValueWrapper();
         convertedValue.setPrimitiveValue(tuple[index]);
         if (convertedValue.getPrimitiveValue() != null) {
            convertedValue.setColumnDefinition(schema[index].getColumnDefinition());
            convertedValue.setTableName(schema[index].getTableName());
            convertedTuple[index] = convertedValue;
         } else {
            //logger.error("Invalid columnType: {} at columnName: {}.",columnDefinitions[index].getColDataType().getDataType(),columnDefinitions[index].getColumnName());
            throw new Exception("Invalid columnType.");
         }
      }
      return convertedTuple;

   }


   public PrimitiveValue[] covertTupleToPrimitiveValue(String tupleString,ColumnDefinition[] columnDefinitions) throws Exception
   {

      if (tupleString == null)
         return null;

      String[] tupleArray = tupleString.split("\\|");
      PrimitiveValue[] convertedTuple = new PrimitiveValue[tupleArray.length];


      for (int index = 0; index < tupleArray.length; index++) {
         PrimitiveValue convertedValue;
         convertedValue = convertToPrimitiveValue(tupleArray[index],columnDefinitions[index].getColDataType().getDataType());
         if (convertedValue != null) {
            convertedTuple[index] = convertedValue;
         } else {
            //logger.error("Invalid columnType: {} at columnName: {}.",columnDefinitions[index].getColDataType().getDataType(),columnDefinitions[index].getColumnName());
            throw new Exception("Invalid columnType.");
         }
      }
      return convertedTuple;

   }

   public PrimitiveValue convertToPrimitiveValue(String value,String dataType)
   {
      if (("STRING").equals(dataType.toUpperCase()))
         return new StringValue(value);
      if (("VARCHAR").equals(dataType.toUpperCase()))
         return new StringValue(value);
      if (("CHAR").equals(dataType.toUpperCase()))
         return new StringValue(value);
      if (("INT").equals(dataType.toUpperCase()))
         return new LongValue(value);
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


      if ((column = (Column) castAs(expression,Column.class)) != null) {
         columnList.add(column);
      }
      if ((andExpression = (AndExpression) castAs(expression,AndExpression.class)) != null) {
         expressionList = getExpressionList(expression);
         for (Expression expressionItem : expressionList) {
            columnList.addAll(getColumnList(expressionItem));
         }
      }
      if ((orExpression = (OrExpression) castAs(expression,OrExpression.class)) != null) {
         columnList.addAll(getColumnList(orExpression.getLeftExpression()));
         columnList.addAll(getColumnList(orExpression.getRightExpression()));
      }
      if ((equalsTo = (EqualsTo) castAs(expression,EqualsTo.class)) != null) {
         columnList.addAll(getColumnList(equalsTo.getLeftExpression()));
         columnList.addAll(getColumnList(equalsTo.getRightExpression()));
      }
      if ((notEqualsTo = (NotEqualsTo) castAs(expression,NotEqualsTo.class)) != null) {
         columnList.addAll(getColumnList(notEqualsTo.getLeftExpression()));
         columnList.addAll(getColumnList(notEqualsTo.getRightExpression()));
      }
      if ((greaterThan = (GreaterThan) castAs(expression,GreaterThan.class)) != null) {
         columnList.addAll(getColumnList(greaterThan.getLeftExpression()));
         columnList.addAll(getColumnList(greaterThan.getRightExpression()));
      }
      if ((greaterThanEquals = (GreaterThanEquals) castAs(expression,GreaterThanEquals.class)) != null) {
         columnList.addAll(getColumnList(greaterThanEquals.getLeftExpression()));
         columnList.addAll(getColumnList(greaterThanEquals.getRightExpression()));
      }
      if ((minorThan = (MinorThan) castAs(expression,MinorThan.class)) != null) {
         columnList.addAll(getColumnList(minorThan.getLeftExpression()));
         columnList.addAll(getColumnList(minorThan.getRightExpression()));
      }
      if ((minorThanEquals = (MinorThanEquals) castAs(expression,MinorThanEquals.class)) != null) {
         columnList.addAll(getColumnList(minorThanEquals.getLeftExpression()));
         columnList.addAll(getColumnList(minorThanEquals.getRightExpression()));
      }
      if ((addition = (Addition) castAs(expression,Addition.class)) != null) {
         columnList.addAll(getColumnList(addition.getLeftExpression()));
         columnList.addAll(getColumnList(addition.getRightExpression()));
      }
      if ((subtraction = (Subtraction) castAs(expression,Subtraction.class)) != null) {
         columnList.addAll(getColumnList(subtraction.getLeftExpression()));
         columnList.addAll(getColumnList(subtraction.getRightExpression()));
      }
      if ((multiplication = (Multiplication) castAs(expression,Multiplication.class)) != null) {
         columnList.addAll(getColumnList(multiplication.getLeftExpression()));
         columnList.addAll(getColumnList(multiplication.getRightExpression()));
      }
      if ((division = (Division) castAs(expression,Division.class)) != null) {
         columnList.addAll(getColumnList(division.getLeftExpression()));
         columnList.addAll(getColumnList(division.getRightExpression()));
      }

      return columnList;

   }

   /*public Boolean validateExpression(Expression expression, Schema[] schemas) {

   }*/

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

   //endregion

}
