package iterators;

import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MapIterator implements RAIterator
{
   //region Variables

   private static final Logger logger = LogManager.getLogger();
   private CommonLib commonLib = CommonLib.getInstance();

   private RAIterator child;
   private List<SelectItem> selectItems;
   private String tableAlias;

   //endregion

   //region Constructor

   public MapIterator(RAIterator child,List<SelectItem> selectItems,String tableAlias)
   {

      this.child = child;
      this.selectItems = selectItems;
      this.tableAlias = tableAlias;

   }

   //endregion

   //region Iterator methods

   @Override
   public boolean hasNext() throws Exception
   {
      return child.hasNext();
   }

   @Override
   public PrimitiveValueWrapper[] next() throws Exception
   {

      SelectExpressionItem selectExpressionItem;
      AllTableColumns allTableColumns;
      AllColumns allColumns;
      Column column;

      PrimitiveValueWrapper[] tuple = child.next();
      ArrayList<PrimitiveValueWrapper> projectedTuple = new ArrayList<>();

      if (tuple == null)
         return null;
      for (int index = 0; index < selectItems.size(); index++) {
         if ((selectExpressionItem = (SelectExpressionItem) CommonLib.castAs(selectItems.get(index),SelectExpressionItem.class)) != null) {
            PrimitiveValueWrapper evaluatedExpression = commonLib.eval(selectExpressionItem.getExpression(),tuple);

            ColumnDefinition evaluatedColumnDefinition = new ColumnDefinition();
            evaluatedColumnDefinition.setColumnName(tuple[index].getColumnDefinition().getColumnName());
            evaluatedColumnDefinition.setColDataType(tuple[index].getColumnDefinition().getColDataType());
            evaluatedColumnDefinition.setColumnSpecStrings(tuple[index].getColumnDefinition().getColumnSpecStrings());
            for (PrimitiveValueWrapper selectedValue : projectedTuple) {
               if (selectExpressionItem.getAlias() != null) {
                  if (selectedValue.getColumnDefinition().getColumnName().equals(selectExpressionItem.getAlias()))
                     throw new Exception(selectExpressionItem.getAlias() + " is an ambiguous column name.");
               } else if ((column = (Column) CommonLib.castAs(selectExpressionItem,Column.class)) != null)
                  if (selectedValue.getColumnDefinition().getColumnName().equals(column.getColumnName()))
                     throw new Exception(column.getColumnName() + " was specified multiple times.");

            }
            evaluatedColumnDefinition.setColumnName(selectExpressionItem.getAlias());
            evaluatedExpression.setColumnDefinition(evaluatedColumnDefinition);

            if (tableAlias != null)
               evaluatedExpression.setTableName(tableAlias);

            projectedTuple.add(evaluatedExpression);

         } else if ((allTableColumns = (AllTableColumns) CommonLib.castAs(selectItems.get(index),AllTableColumns.class)) != null) {
            for (int secondIndex = 0; secondIndex < tuple.length; secondIndex++) {
               if (tuple[secondIndex].getColumnDefinition().getColumnName() == null)
                  throw new Exception("No column name specified for column at index " + index + " for " + tableAlias);
               else if (tuple[secondIndex].getTableName().equals(allTableColumns.getTable().getName())) {
                  projectedTuple.add(tuple[secondIndex]);
               }
            }

         } else if ((allColumns = (AllColumns) CommonLib.castAs(selectItems.get(index),AllColumns.class)) != null) {
            for (int secondIndex = 0; secondIndex < tuple.length; secondIndex++) {
               if (tuple[secondIndex].getColumnDefinition().getColumnName() == null)
                  throw new Exception("No column name specified for column at index " + index + " for " + tableAlias);
            }
            return tuple;

         }
      }

      return projectedTuple.toArray(new PrimitiveValueWrapper[projectedTuple.size()]);

   }

   @Override
   public void reset() throws Exception
   {
      child.reset();
   }

   //endregion
}
