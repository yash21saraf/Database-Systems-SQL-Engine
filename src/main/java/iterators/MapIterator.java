package iterators;

import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static helpers.CommonLib.castAs;

public class MapIterator implements RAIterator
{
    //region Variables

    //private static final Logger logger = LogManager.getLogger();
    private CommonLib commonLib = CommonLib.getInstance();

    private RAIterator child;
    private List<SelectItem> selectItems;
    private String tableAlias;
    private String tableName;
    private ColumnDefinition[] columnDefinitions;


    //endregion

    //region Constructor

    public MapIterator(RAIterator child,List<SelectItem> selectItems,String tableAlias) throws Exception {

        this.child = child;
        this.selectItems = selectItems;
        this.tableAlias = tableAlias;
        if (isAggregateQuery(selectItems)) {
            this.selectItems = getUnpackedSelectedItems(selectItems);
        }


        SelectExpressionItem selectExpressionItem;
        AllTableColumns allTableColumns;
        AllColumns allColumns;
        Column column;


        ColumnDefinition[] childColumnDefinition = child.getColumnDefinition();
        String childTableName = child.getTableName() ;
        String childTableAlias = child.getTableAlias() ;
        ArrayList<ColumnDefinition> projectedTuplenew = new ArrayList() ;
        ArrayList<PrimitiveValueWrapper> projectedTuple = new ArrayList();

        for (int index = 0; index < selectItems.size(); index++) {
            if ((selectExpressionItem = (SelectExpressionItem) CommonLib.castAs(selectItems.get(index),SelectExpressionItem.class)) != null) {
//                PrimitiveValueWrapper evaluatedExpression = commonLib.eval(selectExpressionItem.getExpression(),tuple);

                ColumnDefinition evaluatedColumnDefinition = new ColumnDefinition();
                evaluatedColumnDefinition.setColumnName(childColumnDefinition[index].getColumnName());
                evaluatedColumnDefinition.setColDataType(childColumnDefinition[index].getColDataType());
                evaluatedColumnDefinition.setColumnSpecStrings(childColumnDefinition[index].getColumnSpecStrings());

                evaluatedColumnDefinition.setColumnName(selectExpressionItem.getAlias());

                if (tableAlias != null)
                    this.tableName = tableAlias ;

                projectedTuplenew.add(evaluatedColumnDefinition);

            } else if ((allTableColumns = (AllTableColumns) CommonLib.castAs(selectItems.get(index),AllTableColumns.class)) != null) {
                for (int secondIndex = 0; secondIndex < childColumnDefinition.length; secondIndex++) {
                    if (childColumnDefinition[secondIndex].getColumnName() == null)
                        throw new Exception("No column name specified for column at index " + index + " for " + tableAlias);
                    else if (childTableName.equals(allTableColumns.getTable().getName())) {
                        projectedTuplenew.add(childColumnDefinition[secondIndex]) ;
                    }
                }

            } else if ((allColumns = (AllColumns) CommonLib.castAs(selectItems.get(index),AllColumns.class)) != null) {
                for (int secondIndex = 0; secondIndex < childColumnDefinition.length; secondIndex++) {
                    if (childColumnDefinition[secondIndex].getColumnName() == null)
                        throw new Exception("No column name specified for column at index " + index + " for " + tableAlias);
                }
                projectedTuplenew.addAll(Arrays.asList(childColumnDefinition));
            }
        }

        this.columnDefinitions = projectedTuplenew.toArray(new ColumnDefinition[projectedTuplenew.size()]) ;
    }

    //endregion

    //region Iterator methods

    @Override
    public boolean hasNext() throws Exception
    {
        return child.hasNext();
    }

    @Override
    public PrimitiveValue[] next() throws Exception
    {

        SelectExpressionItem selectExpressionItem;
        AllTableColumns allTableColumns;
        AllColumns allColumns;
        Column column;

        PrimitiveValue[] tuple = child.next() ;
//        PrimitiveValueWrapper[] tuple = child.next();
        PrimitiveValueWrapper[] wrappedTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(tuple, child.getColumnDefinition(), child.getTableName());
        ArrayList<PrimitiveValue> projectedTuple = new ArrayList();

        if (tuple == null)
            return null;
        for (int index = 0; index < selectItems.size(); index++) {
            if ((selectExpressionItem = (SelectExpressionItem) CommonLib.castAs(selectItems.get(index),SelectExpressionItem.class)) != null) {
                PrimitiveValueWrapper evaluatedExpression = commonLib.eval(selectExpressionItem.getExpression(),wrappedTuple);
                projectedTuple.add(evaluatedExpression.getPrimitiveValue());

            } else if ((allTableColumns = (AllTableColumns) CommonLib.castAs(selectItems.get(index),AllTableColumns.class)) != null) {
                for (int secondIndex = 0; secondIndex < tuple.length; secondIndex++) {
                    if (this.columnDefinitions[secondIndex].getColumnName() == null)
                        throw new Exception("No column name specified for column at index " + index + " for " + tableAlias);
                    else if (this.tableName.equals(allTableColumns.getTable().getName())) {
                        projectedTuple.add(tuple[secondIndex]);
                    }
                }

            } else if ((allColumns = (AllColumns) CommonLib.castAs(selectItems.get(index),AllColumns.class)) != null) {
                for (int secondIndex = 0; secondIndex < tuple.length; secondIndex++) {
                    if (this.columnDefinitions[secondIndex].getColumnName() == null)
                        throw new Exception("No column name specified for column at index " + index + " for " + tableAlias);
                }
                return tuple;

            }
        }

        return projectedTuple.toArray(new PrimitiveValue[projectedTuple.size()]);
    }

    @Override
    public void reset() throws Exception
    {
        child.reset();
    }

    @Override
    public RAIterator getChild() {
        return this.child;
    }

    @Override
    public void setChild(RAIterator child) {
        this.child = child ;
    }

    @Override
    public ColumnDefinition[] getColumnDefinition() {
        return new ColumnDefinition[0];
    }

    @Override
    public void setColumnDefinition(ColumnDefinition[] columnDefinition) {

    }

    @Override
    public void setTableName(String tableName) {

    }

    @Override
    public String getTableName() {
        return null;
    }

    @Override
    public void setTableAlias(String tableAlias) {

    }

    @Override
    public String getTableAlias() {
        return null;
    }


    private boolean isAggregateQuery(List<SelectItem> selectItems) {
        Function function;

        for (int index = 0; index < selectItems.size(); index++) {

            if ((function = (Function) castAs(((SelectExpressionItem) selectItems.get(index)).getExpression(), Function.class)) != null) {
                return true;
            }
        }
        return false;
    }

    private List<SelectItem> getUnpackedSelectedItems(List<SelectItem> selectItems) {

        //PlainSelect unpackedPlainItems = plainSelect;
        SelectExpressionItem selectExpressionItem;
        SelectExpressionItem temp;
        Function function;
        Addition addition;
        List<SelectItem> finalList = new ArrayList();

        for (SelectItem selectItem : selectItems) {
            temp = new SelectExpressionItem();
            if ((selectExpressionItem = (SelectExpressionItem) castAs(selectItem, SelectExpressionItem.class)) != null) {
                /*if(selectExpressionItem.getAlias() == null)
                    selectExpressionItem.setAlias(selectExpressionItem.getExpression().toString());*/
                if ((function = (Function) castAs(selectExpressionItem.getExpression(), Function.class)) != null) {
                    if (!function.isAllColumns()) {
                        temp.setExpression(function.getParameters().getExpressions().get(0));
                        if (selectExpressionItem.getAlias() == null) {
                            temp.setAlias(selectExpressionItem.getExpression().toString());
                            //aggColMap.put(selectExpressionItem.getExpression().toString(), (((Function) selectExpressionItem.getExpression()).getName()));
                        } else {
                            temp.setAlias(selectExpressionItem.getAlias());
                            //aggColMap.put(selectExpressionItem.getAlias(), (((Function) selectExpressionItem.getExpression()).getName()));
                        }
                    } else if (function.isAllColumns()) {
                        if (function.getParameters() != null && selectExpressionItem.getAlias() != null) {
                            temp.setExpression(function.getParameters().getExpressions().get(0));
                            temp.setAlias(selectExpressionItem.getAlias());
                        } else {
                            LongValue expression = new LongValue(1);
                            temp.setExpression(expression); // TODO : How to pass Count(*) expression during unpacking?
                            temp.setAlias(selectExpressionItem.getExpression().toString());
                            //aggColMap.put(temp.getAlias(), "count");
                        }
                    }
                } else {
                    temp.setExpression(selectExpressionItem.getExpression());
                    temp.setAlias(selectExpressionItem.getAlias());
                    // Group by Columns : Not Required
                    // groupByMap.put((selectExpressionItem.getExpression()).toString(), selectExpressionItem.getAlias());
                }

                finalList.add(temp);
            } else { // Check for sub-query in projections

            }

        }
        //plainSelect.setSelectItems(new ArrayList(selectItem));

        if (finalList.size() == 0)
            return selectItems;

        return finalList;
    }


    //endregion

}