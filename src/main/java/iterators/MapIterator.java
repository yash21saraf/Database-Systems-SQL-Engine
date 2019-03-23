package iterators;

import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.*;

import static helpers.CommonLib.castAs;

public class MapIterator implements RAIterator {
    //region Variables

    private static int counter = 0;
    private static int sum = 0;
    private static int min = Integer.MAX_VALUE;
    private static int max = Integer.MIN_VALUE;
    boolean useGroupByOutput = false;
    boolean getFromAggResults = false;
    boolean hasAvg = false;

    private CommonLib commonLib = CommonLib.getInstance();
    private RAIterator child;
    private List<SelectItem> selectItems;
    private String tableAlias;
    private List<Column> groupByColumnReferences;
    private Map<String, String> aggColMap = new HashMap();
    private List<String> aggValues = new ArrayList<String>();
    private Map<String, List<String>> groupByMap = new HashMap<String, List<String>>();

    //endregion

    //region Constructor

    public MapIterator(RAIterator child, List<SelectItem> selectItems, String tableAlias, List<Column> groupByColumnReferences) {

        this.child = child;
        this.selectItems = selectItems;
        this.tableAlias = tableAlias;
        this.groupByColumnReferences = groupByColumnReferences;
    }

    //endregion

    //region Iterator methods

    private static void initialize(boolean mul) {
        counter = 0;
        sum = 0;
        min = Integer.MAX_VALUE;
        max = Integer.MIN_VALUE;
    }

    private static double finalize(String type) {

        if (type.equals("count"))
            return counter;
        if (type.equals("sum"))
            return sum;
        if (type.equals("avg"))
            return sum / counter;
        if (type.equals("min"))
            return min;
        if (type.equals("max"))
            return max;

        return (double) 0;
    }

    private void accumulate(PrimitiveValue primitiveValue) {
        counter = counter + 1;
        sum = sum + Integer.parseInt(primitiveValue.toRawString());
        if (min > sum)
            min = sum;
        if (max < sum)
            max = sum;
    }

    @Override
    public boolean hasNext() throws Exception {

        if (useGroupByOutput) {
            if (groupByMap.size() > 0)
                return true;
            else
                return false;
        }

        if(getFromAggResults)
            return false;

        return child.hasNext();
    }

    @Override
    public PrimitiveValueWrapper[] next() throws Exception {

        List<SelectItem> origSelectItems = selectItems;

        if (useGroupByOutput) {
            for (Map.Entry<String, List<String>> entry : groupByMap.entrySet()) {
                String pv = entry.getKey();

                for (String aggVal : entry.getValue())
                    pv = pv + aggVal + "|";

                pv = pv.substring(0, pv.length() - 1);

                String key = entry.getKey();

                String[] primitiveValues = pv.split("\\|");

                PrimitiveValueWrapper[] primitiveValueWrappers = new PrimitiveValueWrapper[origSelectItems.size()];

                int primitiveValuesLength = primitiveValues.length;

                if (hasAvg)
                    primitiveValuesLength--;

                for (int i = 0; i < primitiveValuesLength; i++) {
                    if (i == primitiveValuesLength - 1 && hasAvg) {
                        double sum = Double.parseDouble(primitiveValues[i]);
                        double count = Double.parseDouble(primitiveValues[i + 1]);

                        double avg = sum / count;

                        primitiveValueWrappers[i] = new PrimitiveValueWrapper();
                        primitiveValueWrappers[i].setPrimitiveValue(new DoubleValue(avg));
                    }else {
                        primitiveValueWrappers[i] = new PrimitiveValueWrapper();
                        primitiveValueWrappers[i].setPrimitiveValue(new StringValue(primitiveValues[i]));
                    }
                }

                groupByMap.remove(key);

                return primitiveValueWrappers;
            }
        }

        SelectExpressionItem selectExpressionItem;
        AllTableColumns allTableColumns;
        AllColumns allColumns;
        Column column;

        PrimitiveValueWrapper[] tuple = child.next();
        ArrayList<PrimitiveValueWrapper> projectedTuple = new ArrayList();

        if (tuple == null)
            return null;

        if (isAggregateQuery(selectItems)) {

            selectItems = getUnpackedSelectedItems(selectItems);

            //Process Group by Clause
            if (groupByColumnReferences != null) {

               // selectItems = addGroupByColsToSelectItem(groupByColumnReferences, selectItems);

                List<String> aggTypeOfSelectItems = getAggTypeOfSelectItems(origSelectItems);

                while (child.hasNext()) {

                    if (tuple == null) {
                        tuple = child.next();
                        if (tuple == null)
                            continue;
                    }

                    for (int index = 0; index < selectItems.size(); index++) {
                        if ((selectExpressionItem = (SelectExpressionItem) castAs(selectItems.get(index), SelectExpressionItem.class)) != null) {
                            PrimitiveValueWrapper evaluatedExpression = commonLib.eval(selectExpressionItem.getExpression(), tuple);

                            ColumnDefinition evaluatedColumnDefinition = new ColumnDefinition();
                            evaluatedColumnDefinition.setColumnName(tuple[index].getColumnDefinition().getColumnName()); // "TODO : Code will fail if number of select items are more than number of columns in a table
                            evaluatedColumnDefinition.setColDataType(tuple[index].getColumnDefinition().getColDataType());
                            evaluatedColumnDefinition.setColumnSpecStrings(tuple[index].getColumnDefinition().getColumnSpecStrings());
                          /*  for (PrimitiveValueWrapper selectedValue : projectedTuple) { // TODO: Commented to avoid failures in group by. Need to revalidate the logic here.
                                if (selectExpressionItem.getAlias() != null) {
                                    if (selectedValue.getColumnDefinition().getColumnName().equals(selectExpressionItem.getAlias()))
                                        throw new Exception(selectExpressionItem.getAlias() + " is an ambiguous column name.");
                                } else if ((column = (Column) castAs(selectExpressionItem, Column.class)) != null)
                                    if (selectedValue.getColumnDefinition().getColumnName().equals(column.getColumnName()))
                                        throw new Exception(column.getColumnName() + " was specified multiple times.");

                            }*/
                            evaluatedColumnDefinition.setColumnName(selectExpressionItem.getAlias());
                            evaluatedExpression.setColumnDefinition(evaluatedColumnDefinition);

                            if (tableAlias != null)
                                evaluatedExpression.setTableName(tableAlias);

                            projectedTuple.add(evaluatedExpression);

                        } else if ((allTableColumns = (AllTableColumns) castAs(selectItems.get(index), AllTableColumns.class)) != null) {
                            for (int secondIndex = 0; secondIndex < tuple.length; secondIndex++) {
                                if (tuple[secondIndex].getColumnDefinition().getColumnName() == null)
                                    throw new Exception("No column name specified for column at index " + index + " for " + tableAlias);
                                else if (tuple[secondIndex].getTableName().equals(allTableColumns.getTable().getName())) {
                                    projectedTuple.add(tuple[secondIndex]);
                                }
                            }

                        } else if ((allColumns = (AllColumns) castAs(selectItems.get(index), AllColumns.class)) != null) {
                            for (int secondIndex = 0; secondIndex < tuple.length; secondIndex++) {
                                if (tuple[secondIndex].getColumnDefinition().getColumnName() == null)
                                    throw new Exception("No column name specified for column at index " + index + " for " + tableAlias);
                            }
                            return tuple;

                        }
                    }


                    // Group By logic
                    String groupByCols = "";
                    for (int index = 0; index < groupByColumnReferences.size(); index++)
                        groupByCols = groupByCols + projectedTuple.get(index).getPrimitiveValue().toRawString() + "|";


                    List<String> aggPrimitiveValues = new ArrayList<String>();

                    for (int index = groupByColumnReferences.size(); index < projectedTuple.size(); index++)
                        aggPrimitiveValues.add(projectedTuple.get(index).getPrimitiveValue().toRawString());


                    groupByAccumulator(aggPrimitiveValues, aggTypeOfSelectItems, groupByCols);

                    tuple = null; // current tuple has been processed.
                    projectedTuple = new ArrayList();

                }


                for (Map.Entry<String, List<String>> entry : groupByMap.entrySet()) {
                    String pv = entry.getKey();

                    for (String aggVal : entry.getValue())
                        pv = pv + aggVal + "|";

                    pv = pv.substring(0, pv.length() - 1);

                    String key = entry.getKey();

                    String[] primitiveValues = pv.split("\\|");

                    PrimitiveValueWrapper[] primitiveValueWrappers = new PrimitiveValueWrapper[origSelectItems.size()];

                    int primitiveValuesLength = primitiveValues.length;

                    if (aggTypeOfSelectItems.contains("avg"))
                        primitiveValuesLength--;

                    for (int i = 0; i < primitiveValuesLength; i++) {
                        if (i == primitiveValuesLength - 1 && aggTypeOfSelectItems.contains("avg")) {
                            double sum = Double.parseDouble(primitiveValues[i]);
                            double count = Double.parseDouble(primitiveValues[i + 1]);

                            double avg = sum / count;

                            primitiveValueWrappers[i] = new PrimitiveValueWrapper();
                            primitiveValueWrappers[i].setPrimitiveValue(new DoubleValue(avg));
                        }else {
                            primitiveValueWrappers[i] = new PrimitiveValueWrapper();
                            primitiveValueWrappers[i].setPrimitiveValue(new StringValue(primitiveValues[i]));
                        }
                    }

                    groupByMap.remove(key);
                    useGroupByOutput = true;
                    return primitiveValueWrappers;
                }


            }

            // Process Only Aggregate Queries, "excluding group by"
            else {

                selectItems = getUnpackedSelectedItems(selectItems);

                List<String> aggTypeOfSelectItems = getAggType(origSelectItems);

                while (child.hasNext()) {

                    if (tuple == null) {
                        tuple = child.next();
                        if (tuple == null)
                            continue;
                    }

                    for (int index = 0; index < selectItems.size(); index++) {
                        if ((selectExpressionItem = (SelectExpressionItem) castAs(selectItems.get(index), SelectExpressionItem.class)) != null) {
                            PrimitiveValueWrapper evaluatedExpression = commonLib.eval(selectExpressionItem.getExpression(), tuple);

                            ColumnDefinition evaluatedColumnDefinition = new ColumnDefinition();
                            evaluatedColumnDefinition.setColumnName(tuple[index].getColumnDefinition().getColumnName());
                            evaluatedColumnDefinition.setColDataType(tuple[index].getColumnDefinition().getColDataType());
                            evaluatedColumnDefinition.setColumnSpecStrings(tuple[index].getColumnDefinition().getColumnSpecStrings());
                            for (PrimitiveValueWrapper selectedValue : projectedTuple) {
                                if (selectExpressionItem.getAlias() != null) {
                                    if (selectedValue.getColumnDefinition().getColumnName().equals(selectExpressionItem.getAlias()))
                                        throw new Exception(selectExpressionItem.getAlias() + " is an ambiguous column name.");
                                } else if ((column = (Column) castAs(selectExpressionItem, Column.class)) != null)
                                    if (selectedValue.getColumnDefinition().getColumnName().equals(column.getColumnName()))
                                        throw new Exception(column.getColumnName() + " was specified multiple times.");

                            }
                            evaluatedColumnDefinition.setColumnName(selectExpressionItem.getAlias());
                            evaluatedExpression.setColumnDefinition(evaluatedColumnDefinition);

                            if (tableAlias != null)
                                evaluatedExpression.setTableName(tableAlias);

                            projectedTuple.add(evaluatedExpression);

                        } else if ((allTableColumns = (AllTableColumns) castAs(selectItems.get(index), AllTableColumns.class)) != null) {
                            for (int secondIndex = 0; secondIndex < tuple.length; secondIndex++) {
                                if (tuple[secondIndex].getColumnDefinition().getColumnName() == null)
                                    throw new Exception("No column name specified for column at index " + index + " for " + tableAlias);
                                else if (tuple[secondIndex].getTableName().equals(allTableColumns.getTable().getName())) {
                                    projectedTuple.add(tuple[secondIndex]);
                                }
                            }

                        } else if ((allColumns = (AllColumns) castAs(selectItems.get(index), AllColumns.class)) != null) {
                            for (int secondIndex = 0; secondIndex < tuple.length; secondIndex++) {
                                if (tuple[secondIndex].getColumnDefinition().getColumnName() == null)
                                    throw new Exception("No column name specified for column at index " + index + " for " + tableAlias);
                            }
                            return tuple;

                        }
                    }

                    List<String> aggPrimitiveValues = new ArrayList<String>();

                    aggAccumulator(projectedTuple, aggTypeOfSelectItems);


                    getFromAggResults = true;


                    tuple = null; // current tuple has been processed.
                    projectedTuple = new ArrayList();

                }


                if(aggValues.size() > 0) {
                    String aggResult[] = aggValues.get(0).split("\\|");

                    PrimitiveValueWrapper primitiveValueWrapper[] = new PrimitiveValueWrapper[aggResult.length];

                    for(int index = 0; index < aggResult.length; index++){
                        if(aggTypeOfSelectItems.get(index).equals("avg")){
                            String avgResult[] = aggResult[index].split(" ");
                            double sum = Double.parseDouble(avgResult[0]);
                            double cnt = Double.parseDouble(avgResult[1]);
                            double avg = sum / cnt;
                            primitiveValueWrapper[index] = new PrimitiveValueWrapper();
                            primitiveValueWrapper[index].setPrimitiveValue(new DoubleValue(avg));
                        }else{
                            primitiveValueWrapper[index] = new PrimitiveValueWrapper();
                            primitiveValueWrapper[index].setPrimitiveValue(new StringValue(aggResult[index]));
                        }
                    }
                    return primitiveValueWrapper;
                } else{
                    return null;
                }


            }

        } else { // Process Non-Aggregate Queries

            selectItems = origSelectItems;

            for (int index = 0; index < selectItems.size(); index++) {
                if ((selectExpressionItem = (SelectExpressionItem) castAs(selectItems.get(index), SelectExpressionItem.class)) != null) {
                    PrimitiveValueWrapper evaluatedExpression = commonLib.eval(selectExpressionItem.getExpression(), tuple);

                    ColumnDefinition evaluatedColumnDefinition = new ColumnDefinition();
                    evaluatedColumnDefinition.setColumnName(tuple[index].getColumnDefinition().getColumnName());
                    evaluatedColumnDefinition.setColDataType(tuple[index].getColumnDefinition().getColDataType());
                    evaluatedColumnDefinition.setColumnSpecStrings(tuple[index].getColumnDefinition().getColumnSpecStrings());
                    for (PrimitiveValueWrapper selectedValue : projectedTuple) {
                        if (selectExpressionItem.getAlias() != null) {
                            if (selectedValue.getColumnDefinition().getColumnName().equals(selectExpressionItem.getAlias()))
                                throw new Exception(selectExpressionItem.getAlias() + " is an ambiguous column name.");
                        } else if ((column = (Column) castAs(selectExpressionItem, Column.class)) != null)
                            if (selectedValue.getColumnDefinition().getColumnName().equals(column.getColumnName()))
                                throw new Exception(column.getColumnName() + " was specified multiple times.");

                    }
                    evaluatedColumnDefinition.setColumnName(selectExpressionItem.getAlias());
                    evaluatedExpression.setColumnDefinition(evaluatedColumnDefinition);

                    if (tableAlias != null)
                        evaluatedExpression.setTableName(tableAlias);

                    projectedTuple.add(evaluatedExpression);

                } else if ((allTableColumns = (AllTableColumns) castAs(selectItems.get(index), AllTableColumns.class)) != null) {
                    for (int secondIndex = 0; secondIndex < tuple.length; secondIndex++) {
                        if (tuple[secondIndex].getColumnDefinition().getColumnName() == null)
                            throw new Exception("No column name specified for column at index " + index + " for " + tableAlias);
                        else if (tuple[secondIndex].getTableName().equals(allTableColumns.getTable().getName())) {
                            projectedTuple.add(tuple[secondIndex]);
                        }
                    }

                } else if ((allColumns = (AllColumns) castAs(selectItems.get(index), AllColumns.class)) != null) {
                    for (int secondIndex = 0; secondIndex < tuple.length; secondIndex++) {
                        if (tuple[secondIndex].getColumnDefinition().getColumnName() == null)
                            throw new Exception("No column name specified for column at index " + index + " for " + tableAlias);
                    }
                    return tuple;

                }
            }
        }
        return projectedTuple.toArray(new PrimitiveValueWrapper[projectedTuple.size()]);

    }

    private void aggAccumulator(ArrayList<PrimitiveValueWrapper> projectedTuple, List<String> aggTypeOfSelectItems) {

        int index = 0;
        String newValues = "";
        for(PrimitiveValueWrapper primitiveValueWrapper : projectedTuple){
            String pv = primitiveValueWrapper.getPrimitiveValue().toRawString();

            if(aggValues.size() > 0) {
                String oldValues[] = aggValues.get(0).split("\\|");

                if (aggTypeOfSelectItems.get(index).equals("count")) {
                    int cnt = Integer.parseInt(oldValues[index]) + 1;
                    newValues = newValues + "|" + cnt;
                } else if (aggTypeOfSelectItems.get(index).equals("sum")) {
                    int sum = Integer.parseInt(oldValues[index]) + Integer.parseInt(pv);
                    newValues = newValues + "|" + sum;
                } else if (aggTypeOfSelectItems.get(index).equals("min")) {
                    int min = Math.min(Integer.parseInt(oldValues[index]), Integer.parseInt(pv));
                    newValues = newValues + "|" + min;
                } else if (aggTypeOfSelectItems.get(index).equals("max")) {
                    int max = Math.max(Integer.parseInt(oldValues[index]), Integer.parseInt(pv));
                    newValues =  newValues + "|" + max;
                } else if (aggTypeOfSelectItems.get(index).equals("avg")) {
                    String avgVal[] = oldValues[index].split(" ");
                    int cnt = Integer.parseInt(avgVal[1]) + 1;
                    int sum = Integer.parseInt(avgVal[0]) + Integer.parseInt(pv);
                    newValues =  newValues + "|" + sum + " " + cnt; // Using "/" as separator for avg
                }
            }else{
                if (aggTypeOfSelectItems.get(index).equals("avg")) {
                    newValues = newValues + "|" + pv + " " + "1";
                } else{
                    newValues =  newValues + "|" + pv;
                }
            }

            index++;
        }
        newValues = newValues.substring(1); // Removing last "|"

        aggValues.clear();
        aggValues.add(newValues);
    }

    private void groupByAccumulator(List<String> aggPrimitiveValues, List<String> aggType, String groupByCols) {

        List<String> currentValues;
        List<String> newValues = new ArrayList<String>();
        if (groupByMap.containsKey(groupByCols)) {
            currentValues = groupByMap.get(groupByCols);

            String values = "";
            for (int index = 0; index < aggType.size(); index++) {

                String temp = "";
                if (aggType.get(index).equals("count")) {
                    temp = Integer.parseInt(currentValues.get(index)) + 1 + ""; //aggPrimitiveValues.get(index);
                } else if (aggType.get(index).equals("sum")) {
                    temp = Integer.parseInt(currentValues.get(index)) + Integer.parseInt(aggPrimitiveValues.get(index)) + "";
                } else if (aggType.get(index).equals("min")) {
                    temp = Math.min(Integer.parseInt(currentValues.get(index)), Integer.parseInt(aggPrimitiveValues.get(index))) + "";
                } else if (aggType.get(index).equals("max")) {
                    temp = Math.max(Integer.parseInt(currentValues.get(index)), Integer.parseInt(aggPrimitiveValues.get(index))) + "";
                } else if (aggType.get(index).equals("avg")) {
                    hasAvg = true;
                    String[] tmp = currentValues.get(index).split("\\|");
                    int count = Integer.parseInt(tmp[1]) + 1;
                    int sum = Integer.parseInt(tmp[0]) + Integer.parseInt(aggPrimitiveValues.get(index));
                    temp = sum + "|" + count;
                }

                newValues.add(temp);
            }
            groupByMap.put(groupByCols, newValues);
        } else {
            for (int index = 0; index < aggType.size(); index++) {
                String temp = "";
                if (aggType.get(index).equals("count")) {
                    temp = 1 + ""; //aggPrimitiveValues.get(index);
                } else if (aggType.get(index).equals("sum")) {
                    temp = aggPrimitiveValues.get(index);
                } else if (aggType.get(index).equals("min")) {
                    temp = aggPrimitiveValues.get(index);
                } else if (aggType.get(index).equals("max")) {
                    temp = aggPrimitiveValues.get(index);
                } else if (aggType.get(index).equals("avg")) {
                    hasAvg = true;
                    int count = 1;
                    String sum = aggPrimitiveValues.get(index);
                    temp = sum + "|" + count;
                }

                newValues.add(temp);
            }
            groupByMap.put(groupByCols, newValues);
        }
    }

    private List<String> getAggTypeOfSelectItems(List<SelectItem> selectItems) {
        List<String> list = new ArrayList<String>();

        for (int index = groupByColumnReferences.size(); index < selectItems.size(); index++) {

            Function function = null;
            if ((function = (Function) CommonLib.castAs(((SelectExpressionItem) selectItems.get(index)).getExpression(), Function.class)) != null)
                list.add(function.getName());
        }

        return list;
    }


    private List<String> getAggType(List<SelectItem> selectItems) {
        List<String> list = new ArrayList<String>();

        for (int index = 0; index < selectItems.size(); index++) {
            Function function = null;
            if ((function = (Function) CommonLib.castAs(((SelectExpressionItem) selectItems.get(index)).getExpression(), Function.class)) != null)
                list.add(function.getName());
        }
        return list;
    }

    private List<SelectItem> addGroupByColsToSelectItem(List<Column> groupByColumnReferences, List<SelectItem> selectItems) {
        List<SelectItem> list = new ArrayList<SelectItem>();
        Set<String> groupByItemsSet = new HashSet<String>();


        for (Column column : groupByColumnReferences) {
            SelectExpressionItem selectExpressionItem = new SelectExpressionItem();
            selectExpressionItem.setExpression(column);
            list.add((SelectItem) selectExpressionItem);
            groupByItemsSet.add(column.getColumnName());
        }

        for (SelectItem selectItem : selectItems) {
            SelectExpressionItem sei = new SelectExpressionItem();
            if ((sei = (SelectExpressionItem) commonLib.castAs(selectItem, SelectExpressionItem.class)) != null) {
            }

            if (!groupByItemsSet.contains(sei.getExpression().toString()))
                list.add(selectItem);
        }
        return list;
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

        //SelectItem listSelectItems = selectItems;

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
                            aggColMap.put(selectExpressionItem.getExpression().toString(), (((Function) selectExpressionItem.getExpression()).getName()));
                        } else {
                            temp.setAlias(selectExpressionItem.getAlias());
                            aggColMap.put(selectExpressionItem.getAlias(), (((Function) selectExpressionItem.getExpression()).getName()));
                        }
                    } else if (function.isAllColumns()) {
                        if (function.getParameters() != null && selectExpressionItem.getAlias() != null) {
                            temp.setExpression(function.getParameters().getExpressions().get(0));
                            temp.setAlias(selectExpressionItem.getAlias());
                        } else {
                            LongValue expression = new LongValue(1);
                            temp.setExpression(expression); // TODO : How to pass Count(*) expression during unpacking?
                            temp.setAlias(selectExpressionItem.getExpression().toString());
                            aggColMap.put(temp.getAlias(), "count");
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

    @Override
    public void reset() throws Exception {
        child.reset();
    }

    //endregion
}
