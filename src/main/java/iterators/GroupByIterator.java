package iterators;

import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import helpers.Schema;
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

public class GroupByIterator implements RAIterator {
    //region Variables

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
    private Schema[] schema;

    private CommonLib commonLibInstance = CommonLib.getInstance();
    //endregion

    //region Constructor

    public GroupByIterator(RAIterator child, List<SelectItem> selectItems, String tableAlias, List<Column> groupByColumnReferences) {

        this.child = child;
        this.selectItems = selectItems;
        this.tableAlias = tableAlias;
        this.groupByColumnReferences = groupByColumnReferences;
        this.schema = child.getSchema();
    }

    //endregion

    //region Iterator methods


    @Override
    public boolean hasNext() throws Exception {

        if (useGroupByOutput) {
            if (groupByMap.size() > 0)
                return true;
            else
                return false;
        }

        return child.hasNext();
    }

    @Override
    public PrimitiveValue[] next() throws Exception {

        List<SelectItem> origSelectItems = selectItems;

        if (useGroupByOutput) {
            for (Map.Entry<String, List<String>> entry : groupByMap.entrySet()) {
                String pv = entry.getKey();

                for (String aggVal : entry.getValue())
                    pv = pv + aggVal + "|";

                pv = pv.substring(0, pv.length() - 1);

                String key = entry.getKey();

                String[] primitiveValues = pv.split("\\|");

                PrimitiveValue[] primitiveValueWrappers = new PrimitiveValue[origSelectItems.size()];

                int primitiveValuesLength = primitiveValues.length;

                if (hasAvg)
                    primitiveValuesLength--;

                for (int i = 0; i < primitiveValuesLength; i++) {
                    if (i == primitiveValuesLength - 1 && hasAvg) {
                        double sum = Double.parseDouble(primitiveValues[i]);
                        double count = Double.parseDouble(primitiveValues[i + 1]);

                        double avg = sum / count;

                        primitiveValueWrappers[i] = commonLibInstance.convertToPrimitiveValue(avg+"", "DECIMAL");

                    } else {
                        primitiveValueWrappers[i] = commonLibInstance.convertToPrimitiveValue(primitiveValues[i], "DECIMAL");
                    }
                }

                groupByMap.remove(key);

                return primitiveValueWrappers;
            }
        }

        PrimitiveValue[] tuple = child.next();

        if (tuple == null)
            return null;

        if (isAggregateQuery(selectItems)) {

            selectItems = getUnpackedSelectedItems(selectItems);

            // selectItems = addGroupByColsToSelectItem(groupByColumnReferences, selectItems);

            List<String> aggTypeOfSelectItems = getAggTypeOfSelectItems(origSelectItems);

            while (child.hasNext()) {

                if (tuple == null) {
                    tuple = child.next();
                    if (tuple == null)
                        continue;
                }

                List<PrimitiveValue> projectedTuple = Arrays.asList(tuple);

                String groupByCols = "";
                for (int index = 0; index < groupByColumnReferences.size(); index++)
                    groupByCols = groupByCols + projectedTuple.get(index).toRawString() + "|";

                List<String> aggPrimitiveValues = new ArrayList<String>();

                for (int index = groupByColumnReferences.size(); index < projectedTuple.size(); index++)
                    aggPrimitiveValues.add(projectedTuple.get(index).toRawString());


                groupByAccumulator(aggPrimitiveValues, aggTypeOfSelectItems, groupByCols);

                tuple = null; // current tuple has been processed.
            }

            // Emits a row from Group By Map

            for (Map.Entry<String, List<String>> entry : groupByMap.entrySet()) {
                String pv = entry.getKey();

                for (String aggVal : entry.getValue())
                    pv = pv + aggVal + "|";

                pv = pv.substring(0, pv.length() - 1);

                String key = entry.getKey();

                String[] primitiveValues = pv.split("\\|");

                PrimitiveValue[] primitiveValueWrappers = new PrimitiveValue[origSelectItems.size()];

                int primitiveValuesLength = primitiveValues.length;

                if (aggTypeOfSelectItems.contains("avg"))
                    primitiveValuesLength--;

                for (int i = 0; i < primitiveValuesLength; i++) {
                    if (i == primitiveValuesLength - 1 && aggTypeOfSelectItems.contains("avg")) { // TODO: Checks AVG only at last index in SelectItems
                        double sum = Double.parseDouble(primitiveValues[i]);
                        double count = Double.parseDouble(primitiveValues[i + 1]);

                        double avg = sum / count;

                        primitiveValueWrappers[i] = commonLibInstance.convertToPrimitiveValue(avg+"", "DECIMAL");
                    } else {

                        primitiveValueWrappers[i] =
                                commonLibInstance.convertToPrimitiveValue(primitiveValues[i], "DECIMAL");
                    }
                }

                groupByMap.remove(key);
                useGroupByOutput = true;
                return primitiveValueWrappers;
            }

        } else { // Process group by without Aggregates

        }

        return null; // should not come here
        //return projectedTuple.toArray(new PrimitiveValueWrapper[projectedTuple.size()]);

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
                    temp = Double.parseDouble(currentValues.get(index)) + 1 + ""; //aggPrimitiveValues.get(index);
                } else if (aggType.get(index).equals("sum")) {
                    temp = Double.parseDouble(currentValues.get(index)) + Double.parseDouble(aggPrimitiveValues.get(index)) + "";
                } else if (aggType.get(index).equals("min")) {
                    temp = Math.min(Double.parseDouble(currentValues.get(index)), Double.parseDouble(aggPrimitiveValues.get(index))) + "";
                } else if (aggType.get(index).equals("max")) {
                    temp = Math.max(Double.parseDouble(currentValues.get(index)), Double.parseDouble(aggPrimitiveValues.get(index))) + "";
                } else if (aggType.get(index).equals("avg")) {
                    hasAvg = true;
                    String[] tmp = currentValues.get(index).split("\\|");
                    int count = Integer.parseInt(tmp[1]) + 1;
                    double sum = Double.parseDouble(tmp[0]) + Double.parseDouble(aggPrimitiveValues.get(index));
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

    @Override
    public RAIterator getChild() {
        return this.child;
    }

    @Override
    public void setChild(RAIterator child) {
        this.child = child ;
    }

    @Override
    public Schema[] getSchema() {
        return this.schema ;
    }

    @Override
    public void setSchema(Schema[] schema) {
        this.schema = schema ;
    }


    //endregion
}
