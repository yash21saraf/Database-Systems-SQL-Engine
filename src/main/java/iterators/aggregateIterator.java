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

import java.lang.reflect.Array;
import java.util.*;

import static helpers.CommonLib.castAs;

public class aggregateIterator implements RAIterator {
    //region Variables

    boolean getFromAggResults = false;

    private CommonLib commonLib = CommonLib.getInstance();
    private RAIterator child;
    private List<SelectItem> selectItems;
    private String tableAlias;
    private List<Column> groupByColumnReferences;
    private Map<String, String> aggColMap = new HashMap();
    private List<String> aggValues = new ArrayList<String>();
    private Schema[] schema;

    //endregion

    //region Constructor

    public aggregateIterator(RAIterator child, List<SelectItem> selectItems, String tableAlias) {

        this.child = child;
        this.selectItems = selectItems;
        this.tableAlias = tableAlias;
        this.schema = child.getSchema();

    }

    //endregion

    //region Iterator methods

    @Override
    public boolean hasNext() throws Exception {

        if (getFromAggResults)
            return false;

        return child.hasNext();
    }

    @Override
    public PrimitiveValue[] next() throws Exception {

        List<SelectItem> origSelectItems = selectItems;
        List<String> aggTypeOfSelectItems = getAggType(origSelectItems);

        PrimitiveValue[] tuple = child.next();

        if (tuple == null)
            return null;

        while (child.hasNext()) {

            if (tuple == null) {
                tuple = child.next();
                if (tuple == null)
                    continue;
            }

            List<PrimitiveValue> projectedTuple = Arrays.asList(tuple);
            aggAccumulator(projectedTuple, aggTypeOfSelectItems);

            getFromAggResults = true;

            tuple = null; // current tuple has been processed.
        }


        if (aggValues.size() > 0) {
            String aggResult[] = aggValues.get(0).split("\\|");

            PrimitiveValue primitiveValueWrapper[] = new PrimitiveValue[aggResult.length];

            for (int index = 0; index < aggResult.length; index++) {
                if (aggTypeOfSelectItems.get(index).equals("avg")) {
                    String avgResult[] = aggResult[index].split(" ");
                    double sum = Double.parseDouble(avgResult[0]);
                    double cnt = Double.parseDouble(avgResult[1]);
                    double avg = sum / cnt;
                    primitiveValueWrapper[index] = new DoubleValue(avg);
                } else {
                    primitiveValueWrapper[index] = new StringValue(aggResult[index]);
                }
            }
            return primitiveValueWrapper;
        } else {
            return null;
        }
    }


    private void aggAccumulator(List<PrimitiveValue> projectedTuple, List<String> aggTypeOfSelectItems) {

        int index = 0;
        String newValues = "";
        for (PrimitiveValue primitiveValueWrapper : projectedTuple) {
            String pv = primitiveValueWrapper.toRawString();

            if (aggValues.size() > 0) {
                String oldValues[] = aggValues.get(0).split("\\|");

                if (aggTypeOfSelectItems.get(index).equals("count")) {
                    int cnt = Integer.parseInt(oldValues[index]) + 1;
                    newValues = newValues + "|" + cnt;
                } else if (aggTypeOfSelectItems.get(index).equals("sum")) {
                    double sum = Double.parseDouble(oldValues[index]) + Double.parseDouble(pv);
                    newValues = newValues + "|" + sum;
                } else if (aggTypeOfSelectItems.get(index).equals("min")) {
                    double min = Math.min(Double.parseDouble(oldValues[index]), Double.parseDouble(pv));
                    newValues = newValues + "|" + min;
                } else if (aggTypeOfSelectItems.get(index).equals("max")) {
                    double max = Math.max(Double.parseDouble(oldValues[index]), Double.parseDouble(pv));
                    newValues = newValues + "|" + max;
                } else if (aggTypeOfSelectItems.get(index).equals("avg")) {
                    String avgVal[] = oldValues[index].split(" ");
                    int cnt = Integer.parseInt(avgVal[1]) + 1;
                    double sum = Double.parseDouble(avgVal[0]) + Double.parseDouble(pv);
                    newValues = newValues + "|" + sum + " " + cnt; // Using "/" as separator for avg
                }
            } else {
                if (aggTypeOfSelectItems.get(index).equals("avg")) {
                    newValues = newValues + "|" + pv + " " + "1";
                } else {
                    newValues = newValues + "|" + pv;
                }
            }

            index++;
        }
        newValues = newValues.substring(1); // Removing last "|"

        aggValues.clear();
        aggValues.add(newValues);
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
