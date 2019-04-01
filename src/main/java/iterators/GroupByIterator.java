package iterators;

import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import helpers.Schema;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.*;

import static helpers.CommonLib.castAs;

public class GroupByIterator implements RAIterator {
    //region Variables

    boolean useGroupByOutput = false;
    boolean getFromAggResults = false;
    boolean hasAvg = false;
    List<Integer> indexOfGroupByCols;
    List<Integer> indexOfNonGroupByCols;
    List<String> aggTypeOfSelectItems;
    private CommonLib commonLib = CommonLib.getInstance();
    private RAIterator child;
    private List<SelectItem> selectItems;
    private String tableAlias;
    private List<Column> groupByColumnReferences;
    private Map<String, String> aggColMap = new HashMap();
    private List<String> aggValues = new ArrayList<String>();
    private Map<String, List<PrimitiveValue>> groupByMap = new HashMap<String, List<PrimitiveValue>>();
    private Schema[] schema;
    private CommonLib commonLibInstance = CommonLib.getInstance();
    private ColumnDefinition[] keyColDef;
    //endregion

    //region Constructor

    public GroupByIterator(RAIterator child, List<SelectItem> selectItems, String tableAlias, List<Column> groupByColumnReferences) {

        this.child = child;
        this.selectItems = selectItems;
        this.tableAlias = tableAlias;
        this.groupByColumnReferences = groupByColumnReferences;
        this.schema = child.getSchema();


        indexOfGroupByCols = getIndexOfGroupByCols(selectItems, "groupby");

        indexOfNonGroupByCols = getIndexOfGroupByCols(selectItems, "nongroupby");

        aggTypeOfSelectItems = getAggTypeOfSelectItems(selectItems);

        keyColDef = getKeyColDef(indexOfNonGroupByCols);

    }

    private ColumnDefinition[] getKeyColDef(List<Integer> indexOfGroupByCols) {
        ColumnDefinition[] columnDefinitions = new ColumnDefinition[indexOfGroupByCols.size()];

        for (int index = 0; index < indexOfGroupByCols.size(); index++) {
//            for (int i = 0; i < schema.length; i++) {
//                if(groupByColumnReferences.get(i).getColumnName().equals(schema[i].getColumnDefinition().getColumnName())){
            columnDefinitions[index] = new ColumnDefinition();
            columnDefinitions[index].setColDataType(schema[indexOfGroupByCols.get(index)].getColumnDefinition().getColDataType());
        }


        return columnDefinitions;
    }


    private List<Integer> getIndexOfGroupByCols(List<SelectItem> selectItems, String groupby) {

        List<Integer> list = new ArrayList<Integer>();
        Function function = null;
        if (groupby.equals("groupby")) {

            for (int i = 0; i < selectItems.size(); i++) {
                if ((function = (Function) CommonLib.castAs(((SelectExpressionItem) selectItems.get(i)).getExpression(), Function.class)) != null) {
                    list.add(i);
                }
            }


        } else {
            for (int i = 0; i < selectItems.size(); i++) {
                if ((function = (Function) CommonLib.castAs(((SelectExpressionItem) selectItems.get(i)).getExpression(), Function.class)) == null) {
                    list.add(i);
                }
            }
        }
        return list;
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
            for (Map.Entry<String, List<PrimitiveValue>> entry : groupByMap.entrySet()) {
                String pv = entry.getKey();

                PrimitiveValue key[] = commonLib.covertTupleToPrimitiveValue(pv.substring(0, pv.length()-1), keyColDef);
                PrimitiveValue[] value = new PrimitiveValue[entry.getValue().size()] ;

                for(int i = 0 ; i < value.length ; i++){
                    value[i] = entry.getValue().get(i) ;
                }

                PrimitiveValue[] combineTuple = createTupleFromKeyVal(key, value) ;


                groupByMap.remove(pv);
                useGroupByOutput = true;
                return combineTuple;
            }
        }

        PrimitiveValue[] tuple = child.next();

        if (tuple == null)
            return null;

        if (isAggregateQuery(selectItems)) {

            selectItems = getUnpackedSelectedItems(selectItems);

            // selectItems = addGroupByColsToSelectItem(groupByColumnReferences, selectItems);


            while (child.hasNext()) {

                if (tuple == null) {
                    tuple = child.next();
                    if (tuple == null)
                        continue;
                }

                List<PrimitiveValue> projectedTuple = Arrays.asList(tuple);

                String groupByCols = "";
                for (Integer index : indexOfNonGroupByCols)
                    groupByCols = groupByCols + projectedTuple.get(index).toRawString() + "|";


                List<String> aggPrimitiveValues = new ArrayList<String>();

                for (Integer index : indexOfGroupByCols)
                    aggPrimitiveValues.add(projectedTuple.get(index).toRawString());

                groupByAccumulator(tuple, aggTypeOfSelectItems, indexOfNonGroupByCols, groupByCols);

                tuple = null; // current tuple has been processed.
            }

            // Emits a row from Group By Map

            for (Map.Entry<String, List<PrimitiveValue>> entry : groupByMap.entrySet()) {
                String pv = entry.getKey();

                PrimitiveValue key[] = commonLib.covertTupleToPrimitiveValue(pv.substring(0, pv.length()-1), keyColDef);
                PrimitiveValue[] value = new PrimitiveValue[entry.getValue().size()] ;
                for(int i = 0 ; i < value.length ; i++){
                    value[i] = entry.getValue().get(i) ;
                }

                PrimitiveValue[] combineTuple = createTupleFromKeyVal(key, value) ;


                groupByMap.remove(pv);
                useGroupByOutput = true;
                return combineTuple;
            }

        } else { // Process group by without Aggregates

        }

        return null; // should not come here
        //return projectedTuple.toArray(new PrimitiveValueWrapper[projectedTuple.size()]);

    }

    private PrimitiveValue[] createTupleFromKeyVal(PrimitiveValue[] key, PrimitiveValue[] value) throws PrimitiveValue.InvalidPrimitive {
        PrimitiveValue[] newTuple = new PrimitiveValue[schema.length] ;

        for(int i = 0 ; i < key.length ; i++){
            newTuple[indexOfNonGroupByCols.get(i)] = key[i] ;
        }
        int k = 0 ;
        for(int j = 0; j < aggTypeOfSelectItems.size() ; j++){
            if(aggTypeOfSelectItems.get(j).toLowerCase().equals("avg")){
                double count = value[k].toDouble() ;
                k++ ;
                double sum = value[k].toDouble() ;
                double avg = sum/count ;
                newTuple[indexOfGroupByCols.get(j)] = new DoubleValue(avg);
            }
            else{
                newTuple[indexOfGroupByCols.get(j)] = value[k] ;
            }
            k++ ;
        }
        return newTuple ;
    }

    private void groupByAccumulator(PrimitiveValue[] tuple, List<String> aggType, List<Integer> indexOfNonGroupByCols, String groupByCols) throws PrimitiveValue.InvalidPrimitive {

        List<PrimitiveValue> currentValues;
        List<PrimitiveValue> newValues = new ArrayList<PrimitiveValue>();

        if (groupByMap.containsKey(groupByCols)) {
            currentValues = groupByMap.get(groupByCols);

            String values = "";
            int j = 0 ;
            for (int index = 0; index < currentValues.size(); index++) {

                if (aggType.get(j).toLowerCase().equals("count")) {
                    newValues.add(new LongValue(currentValues.get(index).toLong() + 1));
                } else if (aggType.get(j).toLowerCase().equals("sum")) {
                    newValues.add(commonLib.PrimitiveValueComparator(currentValues.get(index), tuple[indexOfGroupByCols.get(j)], "sum"));
                } else if (aggType.get(j).toLowerCase().equals("min")) {
                    newValues.add(commonLib.PrimitiveValueComparator(currentValues.get(index), tuple[indexOfGroupByCols.get(j)], "min"));
                } else if (aggType.get(j).toLowerCase().equals("max")) {
                    newValues.add(commonLib.PrimitiveValueComparator(currentValues.get(index), tuple[indexOfGroupByCols.get(j)], "max"));
                } else if (aggType.get(j).toLowerCase().equals("avg")) {
                    hasAvg = true;
                    PrimitiveValue count = new DoubleValue(currentValues.get(index).toDouble() + 1);
                    newValues.add(count);
                    index++;
                    PrimitiveValue sum = new DoubleValue(currentValues.get(index).toDouble() + tuple[indexOfGroupByCols.get(j)].toDouble());
                    newValues.add(sum);
                }
                j++ ;
            }
            groupByMap.put(groupByCols, newValues);
        } else {
            for (int index = 0; index < aggType.size(); index++) {
                PrimitiveValue temp;
                if (aggType.get(index).toLowerCase().equals("count")) {
                    temp = new LongValue(1);
                    newValues.add(temp);
                } else if (aggType.get(index).toLowerCase().equals("sum")) {
                    temp = tuple[indexOfGroupByCols.get(index)];
                    newValues.add(temp);
                } else if (aggType.get(index).toLowerCase().equals("min")) {
                    temp = tuple[indexOfGroupByCols.get(index)];
                    newValues.add(temp);
                } else if (aggType.get(index).toLowerCase().equals("max")) {
                    temp = tuple[indexOfGroupByCols.get(index)];
                    newValues.add(temp);
                } else if (aggType.get(index).toLowerCase().equals("avg")) {
                    hasAvg = true;
                    PrimitiveValue count = new DoubleValue(1);
                    PrimitiveValue sum = tuple[indexOfGroupByCols.get(index)];
                    newValues.add(count);
                    newValues.add(sum);
                }
            }
            groupByMap.put(groupByCols, newValues);
        }
    }

    private List<String> getAggTypeOfSelectItems(List<SelectItem> selectItems) {
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
                            temp.setExpression(expression);
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
        this.child = child;
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
        RAIterator child = iterator.getChild();
        child = child.optimize(child);
        iterator.setChild(child);
        return iterator;
    }

    //endregion
}