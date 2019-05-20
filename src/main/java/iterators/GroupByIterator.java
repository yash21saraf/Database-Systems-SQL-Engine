package iterators;

import dubstep.Main;
import helpers.CommonLib;
import helpers.Schema;
import helpers.Sort;
import helpers.Tuple;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
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
    List<PrimitiveValue[]> distinctList = new ArrayList<PrimitiveValue[]>();
    Sort sort;
    PrimitiveValue[] aggPrimitiveValues;
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
    private int returnRow = 0;
    private boolean noGroupByElementFound = false;
    private boolean distinct = false;
    private boolean groupByDataSorted = false;
    private boolean noDatafound = false;
    private PrimitiveValue[] currentTuple;
    private PrimitiveValue[] prevTuple;
    private boolean first = true;
    private HashMap<Integer, String> AggTypeToIndexMap = new HashMap<Integer, String>();
    //endregion

    private Tuple tupleClass;

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

        tupleClass = new Tuple(keyColDef, null);

        mapAggtypeToIndex();

    }

    private void mapAggtypeToIndex() {
        Function function;
        for(int i =0; i <selectItems.size(); i++){
            if ((function = (Function) commonLibInstance.castAs(((SelectExpressionItem) selectItems.get(i)).getExpression(), Function.class)) != null) {
                AggTypeToIndexMap.put(i ,function.getName());
            }
        }
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

        if (!Main.inMem) {
            return !noDatafound;

        }

        if (distinct) {
            if (noGroupByElementFound)
                return false;
            else
                return true;
        }

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

        if (!Main.inMem) {

            PrimitiveValue[] key;

            if (sort == null) {
                sort = new Sort(child, getColumnList(groupByColumnReferences), null, null, false);
                sort.sort();
            }

            if (isAggregateQuery(selectItems)) {

                //selectItems = getUnpackedSelectedItems(selectItems);

                // selectItems = addGroupByColsToSelectItem(groupByColumnReferences, selectItems);

                while ((currentTuple = sort.getTuple()) != null && (isSame(currentTuple, prevTuple) || first)) {

                    if (first)
                        first = false;

                    aggPrimitiveValues = groupByAccumulator(currentTuple, aggPrimitiveValues);
                    prevTuple = currentTuple;
                }

                PrimitiveValue[] returnTuple = aggPrimitiveValues;

                if(currentTuple == null){
                    noDatafound = true;
                    if (hasAvg)
                        return convertAndReturn(aggPrimitiveValues);
                    else
                        return aggPrimitiveValues;
                }

                aggPrimitiveValues = groupByAccumulator(currentTuple, null);
                prevTuple = currentTuple;
                if (hasAvg)
                    return convertAndReturn(returnTuple);
                else
                    return returnTuple;

            } else { // Process group by without Aggregates

                distinct = true;

                while ((currentTuple = sort.getTuple()) != null && (isSame(currentTuple, prevTuple) || first)) {
                    if(first) {
                        first = false;
                    }
                    prevTuple = currentTuple;
                    continue;
                }
                PrimitiveValue[] returnTuple = prevTuple;

                if(currentTuple == null) {
                    noDatafound = true;
                    return prevTuple;
                }

                prevTuple = currentTuple;
                return returnTuple;
            }

        }

        List<SelectItem> origSelectItems = selectItems;

        if (distinct) {
            if (returnRow < distinctList.size())
                return distinctList.get(returnRow++);
            else {
                noGroupByElementFound = true;
                return null;
            }
        }

        if (useGroupByOutput) {
            for (Map.Entry<String, List<PrimitiveValue>> entry : groupByMap.entrySet()) {
                String pv = entry.getKey();

                PrimitiveValue key[] = tupleClass.covertTupleToPrimitiveValue(pv.substring(0, pv.length() - 1));

                PrimitiveValue[] value = new PrimitiveValue[entry.getValue().size()];

                for (int i = 0; i < value.length; i++) {
                    value[i] = entry.getValue().get(i);
                }

                PrimitiveValue[] combineTuple = createTupleFromKeyVal(key, value);

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

                StringBuilder groupByCols = new StringBuilder();

                for (Integer index : indexOfNonGroupByCols) {
                    groupByCols.append(tuple[index].toRawString());
                    groupByCols.append("|");
                }

                groupByAccumulator(tuple, aggTypeOfSelectItems, indexOfNonGroupByCols, groupByCols.toString());

                tuple = null; // current tuple has been processed.
            }

            // Emits a row from Group By Map

            for (Map.Entry<String, List<PrimitiveValue>> entry : groupByMap.entrySet()) {
                String pv = entry.getKey();

                PrimitiveValue key[] = tupleClass.covertTupleToPrimitiveValue(pv.substring(0, pv.length() - 1));
                PrimitiveValue[] value = new PrimitiveValue[entry.getValue().size()];
                for (int i = 0; i < value.length; i++) {
                    value[i] = entry.getValue().get(i);
                }

                PrimitiveValue[] combineTuple = createTupleFromKeyVal(key, value);

                groupByMap.remove(pv);
                useGroupByOutput = true;
                return combineTuple;
            }

        } else { // Process group by without Aggregates

            distinct = true;
            while (child.hasNext()) {

                if (tuple == null) {
                    tuple = child.next();
                    if (tuple == null)
                        continue;
                }

                if (!distinctList.contains(Arrays.toString(tuple)))
                    distinctList.add(tuple);
                tuple = null;
            }

            if (distinctList.size() > 0) {
                return distinctList.get(returnRow++);
            }
            noGroupByElementFound = true;
        }

        return null;
    }

    private List<Column> getColumnList(List<Column> groupByColumnReferences) {
        List<Column> columnList = new ArrayList<Column>();

        Schema[] schemas = child.getSchema();

        for (Column column : groupByColumnReferences) {
            columnList.add(column);
        }

        for (int i = 0; i < schemas.length; i++) {
            if (hasColumnName(columnList, schemas[i])) {
                setTableName(columnList, schemas[i].getColumnDefinition().getColumnName(), schemas[i].getTableName());
            }
        }

        return columnList;
    }

    private boolean hasColumnName(List<Column> columnList, Schema schema) {

        for (Column column : columnList) {
            if (column.getColumnName().equals(schema.getColumnDefinition().getColumnName()))
                return true;
        }
        return false;
    }

    private void setTableName(List<Column> columnList, String columnName, String tableName) {
        for (int i = 0; i < columnList.size(); i++) {
            if (columnList.get(i).getColumnName().equals(columnName)) {
                columnList.get(i).setTable(new Table(null, tableName));
                return;
            }
        }
    }

    private PrimitiveValue[] convertAndReturn(PrimitiveValue[] returnTuple) throws PrimitiveValue.InvalidPrimitive {

        PrimitiveValue[] pv = new PrimitiveValue[selectItems.size()];
        for (int i = 0; i < aggTypeOfSelectItems.size(); i++) {
            if (aggTypeOfSelectItems.get(i).toLowerCase().equals("avg")) {
                double count = returnTuple[indexOfGroupByCols.get(i)].toDouble();
                double sum = returnTuple[indexOfGroupByCols.get(i) + 1].toDouble();
                double avg = sum / count;

                pv[indexOfGroupByCols.get(i)] = new DoubleValue(avg);
            }
        }

        for (int i = 0; i < schema.length; i++) {
            if (indexOfNonGroupByCols.contains(i)) {
                pv[i] = returnTuple[i];
            }
        }

        return pv;
    }

    private boolean isSame(PrimitiveValue[] currentTuple, PrimitiveValue[] prevTuple) {

        for (int i = 0; prevTuple != null && i < indexOfNonGroupByCols.size(); i++) {
            if (!currentTuple[indexOfNonGroupByCols.get(i)].toRawString().equals(prevTuple[indexOfNonGroupByCols.get(i)].toRawString()))
                return false;
        }
        return true;
    }
    // todo: FIX HERE
    private PrimitiveValue[] groupByAccumulator(PrimitiveValue[] currentTuple, PrimitiveValue[] aggPrimitiveValues) throws PrimitiveValue.InvalidPrimitive {

        if (aggPrimitiveValues != null) {

            int j = 0;
            int k = 0;
            for (int index = 0; index < currentTuple.length; index++) {

                if(indexOfNonGroupByCols.contains(index)) {
                    aggPrimitiveValues[k] = currentTuple[index];
                } else {

                    String key = AggTypeToIndexMap.get(index);
                    switch (key){

                        case "COUNT":
                            aggPrimitiveValues[k] = new LongValue(aggPrimitiveValues[k].toLong() + 1);
                            break;
                        case "SUM":
                            aggPrimitiveValues[k] = commonLib.PrimitiveValueComparator(aggPrimitiveValues[k], currentTuple[index], "sum");
                            break;
                        case "MIN":
                            aggPrimitiveValues[k] = commonLib.PrimitiveValueComparator(aggPrimitiveValues[k], currentTuple[index], "min");
                            break;
                        case "MAX":
                            aggPrimitiveValues[k] = commonLib.PrimitiveValueComparator(aggPrimitiveValues[k], currentTuple[index], "max");
                            break;
                        case "AVG":
                            hasAvg = true;
                            aggPrimitiveValues[k] = new DoubleValue(aggPrimitiveValues[k].toDouble() + 1);
                            k++;
                            aggPrimitiveValues[k] = new DoubleValue(aggPrimitiveValues[k].toDouble() + currentTuple[index].toDouble());
                            break;
                    }
                }

                k++;
            }
            return aggPrimitiveValues;
        } else {
            aggPrimitiveValues = new PrimitiveValue[currentTuple.length + getCountofAvg()];

            int k = 0;
            for(int index = 0; index < aggPrimitiveValues.length; index++){
                if(indexOfNonGroupByCols.contains(index)) {
                    aggPrimitiveValues[index] = currentTuple[k++];
                } else {
                    String key = AggTypeToIndexMap.get(index);
                    switch (key){

                        case "COUNT":
                            aggPrimitiveValues[index] = new LongValue(1);
                            break;
                        case "SUM":
                            aggPrimitiveValues[index] = currentTuple[k++];
                            break;
                        case "MIN":
                            aggPrimitiveValues[index] = currentTuple[k++];
                            break;
                        case "MAX":
                            aggPrimitiveValues[index] = currentTuple[k++];
                            break;
                        case "AVG":
                            hasAvg = true;
                            aggPrimitiveValues[index] = new DoubleValue(1);
                            index++;
                            aggPrimitiveValues[index] = currentTuple[k++];
                            break;
                    }
                }
            }
            return aggPrimitiveValues;
        }
    }

    private int getCountofAvg() {
        int cnt = 0;
        for (int i = 0; i < aggTypeOfSelectItems.size(); i++) {
            if (aggTypeOfSelectItems.get(i).equals("AVG"))
                cnt++;
        }
        return cnt;
    }


    private PrimitiveValue[] createTupleFromKeyVal(PrimitiveValue[] key, PrimitiveValue[] value) throws PrimitiveValue.InvalidPrimitive {
        PrimitiveValue[] newTuple = new PrimitiveValue[schema.length];

        for (int i = 0; i < key.length; i++) {
            newTuple[indexOfNonGroupByCols.get(i)] = key[i];
        }
        int k = 0;
        for (int j = 0; j < aggTypeOfSelectItems.size(); j++) {
            if (aggTypeOfSelectItems.get(j).equals("AVG")) {
                double count = value[k].toDouble();
                k++;
                double sum = value[k].toDouble();
                double avg = sum / count;
                newTuple[indexOfGroupByCols.get(j)] = new DoubleValue(avg);
            } else {
                newTuple[indexOfGroupByCols.get(j)] = value[k];
            }
            k++;
        }
        return newTuple;
    }

    private void groupByAccumulator(PrimitiveValue[] tuple, List<String> aggType, List<Integer> indexOfNonGroupByCols, String groupByCols) throws PrimitiveValue.InvalidPrimitive {

        List<PrimitiveValue> currentValues;
        List<PrimitiveValue> newValues = new ArrayList<PrimitiveValue>();

        if (groupByMap.containsKey(groupByCols)) {
            currentValues = groupByMap.get(groupByCols);

            String values = "";
            int j = 0;
            for (int index = 0; index < currentValues.size(); index++) {

                String key = aggType.get(j);

                switch (key){
                    case "COUNT":
                        newValues.add(new LongValue(currentValues.get(index).toLong() + 1));
                        break;
                    case "SUM":
                        newValues.add(commonLib.PrimitiveValueComparator(currentValues.get(index), tuple[indexOfGroupByCols.get(j)], "sum"));
                        break;
                    case "MIN":
                        newValues.add(commonLib.PrimitiveValueComparator(currentValues.get(index), tuple[indexOfGroupByCols.get(j)], "min"));
                        break;
                    case "MAX":
                        newValues.add(commonLib.PrimitiveValueComparator(currentValues.get(index), tuple[indexOfGroupByCols.get(j)], "max"));
                        break;
                    case "AVG":
                        hasAvg = true;
                        PrimitiveValue count = new DoubleValue(currentValues.get(index).toDouble() + 1);
                        newValues.add(count);
                        index++;
                        PrimitiveValue sum = new DoubleValue(currentValues.get(index).toDouble() + tuple[indexOfGroupByCols.get(j)].toDouble());
                        newValues.add(sum);
                        break;
                }
                j++;
            }
            groupByMap.put(groupByCols, newValues);
        } else {
            for (int index = 0; index < aggType.size(); index++) {
                PrimitiveValue temp;

                String key = aggType.get(index);

                switch (key){
                    case "COUNT":
                        temp = new LongValue(1);
                        newValues.add(temp);
                        break;
                    case "SUM":
                        temp = tuple[indexOfGroupByCols.get(index)];
                        newValues.add(temp);
                        break;
                    case "MIN":
                        temp = tuple[indexOfGroupByCols.get(index)];
                        newValues.add(temp);
                        break;
                    case "MAX":
                        temp = tuple[indexOfGroupByCols.get(index)];
                        newValues.add(temp);
                        break;
                    case "AVG":
                        hasAvg = true;
                        PrimitiveValue count = new DoubleValue(1);
                        PrimitiveValue sum = tuple[indexOfGroupByCols.get(index)];
                        newValues.add(count);
                        newValues.add(sum);
                        break;
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
                            aggColMap.put(temp.getAlias(), "COUNT");
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