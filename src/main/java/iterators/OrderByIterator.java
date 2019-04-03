package iterators;

import dubstep.Main;
import helpers.CommonLib;
import helpers.Schema;
import helpers.Sort;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;


public class OrderByIterator implements RAIterator {

    //region Variables

    public boolean sorted = false;

    private RAIterator child;
    private List<OrderByElement> orderByElementsList;
    private List<List<PrimitiveValue>> sortedList = new ArrayList<List<PrimitiveValue>>();
    private List<Integer> indexOfOrderByElements;
    private List<Boolean> orderOfOrderByElements; // asc : true, desc : false
    private int currentIndex = 0;
    private Schema[] schema;
    private PlainSelect plainSelect;

    // On disk variables
    public boolean onDiskSorted = false;
    private boolean noDataFound = false;

    ////////////////////////////////////////
    PrimitiveValue tuple[];
    Sort sort;

    private CommonLib commonLib = CommonLib.getInstance();
    // On disk variables ends here

    //endregion

    //region Constructor

    public OrderByIterator(RAIterator child, List<OrderByElement> orderByElementsList, PlainSelect plainSelect) {

        this.child = child;
        this.orderByElementsList = orderByElementsList;
        this.schema = child.getSchema();
        this.plainSelect = plainSelect;

        Limit limit = plainSelect.getLimit();

        initializeVars(plainSelect);
    }

    private void initializeVars(PlainSelect plainSelect) {

        AllTableColumns allTableColumns;
        AllColumns allColumns;
        SelectItem selectItem;
        List<SelectItem> selectItemList = new ArrayList<SelectItem>();

        if ((allTableColumns = (AllTableColumns) CommonLib.castAs(plainSelect.getSelectItems().get(0), AllTableColumns.class)) != null) {

            for (int i = 0; i < schema.length; i++) {
                if (allTableColumns.getTable().getName().equals(schema[i].getTableName())) {
                    selectItem = new SelectExpressionItem();
                    Column column = new Column();
                    column.setTable(new Table(schema[i].getTableName()));
                    column.setColumnName(schema[i].getColumnDefinition().getColumnName());
                    ((SelectExpressionItem) selectItem).setExpression(column);
                    selectItemList.add(selectItem);
                }
                plainSelect.setSelectItems(selectItemList);
            }


        } else if ((allColumns = (AllColumns) CommonLib.castAs(plainSelect.getSelectItems().get(0), AllColumns.class)) != null) {

            for (int i = 0; i < schema.length; i++) {
                selectItem = new SelectExpressionItem();
                Column column = new Column();
                column.setTable(new Table(schema[i].getTableName()));
                column.setColumnName(schema[i].getColumnDefinition().getColumnName());
                ((SelectExpressionItem) selectItem).setExpression(column);
                selectItemList.add(selectItem);
            }
            plainSelect.setSelectItems(selectItemList);
        }


        indexOfOrderByElements = new ArrayList<Integer>();
        orderOfOrderByElements = new ArrayList<Boolean>();
        List<SelectExpressionItem> listOfSelectItems = new ArrayList<SelectExpressionItem>();

        for (SelectItem selectItems : plainSelect.getSelectItems()) {
            SelectExpressionItem selectExpressionItem = (SelectExpressionItem) commonLib.castAs(selectItems, SelectExpressionItem.class);
            listOfSelectItems.add(selectExpressionItem);
        }

        for (OrderByElement orderByElement : orderByElementsList) {
            int index = 0 ;
            for (SelectExpressionItem selectExpressionItem : listOfSelectItems) {
                if(selectExpressionItem.getAlias() != null && selectExpressionItem.getAlias().equals(orderByElement.getExpression().toString())){
                    indexOfOrderByElements.add(index) ;
                    orderOfOrderByElements.add((orderByElement.isAsc())) ;
                    break ;
                }
                else if (selectExpressionItem.getExpression().equals(orderByElement.getExpression())) {
                    indexOfOrderByElements.add(index);
                    orderOfOrderByElements.add(orderByElement.isAsc());
                    break;
                }
                index++;
            }
        }
    }

    //endregion

    //region Iterator methods

    public static boolean isNumber(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Override
    public boolean hasNext() throws Exception {

        if (!Main.inMem) {
            if (onDiskSorted) {
                if (noDataFound)
                    return false;
                else
                    return true;
            }
        } else {
            if (sorted) {
                if (sortedList.size() > currentIndex)
                    return true;
                else
                    return false;
            }
        }
        return child.hasNext();

    }

    @Override
    public PrimitiveValue[] next() throws Exception {

        if (Main.inMem) {

            if (sorted) {
                PrimitiveValue[] primitiveValueWrappers = sortedList.get(currentIndex).toArray(new PrimitiveValue[sortedList.get(0).size()]);
                currentIndex++;
                return primitiveValueWrappers;
            }

            while (child.hasNext()) {
                PrimitiveValue[] tuple = child.next();
                if (tuple == null)
                    continue;
                sortedList.add(Arrays.asList(tuple));
            }

            Collections.sort(sortedList, new Comparator<List<PrimitiveValue>>() {
                @Override
                public int compare(List<PrimitiveValue> first, List<PrimitiveValue> second) {

                    int i = 0;

                    for (Integer index : indexOfOrderByElements) {
                        String primitiveValue1 = first.get(index).toRawString();
                        String primitiveValue2 = second.get(index).toRawString();

                        if (isNumber(primitiveValue1)) {

                            double pv1 = Double.parseDouble(primitiveValue1);
                            double pv2 = Double.parseDouble(primitiveValue2);

                            if (orderOfOrderByElements.get(i++)) {

                                if (pv1 < pv2)
                                    return -1;
                                else if (pv1 > pv2)
                                    return 1;
                                else {
                                    continue;
                                }

                            } else {

                                if (pv1 < pv2)
                                    return 1;
                                else if (pv1 > pv2)
                                    return -1;
                                else {
                                    continue;
                                }
                            }

                        } else {

                            if (orderOfOrderByElements.get(i++)) {

                                if (primitiveValue1.compareTo(primitiveValue2) != 0)
                                    return primitiveValue1.compareTo(primitiveValue2);
                                else {
                                    continue;
                                }

                            } else {

                                if (primitiveValue1.compareTo(primitiveValue2) != 0)
                                    return -1 * primitiveValue1.compareTo(primitiveValue2);
                                else {
                                    continue;
                                }
                            }
                        }

                    }
                    return 1;
                }
            });

            sorted = true;

            PrimitiveValue[] primitiveValueWrappers = sortedList.get(currentIndex).toArray(new PrimitiveValue[sortedList.get(0).size()]);
            currentIndex++;

            return primitiveValueWrappers;

        } else {
            if (onDiskSorted) {

                tuple = sort.getTuple();
                if (tuple == null) {
                    noDataFound = true;
                }
                return tuple;
            }

            else if (child.hasNext()) {

                List<Column> columnsList = getColumnList(orderByElementsList);
                sort = new Sort(child, columnsList, orderOfOrderByElements, indexOfOrderByElements, false);

                sort.sort();

                onDiskSorted = true;
                tuple = sort.getTuple();
                if (tuple == null) {
                    noDataFound = true;
                }
                return tuple;
            }
            return null;
        }

    }

    private List<Column> getColumnList(List<OrderByElement> orderByElementsList) {
        List<Column> columnList = new ArrayList<Column>();

        Schema[] schemas = child.getSchema();

        for (OrderByElement expression : orderByElementsList) {
            columnList.add((Column) expression.getExpression());
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


    public List<OrderByElement> getOrderByElementsList()
    {
        return orderByElementsList;
    }

    public PlainSelect getPlainSelect()
    {
        return plainSelect;
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
    public RAIterator optimize(RAIterator iterator)
    {
        RAIterator child = iterator.getChild();
        child = child.optimize(child);
        iterator.setChild(child);
        return iterator;
    }

    //endregion
}
