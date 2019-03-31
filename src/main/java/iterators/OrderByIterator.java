package iterators;

import dubstep.Main;
import helpers.CommonLib;
import helpers.Schema;
import helpers.Sort;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import sun.tools.tree.AndExpression;

import java.io.*;
import java.util.*;


public class OrderByIterator implements RAIterator {

    //region Variables

    public boolean sorted = false;
    // On disk variables
    public boolean onDiskSorted = false;
    String path = TableIterator.TABLE_DIRECTORY;
    Sort sort;
    private RAIterator child;
    private List<OrderByElement> orderByElementsList;
    private List<PrimitiveValue[]> sortedList = new ArrayList<PrimitiveValue[]>();
    private List<Integer> indexOfOrderByElements;
    private List<Boolean> orderOfOrderByElements; // asc : true, desc : false
    private int currentIndex = 0;
    private Schema[] schema;
    private PlainSelect plainSelect;
    private List<String> onDiskSortedList = new ArrayList<String>();
    private boolean noDataFound = false;
    private int onDiscRowToReturn = 0;
    private BufferedReader brMergedFile1;
    private BufferedReader brMergedFile2;
    private List<String> mergedFileLists = new ArrayList<String>();
    private String leftData;
    private String rightData;
    private Limit limit;
    private boolean limitReached = false;
    private long totalRowCounter = 0;
    private CommonLib commonLib = CommonLib.getInstance();
    long blockSize = commonLib.blockSize;
    PrimitiveValue tuple[];

    // On disk variables ends here

    //endregion

    //region Constructor

    public OrderByIterator(RAIterator child, List<OrderByElement> orderByElementsList, PlainSelect plainSelect) {

        this.child = child;
        this.orderByElementsList = orderByElementsList;
        this.schema = child.getSchema();
        this.plainSelect = plainSelect;

        limit = plainSelect.getLimit();

        initializeVars(plainSelect);
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

    //region Iterator methods

    private void initializeVars(PlainSelect plainSelect) {
        indexOfOrderByElements = new ArrayList<Integer>();
        orderOfOrderByElements = new ArrayList<Boolean>();
        List<SelectExpressionItem> listOfSelectItems = new ArrayList<SelectExpressionItem>();

        for (SelectItem selectItems : plainSelect.getSelectItems()) {
            SelectExpressionItem selectExpressionItem = (SelectExpressionItem) commonLib.castAs(selectItems, SelectExpressionItem.class);
            listOfSelectItems.add(selectExpressionItem);
        }

        for (OrderByElement orderByElement : orderByElementsList) {
            int index = 0;
            for (SelectExpressionItem selectExpressionItem : listOfSelectItems) {
                if (selectExpressionItem.getExpression().equals(orderByElement.getExpression())) {
                    indexOfOrderByElements.add(index);
                    orderOfOrderByElements.add(orderByElement.isAsc());
                    break;
                }
                index++;
            }
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
                if (noDataFound || (sortedList.size() < currentIndex))
                    return false;
                else
                    return true;
            }
        }
        return child.hasNext();

    }

    @Override
    public PrimitiveValue[] next() throws Exception {

        if (Main.inMem) {

            if (sorted) {
                tuple = sortedList.get(currentIndex);
                if(tuple == null)
                    noDataFound = true;
                currentIndex++;
                return tuple;
            }

            while (child.hasNext()) {
                tuple = child.next();
                if (tuple == null)
                    continue;
                sortedList.add(tuple);
            }

            Collections.sort(sortedList, new Comparator<PrimitiveValue[]>() {
                @Override
                public int compare(PrimitiveValue[] a, PrimitiveValue[] b) {
                    int i = 0;

                    for (Integer index : indexOfOrderByElements) {

                        try {
                            if (orderOfOrderByElements.get(i)) {

                                if (a[index] instanceof StringValue) {
                                    int comp = a[index].toString().compareTo(b[index].toString());
                                    if (comp != 0)
                                        return comp;
                                    else
                                        continue;
                                } else if (a[index] instanceof LongValue) {
                                    int comp = Long.valueOf(a[index].toLong()).compareTo(Long.valueOf(b[index].toLong()));
                                    if (comp != 0)
                                        return comp;
                                    else
                                        continue;
                                } else if (a[index] instanceof DoubleValue) {
                                    int comp = Double.valueOf(a[index].toDouble()).compareTo(Double.valueOf(b[index].toDouble()));
                                    if (comp != 0)
                                        return comp;
                                    else
                                        continue;
                                } else if (a[index] instanceof DateValue) {
                                    int comp = ((DateValue) a[index]).getValue().compareTo(((DateValue) b[index]).getValue());
                                    if (comp != 0)
                                        return comp;
                                    else
                                        continue;
                                }
                            } else {
                                if (a[index] instanceof StringValue) {
                                    int comp = a[index].toString().compareTo(b[index].toString());
                                    if (comp != 0)
                                        return -1 * comp;
                                    else
                                        continue;
                                } else if (a[index] instanceof LongValue) {
                                    int comp = Long.valueOf(a[index].toLong()).compareTo(Long.valueOf(b[index].toLong()));
                                    if (comp != 0)
                                        return -1 * comp;
                                    else
                                        continue;
                                } else if (a[index] instanceof DoubleValue) {
                                    int comp = Double.valueOf(a[index].toDouble()).compareTo(Double.valueOf(b[index].toDouble()));
                                    if (comp != 0)
                                        return -1 * comp;
                                    else
                                        continue;
                                } else if (a[index] instanceof DateValue) {
                                    int comp = ((DateValue) a[index]).getValue().compareTo(((DateValue) b[index]).getValue());
                                    if (comp != 0)
                                        return -1 * comp;
                                    else
                                        continue;
                                }
                            }

                        } catch (Exception e) {

                        }
                    }

                    return -1;
                }
            });

            sorted = true;

            PrimitiveValue[] primitiveValue = sortedList.get(currentIndex);
            currentIndex++;

            return primitiveValue;

        } else {
            if (onDiskSorted) {

                tuple = sort.getTuple();
                if (tuple == null) {
                    noDataFound = true;
                }
                return tuple;
            }

            if (child.hasNext()) {

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
                columnList.get(i).setTable(new Table(null, tableName.toLowerCase()));
                return;
            }
        }
    }

    public List<OrderByElement> getOrderByElementsList() {
        return orderByElementsList;
    }

    public PlainSelect getPlainSelect() {
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
    public RAIterator optimize(RAIterator iterator) {
        RAIterator child = iterator.getChild();
        child = child.optimize(child);
        iterator.setChild(child);
        return iterator;
    }

    //endregion
}
