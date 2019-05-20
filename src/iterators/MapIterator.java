package iterators;

import builders.IteratorBuilder;
import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import helpers.Schema;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
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
    private Schema[] schema ;
    private boolean isAggquery = false;


    //endregion

    //region Constructor

    public MapIterator(RAIterator child,List<SelectItem> selectItems,String tableAlias) throws Exception {

        this.child = child;
        this.selectItems = selectItems;
        this.tableAlias = tableAlias;

        createSchema(selectItems, child.getSchema()) ;
        if (this.isAggquery) {
            this.selectItems = getUnpackedSelectedItems(selectItems);
        }

    }

    private void createSchema(List<SelectItem> selectItems, Schema[] childSchema) {
        SelectExpressionItem selectExpressionItem;
        AllTableColumns allTableColumns;
        AllColumns allColumns;
        Column column;

        ArrayList<Schema> projectedTuplenew = new ArrayList() ;

        for (int index = 0; index < selectItems.size(); index++) {

            if (selectItems.get(index) instanceof SelectExpressionItem) {
                selectExpressionItem = (SelectExpressionItem) CommonLib.castAs(selectItems.get(index),SelectExpressionItem.class) ;
                Expression expression = selectExpressionItem.getExpression() ;
                String alias = selectExpressionItem.getAlias();
                if(expression instanceof Function){
                    Function function = (Function) CommonLib.castAs(expression, Function.class) ;
                    if(function.isAllColumns()){
                        Schema newSchema = new Schema();
                        ColDataType colDataType = new ColDataType();
                        colDataType.setDataType("int");
                        newSchema.setColumnDefinition(alias, colDataType, null);
                        newSchema.setTableName(childSchema[0].getTableName());
                        projectedTuplenew.add(newSchema);
                    }else {
                        Expression expp = function.getParameters().getExpressions().get(0);
                        Column tempCol = commonLib.getColumnList(expp).get(0);
                        for (Schema schema : childSchema) {
                            if (schema.getColumnDefinition().getColumnName().equals(tempCol.getColumnName())) {

                                Schema newSchema = new Schema();
                                newSchema.setColumnDefinition(alias,schema.getColumnDefinition().getColDataType(),null);
                                newSchema.setTableName(schema.getTableName());
                                projectedTuplenew.add(newSchema);
                                break;
                            }
                        }
                    }
                    this.isAggquery = true ;
                }

                else if(expression instanceof Column){
                    column = (Column) CommonLib.castAs(expression,Column.class) ;
                    for(Schema schema : childSchema){
                        if(schema.getColumnDefinition().getColumnName().equals(column.getColumnName())){

                            Schema newSchema = new Schema();
                            if(alias == null)
                                newSchema.setColumnDefinition(schema.getColumnDefinition());
                            else
                                newSchema.setColumnDefinition(alias, schema.getColumnDefinition().getColDataType(), schema.getColumnDefinition().getColumnSpecStrings());
                            newSchema.setTableName(schema.getTableName());
                            projectedTuplenew.add(newSchema);
                            break ;
                        }
                    }
                }

                else{
                    Schema newSchema = new Schema() ;
                    newSchema.setColumnDefinition(alias, null, null); //TODO: How to get columnDefinition for functions, for now set null
                    newSchema.setTableName(null);
                    projectedTuplenew.add(newSchema) ;
                }

            } else if ((allTableColumns = (AllTableColumns) CommonLib.castAs(selectItems.get(index),AllTableColumns.class)) != null) {
                projectedTuplenew.addAll(Arrays.asList(IteratorBuilder.iteratorSchemas.get(allTableColumns.getTable().getName())));
            } else if ((allColumns = (AllColumns) CommonLib.castAs(selectItems.get(index),AllColumns.class)) != null) {
                projectedTuplenew.addAll(Arrays.asList(childSchema));
            }
        }
        this.schema = projectedTuplenew.toArray(new Schema[projectedTuplenew.size()]) ;
        Schema[] newSchema = new Schema[projectedTuplenew.size()] ;

        if(this.tableAlias != null){
            for(int i = 0 ; i < projectedTuplenew.size() ; i++){
                Schema temp = new Schema() ;
                temp.setColumnDefinition(this.schema[i].getColumnDefinition());
                temp.setTableName(this.tableAlias) ;
                newSchema[i] = temp ;
            }
            IteratorBuilder.iteratorSchemas.put(this.tableAlias, newSchema);
        }
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
        PrimitiveValueWrapper[] wrappedTuple = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(tuple, child.getSchema());
        ArrayList<PrimitiveValue> projectedTuple = new ArrayList();

        if (tuple == null)
            return null;
        for (int index = 0; index < selectItems.size(); index++) {
            if ((selectExpressionItem = (SelectExpressionItem) CommonLib.castAs(selectItems.get(index),SelectExpressionItem.class)) != null) {
                PrimitiveValueWrapper evaluatedExpression = commonLib.eval(selectExpressionItem.getExpression(),wrappedTuple);
                projectedTuple.add(evaluatedExpression.getPrimitiveValue());

            } else if ((allTableColumns = (AllTableColumns) CommonLib.castAs(selectItems.get(index),AllTableColumns.class)) != null) {
                for (int secondIndex = 0; secondIndex < tuple.length; secondIndex++) {
                    if (this.schema[secondIndex].getTableName().equals(allTableColumns.getTable().getName())) {
                        projectedTuple.add(tuple[secondIndex]);
                    }
                }

            } else if ((allColumns = (AllColumns) CommonLib.castAs(selectItems.get(index),AllColumns.class)) != null) {
                projectedTuple.addAll(Arrays.asList(tuple)) ;
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
    public Schema[] getSchema() {
        if(this.tableAlias != null){
            return IteratorBuilder.iteratorSchemas.get(this.tableAlias) ;
        }
        else return this.schema ;
    }

    @Override
    public void setSchema(Schema[] schema) {
        this.schema = schema ;
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

    public List<SelectItem> getSelectItems()
    {
        return selectItems;
    }

    public String getTableAlias()
    {
        return tableAlias;
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