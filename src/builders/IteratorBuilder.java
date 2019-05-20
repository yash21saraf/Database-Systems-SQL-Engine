package builders;

import dubstep.Main;
import helpers.CommonLib;
import helpers.IndexMaker;
import helpers.PrimitiveValueWrapper;
import helpers.Schema;
import iterators.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;

import java.util.*;

public class IteratorBuilder {

    public static Map<String, Schema[]> iteratorSchemas = new HashMap();
    public static Map<String, CreateTable> schemas = new HashMap();

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static Map<String, List<PrimitiveValue[]>> newInserts = new HashMap<>();
    public static HashMap<String, List<Statement>> listOfStatements = new HashMap<>() ;
    private CommonLib commonLib = CommonLib.getInstance();

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    public IteratorBuilder() {
    }

    /**
     * Method to read in CREATE TABLE statement
     * Stores the associated List<ColumnDefinition> schema
     *
     * @param statement Statement interface object extracted from JSQLParser
     *                  Statement can be cast as CreateTable or Select
     * @return The final RAIterator object to execute SQL query in volcano-style
     */
    public RAIterator parseStatement(Statement statement) throws Exception {

        CreateTable createTable;
        Select select;
        Insert insert ;
        Delete delete ;
        Update update ;

        if ((createTable = (CreateTable) CommonLib.castAs(statement, CreateTable.class)) != null) {
            buildCreateTable(createTable);
            if(Main.isPhase1){
                IndexMaker.createIndex(createTable);
            }
            return null;

        } else if ((select = (Select) CommonLib.castAs(statement, Select.class)) != null) {
            return buildSelect(select);

        } else if ((insert = (Insert) CommonLib.castAs(statement, Insert.class)) != null){
            List<PrimitiveValue[]> updatedList;
            List<PrimitiveValue[]> currentList = newInserts.get(insert.getTable().getName());
            updatedList = (currentList != null) ? currentList : new LinkedList<PrimitiveValue[]>();
            ExpressionList itemsList = (ExpressionList) insert.getItemsList();
            ArrayList<PrimitiveValue> currentTuple = new ArrayList<>() ;
            for(Expression expression : itemsList.getExpressions()){
                currentTuple.add((PrimitiveValue) expression);
            }
            PrimitiveValue[] currentTupleArray = currentTuple.toArray(new PrimitiveValue[0]);
            updatedList.add(currentTupleArray);
            newInserts.put(insert.getTable().getName(), updatedList);
            return null ;
        } else if ((delete = (Delete) CommonLib.castAs(statement, Delete.class)) != null){

            // Update the present list
            if(newInserts.containsKey(delete.getTable().getName())){
                List<PrimitiveValue[]> currentInserts = newInserts.get(delete.getTable().getName());
                List<PrimitiveValue[]> updatedInserts = new ArrayList<>() ;
                for(PrimitiveValue[] tuple : currentInserts){
                    PrimitiveValueWrapper[]  nextLineWrapper = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(tuple, schemas.get(delete.getTable().getName()).getColumnDefinitions().toArray(new ColumnDefinition[0]), delete.getTable().getName());
                    PrimitiveValueWrapper result = commonLib.eval(new InverseExpression(delete.getWhere()), nextLineWrapper);
                    if(result.getPrimitiveValue().toBool()){
                        updatedInserts.add(tuple) ;
                    }
                }
                newInserts.put(delete.getTable().getName(), updatedInserts) ;
            }

            // Maintain list for original Table values
            List<Statement> updatedList;
            List<Statement> currentList = listOfStatements.get(delete.getTable().getName());
            updatedList = (currentList != null) ? currentList : new LinkedList<Statement>();
            if(updatedList.size() == 0){
                updatedList.add(delete) ;
            }else{
                if (updatedList.get(updatedList.size()-1) instanceof Delete){
                    delete.setWhere(new OrExpression(delete.getWhere(), ((Delete) updatedList.get(updatedList.size()-1)).getWhere()));
                    updatedList.set(updatedList.size()-1, delete) ;
                }else{
                    updatedList.add(delete) ;
                }
            }
            listOfStatements.put(delete.getTable().getName(), updatedList);
            return null ;
        } else if ((update = (Update) CommonLib.castAs(statement, Update.class)) != null){
            // Update the newly inserted values
            if(newInserts.containsKey(update.getTable().getName())){
                List<PrimitiveValue[]> currentInserts = newInserts.get(update.getTable().getName());
                List<PrimitiveValue[]> updatedInserts = new ArrayList<>() ;


                ArrayList<Integer> updateIndex = new ArrayList<>() ;
                for(Column column : update.getColumns()){
                    for(int i = 0; i < schemas.get(update.getTable().getName()).getColumnDefinitions().size() ; i++){
                        if(column.getColumnName().equals(schemas.get(update.getTable().getName()).getColumnDefinitions().get(i).getColumnName())){
                            updateIndex.add(i) ;
                        }
                    }
                }
                for(PrimitiveValue[] tuple : currentInserts){
                    PrimitiveValueWrapper[]  nextLineWrapper = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(tuple, schemas.get(update.getTable().getName()).getColumnDefinitions().toArray(new ColumnDefinition[0]), update.getTable().getName());
                    CaseExpression exp = new CaseExpression() ;
                    ArrayList<WhenClause> whenClauseArray = new ArrayList<>() ;
                    WhenClause whenClause = new WhenClause() ;
                    whenClause.setWhenExpression(update.getWhere());
                    Expression thenExp = null;
                    Expression elseExp = null ;
                    for(int i = 0; i < update.getColumns().size(); i++){
                        thenExp = update.getExpressions().get(i) ;
                        elseExp = update.getColumns().get(i) ;
                        whenClause.setThenExpression(thenExp);
                        whenClauseArray.add(whenClause) ;
                        exp.setWhenClauses(whenClauseArray);
                        exp.setElseExpression(elseExp);
                        PrimitiveValueWrapper result = commonLib.eval(exp, nextLineWrapper);
                        tuple[updateIndex.get(i)] = result.getPrimitiveValue() ;
                    }
                    updatedInserts.add(tuple) ;
                }
                newInserts.put(update.getTable().getName(), updatedInserts) ;
            }

            // Update the original Table values
            List<Statement> updatedList;
            List<Statement> currentList = listOfStatements.get(update.getTable().getName());
            updatedList = (currentList != null) ? currentList : new LinkedList<Statement>();
            updatedList.add(update) ;
            listOfStatements.put(update.getTable().getName(), updatedList);
            return null ;
        }
        throw new Exception("Invalid statement");
    }


    /**
     * Method to parse the SelectBody interface which can be a PlainSelect object or Union object
     *
     * @param selectBody   Interface containing the body of a SELECT statement
     * @param rootIterator RAIterator object upon which the SelectBody's iterators will be added
     * @return RAIterator object containing the SelecyBody's iterators
     */
    private RAIterator parseSelectBody(SelectBody selectBody, String selectAlias, RAIterator rootIterator) throws Exception {

        Union union;
        PlainSelect plainSelect;

        if ((union = (Union) CommonLib.castAs(selectBody, Union.class)) != null) {
            rootIterator = buildUnion(union, rootIterator);
        } else if ((plainSelect = (PlainSelect) CommonLib.castAs(selectBody, PlainSelect.class)) != null) {
            //plainSelect = rebuildSelect(plainSelect);
            rootIterator = buildPlainSelect(plainSelect, selectAlias, rootIterator);
        }

        return rootIterator;
    }


    private RAIterator parseFromItem(FromItem fromItem, RAIterator rootIterator) throws Exception {

        Table table;
        SubSelect subSelect;
        SubJoin subJoin;

        if ((table = (Table) CommonLib.castAs(fromItem, Table.class)) != null) {
            rootIterator = buildTable(table, rootIterator);
        } else if ((subSelect = (SubSelect) CommonLib.castAs(fromItem, SubSelect.class)) != null) {
            rootIterator = buildSubSelect(subSelect, rootIterator);
        } else if ((subJoin = (SubJoin) CommonLib.castAs(fromItem, SubJoin.class)) != null) {
            rootIterator = buildSubJoin(subJoin, rootIterator);
        }

        return rootIterator;

    }

    //endregion

    //region Building methods

    /**
     * Method to read in CREATE TABLE statement
     * Stores the associated List<ColumnDefinition> schema
     * }
     *
     * @param createTable CreateTable object containing the schema and relevant details
     *                    of the base table to be read from filesystem
     */
    private void buildCreateTable(CreateTable createTable) {

      /*if (createTable.getTable().getAlias() != null)
         schemas.put(createTable.getTable().getAlias(),createTable);
      else*/
        schemas.put(createTable.getTable().getName().toUpperCase(), createTable);
    }

    /**
     * Method to read in SELECT statement and begin building Iterator object
     * Calls buildWithItem() and parseSelectBody() methods
     *
     * @param select Select object which is any valid SQL query
     * @return The final RAIterator object to execute SQL query in volcano-style
     */
    private RAIterator buildSelect(Select select) throws Exception {

        RAIterator rootIterator = null;

        if (select.getWithItemsList() != null) {
            for (WithItem withItem : select.getWithItemsList())
                rootIterator = buildWithItem(withItem, rootIterator);
        }

        rootIterator = parseSelectBody(select.getSelectBody(), null, rootIterator);

        return rootIterator;

    }

    /**
     * Method to parse WITH q AS... clause of SQL Statement
     *
     * @param withItem     The WithItem object containing various parts of the WITH statement
     * @param rootIterator RAIterator object upon which the WITH statement's iterators will be added
     * @return RAIterator object containing the underlying iterators of the WITH statement
     */
    private RAIterator buildWithItem(WithItem withItem, RAIterator rootIterator) throws Exception {

        rootIterator = parseSelectBody(withItem.getSelectBody(), withItem.getName(), rootIterator);

        return rootIterator;

    }


    private RAIterator buildUnion(Union union, RAIterator rootIterator) throws Exception {

        RAIterator[] plainSelectIterators = new RAIterator[union.getPlainSelects().size()];
        for (int index = 0; index < plainSelectIterators.length; index++) {
            RAIterator plainSelectIterator = null;
            plainSelectIterators[index] = buildPlainSelect(union.getPlainSelects().get(index), null, plainSelectIterator);
        }

        return new UnionIterator(union, plainSelectIterators);

    }

    /**
     * Method to build iterator on the PlainSelect object, which can contain
     * the various modifiers in a SQL statement
     *
     * @param plainSelect  PlainSelect object representing the SELECT targetlist FROM tables WHERE clauses
     * @param rootIterator RAIterator object upon which the PlainSelect's iterators will be added
     * @return RAIterator object containing the PlainSelect's iterators
     */
    private RAIterator buildPlainSelect(PlainSelect plainSelect, String selectAlias, RAIterator rootIterator) throws Exception {

        if (plainSelect.getFromItem() != null)
            rootIterator = parseFromItem(plainSelect.getFromItem(), rootIterator);

        if (plainSelect.getJoins() != null)
            if (!plainSelect.getJoins().isEmpty())
                for (Join join : plainSelect.getJoins())
                    rootIterator = buildJoin(join, rootIterator);

        if (plainSelect.getWhere() != null)
            rootIterator = new FilterIterator(rootIterator, plainSelect.getWhere());

        rootIterator = new MapIterator(rootIterator, plainSelect.getSelectItems(), selectAlias);

        if (plainSelect.getGroupByColumnReferences() != null) {
            rootIterator = new GroupByIterator(rootIterator, plainSelect.getSelectItems(), selectAlias, plainSelect.getGroupByColumnReferences());
        } else if (plainSelect.getGroupByColumnReferences() == null && isAggregateQuery(plainSelect.getSelectItems())) { // Aggregate Iterator
            rootIterator = new aggregateIterator(rootIterator, plainSelect.getSelectItems(), selectAlias);
        }


        if (plainSelect.getHaving() != null) {
            rootIterator = new HavingIterator(rootIterator, plainSelect.getSelectItems(), plainSelect.getHaving());
        }

        if (plainSelect.getOrderByElements() != null) {
            rootIterator = new OrderByIterator(rootIterator, plainSelect.getOrderByElements(), plainSelect);
        }

        if (plainSelect.getLimit() != null) {
            rootIterator = new LimitIterator(rootIterator, plainSelect.getLimit());
        }

        return rootIterator;

    }

    /**
     * @param table        Table object using which the TableIterator will be created
     * @param rootIterator RAIterator object upon which the TableIterator will be created
     * @return RAIterator object containing a TableIterator
     */
    private RAIterator buildTable(Table table, RAIterator rootIterator) throws Exception {
        String tableAlias;
        if (table.getAlias() != null)
            tableAlias = table.getAlias();
        else
            tableAlias = table.getName();

        ColumnDefinition[] columnDefinitions = schemas.get(table.getName().toUpperCase()).getColumnDefinitions().toArray(new ColumnDefinition[schemas.get(table.getName().toUpperCase()).getColumnDefinitions().size()]);
        rootIterator = new TableIterator(table.getName(), tableAlias, columnDefinitions);
        return rootIterator;
    }

    /**
     * @param subSelect    SELECT statement which is used as a FROM clause
     * @param rootIterator RAIterator upon which the SubSelect's iterators will be added
     * @return RAIterator object containing the SubSelect's iterators
     */
    private RAIterator buildSubSelect(SubSelect subSelect, RAIterator rootIterator) throws Exception {

        rootIterator = parseSelectBody(subSelect.getSelectBody(), subSelect.getAlias(), rootIterator);
        return rootIterator;

    }

    /**
     * @param subJoin      Table created by JOIN
     * @param rootIterator RAIterator object upon which the SubJoin's iterators will be added
     * @return RAIterator object containing the SubJoin's iterators
     */
    private RAIterator buildSubJoin(SubJoin subJoin, RAIterator rootIterator) throws Exception {

        RAIterator leftIterator = null;
        leftIterator = parseFromItem(subJoin.getLeft(), leftIterator);
        rootIterator = buildJoin(subJoin.getJoin(), leftIterator);
        return rootIterator;

    }

    /**
     * @param join         JOIN part in a SQL statement
     * @param rootIterator RAIterator object upon which the JoinIterator will be added
     * @return RAIterator object containing the JoinIterator
     */
    private RAIterator buildJoin(Join join, RAIterator rootIterator) throws Exception {

        RAIterator rightIterator = null;
        rightIterator = parseFromItem(join.getRightItem(), rightIterator);
        rootIterator = new JoinIterator(rootIterator, rightIterator, join.getOnExpression());
        return rootIterator;

    }

    //endregion

    private boolean isAggregateQuery(List<SelectItem> selectItems) {
        Function function;

        for (int index = 0; index < selectItems.size(); index++) {
            if (selectItems.get(index) instanceof AllColumns) {
                continue;
            } else if (selectItems.get(index) instanceof AllTableColumns) {
                continue;
            } else if (((SelectExpressionItem) selectItems.get(index)).getExpression() instanceof Function) {
                return true;
            }
        }
        return false;
    }
}
