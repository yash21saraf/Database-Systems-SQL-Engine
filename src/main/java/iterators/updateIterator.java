package iterators;

import dubstep.Main;
import helpers.CommonLib;
import helpers.PrimitiveValueWrapper;
import helpers.Schema;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.update.Update;

import java.sql.Array;
import java.sql.Statement;
import java.util.*;

public class updateIterator implements RAIterator {
    //region Variables

    //private static final Logger logger = LogManager.getLogger();
    private CommonLib commonLib = CommonLib.getInstance();

    private RAIterator child;
    private Update statement;
    private Schema[] schema;
    private PrimitiveValue[] tuple;
    private boolean hasNextValue = false;
    private boolean usingInserts = false ;
    private ArrayList<Integer> updateIndex ;

    //endregion

    //region Constructor

    public updateIterator(RAIterator child, Update statement) {

        this.child = child;
        this.statement = statement;
        this.schema = child.getSchema();

        ArrayList<Integer> updateIndex = new ArrayList<>() ;
        for(Column column : statement.getColumns()){
            for(int i = 0; i < this.schema.length ; i++){
                if(column.getColumnName().equals(this.schema[i].getColumnDefinition().getColumnName())){
                    updateIndex.add(i) ;
                }
            }
        }
        this.updateIndex = updateIndex ;

    }

    //endregion

    //region Iterator methods

    public boolean hasNext() throws Exception {

        if (hasNextValue)
            return true;

        while (child.hasNext()) {
            tuple = child.next();
            if (tuple == null)
                return false;
            else if(child instanceof TableIterator && ((TableIterator) child).getUsingInserts()){
                this.usingInserts = true ;
                return true ;
            }else if(child instanceof IndexIterator && ((IndexIterator) child).getUsingInserts()){
                this.usingInserts = true ;
                return true ;
            }else if(child instanceof FilterIterator && ((FilterIterator) child).getUsingInserts()){
                this.usingInserts = true ;
                return true ;
            }else if(child instanceof updateIterator && ((updateIterator) child).getUsingInserts()){
                this.usingInserts = true ;
                return true ;
            }
            hasNextValue = true ;
            tuple = processTuple(tuple) ;
            return true ;
        }

        return false;
    }

    private PrimitiveValue[] processTuple(PrimitiveValue[] tuple) throws Exception {
            PrimitiveValueWrapper[]  nextLineWrapper = commonLib.convertTuplePrimitiveValueToPrimitiveValueWrapperArray(tuple, this.schema);
            CaseExpression exp = new CaseExpression() ;
            ArrayList<WhenClause> whenClauseArray = new ArrayList<>() ;
            WhenClause whenClause = new WhenClause() ;
            whenClause.setWhenExpression(statement.getWhere());
            Expression thenExp = null;
            Expression elseExp = null ;
            for(int i = 0; i < statement.getColumns().size(); i++){
                thenExp = statement.getExpressions().get(i) ;
                elseExp = statement.getColumns().get(i) ;
                whenClause.setThenExpression(thenExp);
                whenClauseArray.add(whenClause) ;
                exp.setWhenClauses(whenClauseArray);
                exp.setElseExpression(elseExp);
                PrimitiveValueWrapper result = commonLib.eval(exp, nextLineWrapper);
                tuple[updateIndex.get(i)] = result.getPrimitiveValue() ;
            }
            return tuple;
    }

    @Override
    public PrimitiveValue[] next() throws Exception {
        hasNextValue = false;
        return tuple;
    }

    @Override
    public void reset() throws Exception {
        this.usingInserts = false ;
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

    public boolean getUsingInserts(){
        return this.usingInserts ;
    }

    //endregion
}
