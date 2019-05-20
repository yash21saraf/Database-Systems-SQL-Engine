package helpers;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.sql.SQLException;

public class BasicEval {

    PrimitiveValue pv[];
    private  Schema []schema;
    private ColumnDefinition []columnDefinitions;

   /* public Eval eval = new Eval() {
        @Override
        public PrimitiveValue eval(Column column) throws SQLException {
            return pv[0];
        }
    };*/

    public BasicEval(PrimitiveValue[] primitiveValues) {
        this.pv = primitiveValues;
    }

    public BasicEval(ColumnDefinition []columnDefinitions, PrimitiveValue[] primitiveValues){
        this.columnDefinitions = columnDefinitions;
        this.pv = primitiveValues;
    }


    public Eval eval = new Eval()
    {
        @Override
        public PrimitiveValue eval(Column column) throws SQLException
        {
            int i=0;
            for (PrimitiveValue tuple : pv) {

                if (column.getWholeColumnName().equals(columnDefinitions[i].getColumnName()))
                    return tuple;
                else if (column.getTable().getName() == null && column.getTable().getAlias() == null && column.getWholeColumnName().equals(columnDefinitions[i].getColumnName()))
                    return tuple;
            }
            throw new SQLException("No column with name: " + column.getColumnName() + ".");
        }
    };


    /*public PrimitiveValue eval(Expression expression,PrimitiveValue[] tuple) throws SQLException
    {
        try {
            PrimitiveValue evaluatedExpression = new PrimitiveValue();
            this.tuples.add(tuple);
            evaluatedExpression.setPrimitiveValue(eval.eval(expression));
            this.tuples.remove(tuple);
            return evaluatedExpression;
        } catch (SQLException e) {
            //e.printStackTrace();
            throw e;
        }
    }*/

}