package helpers;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;

public class BasicEval {

    PrimitiveValue pv[];

    public Eval eval = new Eval() {
        @Override
        public PrimitiveValue eval(Column column) throws SQLException {
            return pv[0];
        }
    };

    public BasicEval(PrimitiveValue[] primitiveValues) {
        this.pv = primitiveValues;
    }

}