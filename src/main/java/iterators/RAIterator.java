package iterators;

import helpers.PrimitiveValueWrapper;
import helpers.Schema;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

public interface RAIterator
{

   public boolean hasNext() throws Exception;

   public PrimitiveValue[] next() throws Exception;

   public void reset() throws Exception;

   public RAIterator getChild();

   public void setChild(RAIterator child);

   public Schema[] getSchema() ;

   public void setSchema(Schema[] schema) ;

   public RAIterator optimize(RAIterator iterator);
}
