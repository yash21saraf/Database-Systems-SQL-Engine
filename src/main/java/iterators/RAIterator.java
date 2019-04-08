package iterators;

import helpers.Schema;
import net.sf.jsqlparser.expression.PrimitiveValue;

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
