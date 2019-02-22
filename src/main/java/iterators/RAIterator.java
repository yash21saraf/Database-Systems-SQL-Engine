package iterators;

import helpers.PrimitiveValueWrapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

public interface RAIterator
{

   public boolean hasNext() throws Exception;

   public PrimitiveValueWrapper[] next() throws Exception;

   public void reset() throws Exception;

}
