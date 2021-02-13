/*
 *   $Author:   zf297a  $
 * $Revision:   1.2  $
 *     $Date:   31 Jan 2008 12:20:08  $
 * $Workfile:   InsertAgingBuffer.java  $
 *      $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\InsertAgingBuffer.java-arc  $
   
      Rev 1.2   31 Jan 2008 12:20:08   zf297a
   Converted to using a Hashtable
   
      Rev 1.1   31 Oct 2007 09:43:34   zf297a
   Added PVCS keywords.  Make sure that a Rec object is returned whenever the remove method is invoked before trying to invoke the insert method of the Rec object.
 */
import org.apache.log4j.Logger ;

public class InsertAgingBuffer extends AgingBuffer
{
    	static Logger logger = Logger.getLogger(InsertAgingBuffer.class.getName());
	int initialSize ;

	public InsertAgingBuffer(int initialCapacity)
	{
		super(initialCapacity);
		initialSize = initialCapacity ;
	}
	public  void add(Rec r) {
	    if (super.size() % 10000 == 0) {
	    		logger.debug("add to insert buffer: " + super.size()) ;
	    }
	    super.add(r) ;	    
	}
}
