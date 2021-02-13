/*
 *   $Author:   zf297a  $
 * $Revision:   1.2  $
 *     $Date:   31 Jan 2008 12:14:18  $
 * $Workfile:   AgingBuffer.java  $
 *      $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\AgingBuffer.java-arc  $
   
      Rev 1.2   31 Jan 2008 12:14:18   zf297a
   Converted to Hashtable and added logger debug's.
   
      Rev 1.1   31 Oct 2007 09:37:16   zf297a
   Added PVCS keywords.  Made sure the full flag is set whenever the maximum size of the queue is exceeded versus checking when it is equal to the maximum size!
 */
import org.apache.log4j.Logger ;

public abstract class AgingBuffer extends java.util.Hashtable {

    	static Logger logger = Logger.getLogger(AgingBuffer.class.getName());

	protected boolean full;
	protected boolean reportOnce ;
	private int initialCapacity ;

	public AgingBuffer(int initialCapacity) {
	    super(initialCapacity) ;
	    this.initialCapacity = initialCapacity ;
	    logger.debug("initialCapacity=" + initialCapacity) ;
	}

    	public Object remove(){

		if(full) {
			full = false;
			reportOnce = false ;
		} else if(super.size() == 0 ) {
			return null;
		}

		/* remove the first key returned */
		java.util.Enumeration keys = super.keys() ;
		if (keys.hasMoreElements()) {
			return super.remove(keys.nextElement()) ;
		} else {
			return null ;
		}
	}

	public Object remove(Rec inRec) {

	    if (full) {
	        full = false ;
		reportOnce = false ;
	    } else if(super.size() == 0) {
	        return null ;
	    }

	    return super.remove(inRec.getKey()) ;
	}

	public void add(Rec r) {
	    super.put(r.getKey(),r) ;
	    if (super.size() > initialCapacity) {
		if (! reportOnce) {
			reportOnce = true ;
			System.err.println("Aging buffer size is greater than the initialCapacity of  " + initialCapacity) ;
		}
	    }
	}

}
