import org.apache.log4j.Logger ;

public class TableSnapshot implements Snapshot
{
    protected InputBuffer buf ;
    protected DBFactory factory ;
    private int recsIn = 0 ;
    static Logger logger = Logger.getLogger(TableSnapshot.class.getName());

    TableSnapshot(int maxBuf,DBFactory factory)  {
	logger.info("TableSnapshot maxBuf=: " + maxBuf) ;
        buf = new InputBuffer(maxBuf) ;
        this.factory = factory ;
	logger.info("TableSnapshot: InputBuffer created and factory linked") ;
    }

    public void save() {
    }

    public InputBuffer getBlocks(int n) {
        buf.clear() ; /* make sure the buffer is empty */
	long start = System.currentTimeMillis() ;
        logger.debug("getBlocks: loading buf with a size of " + n) ;
        logger.debug("for " + factory.getQuery()) ;

	try {
		for (int i = 0; (i < n && factory.moreRecsToMake()); i++) {
		    Rec theRec = factory.createRec() ;
		    buf.put(theRec.getKey(),theRec) ;
		    recsIn++ ;
		    if (recsIn % 10000 == 0) {
			logger.debug("processed " + recsIn) ;
		    }
		}
	} catch (NullPointerException e) {
		e.printStackTrace() ;
		logger.debug("processed " + recsIn) ;
	    	logger.debug("returning null buf") ;
		return null ;
	} catch (Exception e) {
		e.printStackTrace() ;
		if (e.getMessage() != null) {
			logger.debug(e.getMessage()) ;
		}
		logger.debug("processed " + recsIn) ;
	    	logger.debug("returning null buf") ;
             	return null ;
	}

	long elapsed = System.currentTimeMillis() - start ;
        logger.debug("buf loaded with " + buf.size() + " records, elapsed time " + elapsed / 1000.0) ;
        if (buf.size() > 0)
            return buf ;
        else {
	    logger.debug("returning null buf") ;
            return null ;
	}
    }

    public int getRecsIn() {
        return this.recsIn ;
    }
}
