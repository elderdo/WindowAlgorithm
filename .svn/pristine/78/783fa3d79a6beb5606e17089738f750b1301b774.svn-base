import java.util.Date ;
import java.util.ListIterator ;
import org.apache.log4j.Logger ;

public class WindowAlgo
{
    int debugThreshold = 50000 ;
    int loopThreshold = 100 ;
    int agingBufSize = 20000 ;
    boolean debug = false ;

    static Logger logger = Logger.getLogger(WindowAlgo.class.getName());

    AgingBuffer agb1 ;
    AgingBuffer agb2 ;
    InputBuffer b1 = null ;
    InputBuffer b2 = null ;
    int inputBufferSize = 73 ;

    private void getProperties() {
	try {

		java.util.Properties p = 
			new AppProperties(WindowAlgo.class.getName()).getProperties() ;

       		debug = p.getProperty("debug","false").equals("true") ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","50000") ).intValue() ;
		agingBufSize = Integer.valueOf( p.getProperty("ageBufSize","20000") ).intValue() ;
		loopThreshold = Integer.valueOf( p.getProperty("loopThreshold","100") ).intValue() ;

	} catch (java.io.IOException e) {
		System.err.println("WindowAlgo: warning: " + e.getMessage()) ;
	} catch (java.lang.Exception e) {
		System.err.println("WindowAlgo: warning: " + e.getMessage()) ;
	}
    }

    public WindowAlgo() {
	getProperties() ;
    	agb1 = new DeleteAgingBuffer(agingBufSize) ;
    	agb2 = new InsertAgingBuffer(agingBufSize) ;
    }

    public WindowAlgo(int maxBufSize, int maxAgingBufSize) {
	getProperties() ;
        inputBufferSize = maxBufSize ;
	logger.debug("inputBufferSize=" + inputBufferSize) ;
	// argument takes priority over property file
        agingBufSize = maxAgingBufSize ;
	logger.debug("agingBufSize=" + agingBufSize) ;
    	agb1 = new DeleteAgingBuffer(agingBufSize) ;
    	agb2 = new InsertAgingBuffer(agingBufSize) ;
    }
    public void setDebug(boolean value) {
        debug = value ;
    }
    public boolean getDebug() {
        return debug ;
    }
    public void diff(Snapshot F1, Snapshot F2) {
	long start = System.currentTimeMillis() ;
        logger.debug("diff: F1=" + F1 + " F2=" + F2 ) ;
        logger.debug("diff: Getting F1 buffer.") ;
        b1 = F1.getBlocks(inputBufferSize) ;
        logger.debug("diff: b1 input buffer created: b1 == null " + (b1 == null)) ;
        logger.debug("diff: Getting F2 buffer.") ;
        b2 = F2.getBlocks(inputBufferSize) ;
        logger.debug("diff: b2 input buffer created: b2 == null " + (b2 == null)) ;

        boolean showOne = false ;

	long wStart = System.currentTimeMillis() ;
	int loopCnt = 0 ;
	while (b1 != null && b2 != null) {
            System.out.flush() ;
            match(b1,b2) ;
            matchBuffer1AgingBuffer2() ;
            matchBuffer2AgingBuffer1() ;
            add(b1,agb1) ;
            add(b2,agb2) ;
            b1 = F1.getBlocks(inputBufferSize) ;
            b2 = F2.getBlocks(inputBufferSize) ;
	    loopCnt++ ;
	    if (b1 == null) {
	    	logger.debug("diff: input buffer 1 (old master) is empty") ;
	    }		
	    if (b2 == null) {
	    	logger.debug("diff: input buffer 2 (new master) is empty") ;
	    }
	    if (loopCnt % loopThreshold == 0) {
		    logger.debug("diff: looped through buffers " + loopCnt + " times.") ;
	    }
        }
	long wElapsed = System.currentTimeMillis() - wStart ;
	logger.debug("diff: main while loop cnt " + loopCnt + " elapsed time: " + wElapsed / 1000.0) ;
	//
        // Empty aging buffers, input buffers, and snapshot
	long emptyStart = System.currentTimeMillis() ;
	if (b1 != null && agb2 != null && b1.size() > 0 && agb2.size() > 0) {
       		matchBuffer1AgingBuffer2() ;
	}
	if (b2 != null && agb1 != null && b2.size() > 0 && agb1.size() > 0) {
	        matchBuffer2AgingBuffer1() ;
	}
        if (agb1.size() > 0) {
	    logger.debug("reportDeletes aging buffer 1") ;
            reportDeletes(agb1) ;
	}
        while (b1 != null) {
	    logger.debug("reportDeletes input buffer 1") ;
            reportDeletes(b1) ;
            b1 = F1.getBlocks(inputBufferSize) ;
        }
        if (agb2.size() > 0) {
	    logger.debug("reportInserts aging buffer 2") ;
            reportInserts(agb2) ;
	}
        while (b2 != null) {
	    logger.debug("reportInserts input buffer 2") ;
            reportInserts(b2) ;
            b2 = F2.getBlocks(inputBufferSize) ;
        }
	long emptyElapsed = System.currentTimeMillis() - emptyStart ;
	logger.debug("diff: empty buffers elapsed time: " + emptyElapsed / 1000.0) ;
        F2.save() ;
	long elapsed = System.currentTimeMillis() - start ;
	logger.debug("diff elapsed time: " + elapsed / 1000.0) ;
    }

    void add(InputBuffer buf, AgingBuffer ab) {
	int cnt = 0 ;
	for (java.util.Enumeration bufKeys = buf.keys(); bufKeys.hasMoreElements();) {
	    Object theKey = bufKeys.nextElement() ;
            ab.add((Rec) buf.get(theKey)) ;
            buf.remove(theKey) ;
	    cnt++ ;
	    if (cnt % debugThreshold == 0) {
		logger.debug("add from input buf to aging buffer processed: " + cnt) ;
	    }
        }
    }

    void reportDeletes(java.util.Hashtable b) {
	int cnt = 0 ;
	long start = System.currentTimeMillis() ;
	logger.debug("reportDeletes for buffer: b.size()=" + b.size()) ;
	for (java.util.Enumeration bufElements = b.elements() ; bufElements.hasMoreElements();) {
		Rec r = (Rec) bufElements.nextElement() ;
                r.delete() ;
		cnt++ ;
		if (cnt % debugThreshold == 0) {
			logger.debug("reportDeletes for buffer processed: " + cnt) ;
		}
        }
	b.clear() ;
	long elapsed = System.currentTimeMillis() - start ;
	logger.debug("reportDeletes for buffer: elapsed time=" + elapsed / 1000.0) ;
    }


    void reportInserts(java.util.Hashtable b) {
	int cnt = 0  ;
	long start = System.currentTimeMillis() ;
	logger.debug("reportInserts for buffer: b.size()=" + b.size()) ;
        for (java.util.Enumeration bufElements = b.elements(); bufElements.hasMoreElements();) {
	    Rec r = (Rec) bufElements.nextElement() ;
            r.insert() ;
	    cnt++ ;
	    if (cnt % debugThreshold == 0) {
		logger.debug("reportInserts for buffer processed: " + cnt) ;
	    }
        }
	b.clear() ;
	long elapsed = System.currentTimeMillis() - start ;
	logger.debug("reportInserts for buffer: elapsed time=" + elapsed / 1000.0) ;
    }


    void matchBuffer1AgingBuffer2() {
	long start = System.currentTimeMillis() ;
	int cnt = 0 ;
	int updateCnt = 0 ;
	int hits = 0 ;
	logger.debug("matchBuffer1AgingBuffer2 b1.size=" + b1.size() + " agb2.size=" + agb2.size()) ;
	for (java.util.Enumeration bufKeys = b1.keys() ; bufKeys.hasMoreElements();) {
	    Object theKey = bufKeys.nextElement() ;
            Rec r1 = (Rec) b1.get(theKey) ;
            Rec r2 = (Rec) agb2.get(r1.getKey());
            if (r2 != null) {
		hits++ ;
                b1.remove(theKey) ;
                agb2.remove(r2) ;
                if (!r2.bodiesEqual(r1)) {
			logger.debug("r1.key=" + r1.getKey() + " r1.body=" + r1.getBody() ) ;
			logger.debug("updating r2.key=" + r2.getKey() + " r2.body=" + r2.getBody() ) ;
                    	r2.update() ;
			updateCnt++ ;
		}
            }
	    cnt++ ;
	    if (cnt % debugThreshold == 0) {
	    	logger.debug("matchBuffer1AgingBuffer2 processed " + cnt + " records.") ;
	    }
        }
	long elapsed = System.currentTimeMillis() - start ;
	logger.debug("matchBuffer1AgingBuffer2 b1.size=" + b1.size() + " agb2.size=" + agb2.size()) ; 
	logger.debug( "matchBuffer1AgingBuffer2 hits: " + hits + " updates: " + updateCnt 
			+ " elapsed time: " + elapsed / 1000.0) ;

    }

    void matchBuffer2AgingBuffer1() {
	long start = System.currentTimeMillis() ;
	int cnt = 0 ;
	int updateCnt = 0 ;
	int hits = 0 ;
	logger.debug("matchBuffer2AgingBuffer1 b2.size=" + b2.size() + " agb1.size=" + agb1.size()) ;
	for (java.util.Enumeration bufKeys = b2.keys() ; bufKeys.hasMoreElements();) {
	    Object theKey = bufKeys.nextElement() ;
            Rec r2 = (Rec) b2.get(theKey) ;
            Rec r1 = (Rec) agb1.get(r2.getKey());
            if (r1 != null) {
		hits++ ;
                b2.remove(theKey) ;
                agb1.remove(r2) ;
                if (!r2.bodiesEqual(r1)) {
			logger.debug("r1.key=" + r1.getKey() + " r1.body=" + r1.getBody() ) ;
			logger.debug("updating r2.key=" + r2.getKey() + " r2.body=" + r2.getBody() ) ;
                    	r2.update() ;
		}
            }
	    cnt++ ;
	    if (cnt % debugThreshold == 0) {
	    	logger.debug("matchBuffer2AgingBuffer1 processed " + cnt + " records.") ;
	    }
        }
	long elapsed = System.currentTimeMillis() - start ;
	logger.debug("matchBuffer2AgingBuffer1 b2.size=" + b2.size() + " agb1.size=" + agb1.size()) ; 
 	logger.debug("matchBuffer2AgingBuffer1 hits: " + hits + " updates: " + updateCnt 
			+ " elapsed time: " + elapsed / 1000.0) ;

    }

    void match(InputBuffer buf1, InputBuffer buf2) {
	long start = System.currentTimeMillis() ;
        logger.debug("match(" + buf1.size() + "," + buf2.size() + ")") ;
        Date d1 = new Date() ;
	int cnt = 0 ;
	int hits = 0 ;
	int updateCnt = 0 ;
        for (java.util.Enumeration b1Keys = buf1.keys(); b1Keys.hasMoreElements();) {
            if (cnt == 0 ) {
	    	logger.debug("start for loop") ;
	    }
	    cnt++ ;
	    if (cnt % debugThreshold == 0 ) {
		    logger.debug("processing rec # " + cnt + " of buf1") ;
	    }
	    Object rec1Key = b1Keys.nextElement() ;
	    Rec r2 = (Rec) buf2.get(rec1Key) ;
	    if (r2 != null) {
                Rec r1 = (Rec) buf1.get(rec1Key) ;
                if (r2.keysEqual(r1)) {
		    hits++ ;
                    b1.remove(rec1Key) ;
                    b2.remove(r2.getKey()) ;
                    if (r2.bodiesEqual(r1)) {
			if (hits % loopThreshold == 0) {
				logger.debug("noupdate: r1.key=" + r1.getKey()  ) ;
			}
                    } else {
			if (updateCnt % loopThreshold == 0) {
				logger.debug("update: r1.key=" + r1.getKey()  ) ;
			}
                        r2.update() ;
			updateCnt++ ;
                    }
                }
            }
        }
	long elapsed = System.currentTimeMillis() - start ;
        logger.debug("match(" + buf1.size() + "," + buf2.size() + ") processed " + cnt) ; 
	logger.debug("matched " + hits + " updates " + updateCnt + " in elapsed time: " + elapsed / 1000.0) ;
    }


}
