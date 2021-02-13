import java.sql.* ;
import java.math.BigDecimal ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;
import java.util.Properties ;
import java.io.FileInputStream ;
import java.io.*;



/*   $Author:   zf297a  $
   $Revision:   1.3  $
       $Date:   02 Dec 2008 11:36:14  $
   $Workfile:   LpOnHand.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\LpOnHand.java.-arc  $
/*
/*   Rev 1.3   02 Dec 2008 11:36:14   zf297a
/*Fixed F1 (spo view) and F2 (amd x_ view) comments.  Removed s variable that was not being used.
/*
/*   Rev 1.2   10 Jul 2008 08:38:58   zf297a
/*Fixed handling of exceptions for the FileOutputStream: issue exception message and exit with a retun code of 4.
/*
/*   Rev 1.1   13 May 2008 21:51:40   zf297a
/*Fixed table names and diagnostic code by adding printStackTrace.
/*
/*   Rev 1.0   05 May 2008 11:45:20   zf297a
/*Initial revision.

	*/

public class LpOnHand implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static SpoConnection spo = SpoConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;
    static int frequency = 100 ;

    static boolean debug ;
    static boolean useTestBatch = false;
    static boolean no_op    = false;
    static int bufSize      = 500000 ;
    static int ageBufSize   = 500000 ;
    static int prefetchSize = 200 ;
    static int debugThreshold = 10 ;
    static int showDiffThreshold = 500 ;
    static String F1Instance = "SPOC17V2";
    static String F1InstanceSQL = F1Instance + ".";
    static String F2Instance = "";
    static String F2InstanceSQL = "";

    static Logger logger = Logger.getLogger(LpOnHand.class.getName());

    static FileOutputStream o1;
    static PrintStream p; // declare a print stream object  
    static long batchNum;
    static PreparedStatement pstmt;
    static long transTime = 0;        // keeps track of time spent in transactions
    static long flatFileTime = 0;     // keeps track of time spent writing flatfiles
    static String flatFileName;       // name of flat file to output   


    private static TableSnapshot F1 = null ; // v_lp_on_hand (spo)
    private static TableSnapshot F2 = null ; // x_lp_on_hand_v (amd)

    // loads the parameter from this class's properties file if they exist
    static private void loadParams() {
	try {

		java.util.Properties p        = new AppProperties(LpOnHand.class.getName()).getProperties() ;

		useTestBatch = p.getProperty("useTestBatch", "false").equals("true");
		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","200") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","10") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","10") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","200") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","200") ).intValue() ;
		flatFileName = String.valueOf( p.getProperty("dumpFile", "LpOnHand.csv"));
		F1Instance = String.valueOf( p.getProperty("F1Instance", "SPOC17V2"));
		F2Instance = String.valueOf( p.getProperty("F2Instance", ""));
		logger.debug("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" + prefetchSize +
		" no_op=" + no_op + " debug=" + debug + " debugThreshold=" + debugThreshold + " showDiffThreshold=" +
	       	showDiffThreshold) ;

		if (debug) {
			System.out.println("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" +
			prefetchSize + " no_op=" + no_op + " debugThreshold=" + debugThreshold + " showDiffThreshold=" + 
			showDiffThreshold) ;
		}

	} catch (java.io.IOException e) {
		System.err.println("Warning: " + e.getMessage()) ;
		e.printStackTrace() ;
	} catch (java.lang.Exception e) {
		System.err.println("Warning: " + e.getMessage()) ;
		e.printStackTrace() ;
	}
    }


    public static void main(String[] args) {
        loadParams();
	// adds an appropraite . after the instance if the instance is not empty to be used in SQL statement
	if (F1Instance == "") {
		F1InstanceSQL = "";
	}
	else {
		F1InstanceSQL = F1Instance + ".";
	}
	if (F2Instance == "") {
		F2InstanceSQL = "";
	}
	else {
		F2InstanceSQL = F1Instance + ".";
	}


	if (useTestBatch) {
		batchNum = 9999;
	}
	else	{
	    SpoBatch.setOracleInstance(F1Instance);
	    batchNum = SpoBatch.createBatch("X_IMP_LP_ON_HAND") ;			
	}


	System.out.println("start time: " + now()) ;
        if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("-d")) {
                    debug = true ;
                }
            }
        }

	try {
		o1 = new FileOutputStream(flatFileName);
		System.out.println("flatfile: " + flatFileName);
		p = new PrintStream( o1 );
	} catch (java.io.FileNotFoundException e) {
		System.err.println(e.getMessage()) ;
		e.printStackTrace() ;
		System.exit(4) ;
	} catch (java.lang.SecurityException e) {
		System.err.println(e.getMessage()) ;
		e.printStackTrace() ;
		System.exit(4) ;
	} catch (Exception e) {
		System.err.println(e.getMessage()) ;
		e.printStackTrace() ;
		System.exit(4) ;
	}

        WindowAlgo w = new WindowAlgo(/* input buf */ bufSize,
            /* aging buffer */ ageBufSize) ;

        w.setDebug(debug) ;
        try {  
// keeps track of how long it took to read the 2 tables and form the arrays
            long readStart = System.currentTimeMillis();
	    F1 = new TableSnapshot(bufSize, new LpOnHandFactory("SELECT * FROM " + F1InstanceSQL + 
	    "V_LP_ON_HAND order by location, part, on_hand_type", spo, prefetchSize)) ;
	    if (F1 == null) {
		    System.out.println("F1: the old master was not set") ;
		    System.out.println("SELECT * FROM " + F1InstanceSQL + 
			    "V_LP_ON_HAND order by location, part, on_hand_type") ;
		    System.exit(4) ;
	    }
            F2 = new TableSnapshot(ageBufSize, new LpOnHandFactory("SELECT * FROM " + F2InstanceSQL + 
	    "x_lp_on_hand_v order by location, part, on_hand_type" , amd, prefetchSize)) ;
	    if (F2 == null) {
		    System.out.println("F2: the new master was not set") ;
		    System.out.println("SELECT * FROM " + F2InstanceSQL + 
	    "x_lp_on_hand_v order by location, part, on_hand_type" ) ;
		    System.exit(4) ;
	    }

	    logger.debug("F1 & F2 set") ;
	    long readTime = System.currentTimeMillis() - readStart;
	    logger.debug("start w.diff") ;
            w.diff(F1, F2) ;
	    logger.debug("end w.diff") ;
	    p.close();
	    long batchStart = System.currentTimeMillis();
	    transTime = transTime + (System.currentTimeMillis() - batchStart);

	    System.out.println("time to write flat file:" + flatFileTime/1000 + "seconds");
	    System.out.println("time to read SQL and put in buffer:" + readTime/1000 + "seconds");
        }
        catch (SQLException e) {
            System.err.println(e.getMessage()) ;
	    e.printStackTrace() ;
            logger.error(e.getMessage()) ;	 
	    p.close();   
            System.exit(4) ;
        }
        catch (ClassNotFoundException e) {
            System.err.println(e.getMessage()) ;
	    e.printStackTrace() ;
            logger.error(e.getMessage()) ;	    
	    p.close();
            System.exit(4) ;
        }
	catch (java.lang.NullPointerException e) {
		if (e != null) {
			e.printStackTrace() ;		
			if (e.getMessage() != null) {
				System.err.println(e.getMessage()) ;		
			}
		}
		System.exit(4) ;
	}

	finally {
            updateCounts() ;

        }
    }

    private static void updateCounts() {

	if (F1 != null) {
	   	System.out.println("v_lp_on_hand=" + F1.getRecsIn()) ;
        	logger.info("v_lp_on_hand=" + F1.getRecsIn()) ;
	}
	if (F2 != null) {
	        System.out.println("x_lp_on_hand_v=" + F2.getRecsIn()) ;
        	logger.info("x_lp_on_hand_v=" + F2.getRecsIn()) ;
	}
  
        System.out.println("rows inserted=" + rowsInserted) ;
        logger.info("rows inserted=" + rowsInserted) ;
        System.out.println("rows updated=" + rowsUpdated) ;
        logger.info("rows updated=" + rowsUpdated) ;
        System.out.println("rows deleted=" + rowsDeleted) ;
        logger.info("rows deleted=" + rowsDeleted) ;

        System.out.println("end time: " + now()) ;

    }

    class Key implements Comparable {
        String      part_no ;
        String      location ;
        String      on_hand_type ;

        public boolean equals(Object o) {
            Key k = (LpOnHand.Key) o ;
            return (k.part_no.equals(part_no) && k.location.equals(location) && k.on_hand_type.equals(on_hand_type) );
        }
        public int hashCode() {
            return part_no.hashCode() + location.hashCode() + on_hand_type.hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (theKey.part_no + theKey.location + theKey.on_hand_type).compareTo(part_no + location + on_hand_type) ;
        }
        public String toString() {
          return "part_no=" + part_no + " location=" + location + " on_hand_type=" + on_hand_type  ;
        }
    }
    Key key ;
    Key keyToCompare;
    class Body {
        int         quantity ;

        public String toString() {
            return "quantity=" + quantity ;
 }
        boolean equal(String s1, String s2) {
            if (s1 != null)
                if (s2 != null)
                    return s1.equals(s2) ;
                else
                    return false ;
            else if (s2 == null)
                return true ; // both null
            else
                return false ; // s1 == null && s2 != null

        }
        boolean equal(int i1, boolean null1, int i2, boolean null2) {
            if (!null1)
                if (!null2)
                    return i1 == i2 ;
                else
                    return false ;
            else if (null2)
                return true ; // both null
            else
                return false ; // i1 == null && i2 != null
        }
        boolean equal(double d1, boolean null1, double d2, boolean null2) {
            if (!null1)
                if (!null2)
                    return  new Double(d1).compareTo(new Double(d2)) == 0  ;
                else
                    return false ;
            else if (null2)
                return true ; // both null
            else
                return false ; // i1 == null && i2 != null
        }
        boolean equal(BigDecimal d1, boolean null1, BigDecimal d2, boolean null2) {
            if (!null1)
                if (!null2)
                    return d1.compareTo(d2) == 0 ;
                else
                    return false ;
            else if (null2)
                return true ; // both null
            else
                return false ; // i1 == null && i2 != null
        }
        boolean equal(float f1, boolean null1, float f2, boolean null2) {
            if (!null1)
                if (!null2)
                    return f1 == f2 ;
                else
                    return false ;
            else if (null2)
                return true ; // both null
            else
                return false ; // i1 == null && i2 != null
        }
        boolean equal(java.sql.Date date1, boolean null1, java.sql.Date date2, boolean null2) {
            if (!null1)
                if (!null2)
                    return date1.compareTo(date2) == 0 ;
                else
                    return false ;
            else if (null2)
                return true ; // both null
            else
                return false ; // i1 == null && i2 != null
        }
        int showDiffCnt = 0 ;
        private void showDiff(boolean result, String fieldName, String field1, String field2) {
                if (!result) {
		    showDiffCnt++ ;
		    if (showDiffCnt % showDiffThreshold == 0) {
                    	logger.debug("key = " + key + " field: " + fieldName + " *" + field1 + "* *" + field2 + "*") ;
		    }
                }
        }

        public boolean equals(Object o) {
            Body b = (Body) o ;
            boolean result ;
            result = (b.quantity == quantity) ;
            showDiff(result, "quantity", b.quantity + "", quantity + "") ;
            return result ;
        }
    }
    Body body ;
    Body bodyToCompare;

    LpOnHand(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.part_no = r.getString("PART") ;
        key.location = r.getString("LOCATION") ;
        key.on_hand_type = r.getString("ON_HAND_TYPE") ;

        body = new Body() ;
        body.quantity = r.getInt("QUANTITY") ;
        }
        catch (java.sql.SQLException e) {
            updateCounts() ;
            System.err.println(e.getMessage() ) ;
	    e.printStackTrace() ;
            logger.fatal(e.getMessage()) ;
            System.exit(4) ;
        }
    }

    public Comparable  getKey() {
        return  key ;
    }
    public Object getBody() {
        return body ;
    }

    public boolean  keysEqual(Rec r) {
        boolean result = key.equals(r.getKey()) ;
        return (key.equals( r.getKey() )) ;
    }
    public boolean  bodiesEqual(Rec r) {
        return (body.equals(r.getBody())) ;
    }
    private void setDouble(CallableStatement cstmt,
            int paramaterIndex, double value, boolean isNull) {
        try {
            if (isNull) {
                cstmt.setNull(paramaterIndex, java.sql.Types.DOUBLE) ;
            }
            else {
                cstmt.setDouble(paramaterIndex, value) ;
            }
        } catch (java.sql.SQLException e) {
            updateCounts() ;
            System.err.println(e.getMessage()) ;
	    e.printStackTrace() ;
            logger.fatal(e.getMessage()) ;
            System.exit(4) ;
        }
    }

    private void setBigDecimal(CallableStatement cstmt,
            int paramaterIndex, BigDecimal value, boolean isNull) {
        try {
            if (isNull) {
                cstmt.setNull(paramaterIndex, java.sql.Types.DOUBLE) ;
            }
            else {
                cstmt.setBigDecimal(paramaterIndex, value) ;
            }
        } catch (java.sql.SQLException e) {

            updateCounts() ;
            System.err.println(e.getMessage()) ;
	    e.printStackTrace() ;
            logger.fatal(e.getMessage()) ;
            System.exit(4) ;
        }
    }
    private void setInt(CallableStatement cstmt,
            int paramaterIndex, int value, boolean isNull) {
        try {
            if (isNull)
                cstmt.setNull(paramaterIndex, java.sql.Types.NUMERIC) ;
            else
                cstmt.setInt(paramaterIndex, value) ;
        } catch (java.sql.SQLException e) {
            updateCounts() ;
            System.err.println(e.getMessage()) ;
	    e.printStackTrace() ;
            logger.fatal(e.getMessage()) ;
            System.exit(4) ;
        }
    }

    public void     insert() {
	writeFlatFile("INS");

		rowsInserted++ ;
		if (logger.isDebugEnabled() && rowsInserted % frequency == 0) {
			logger.info("rowsInserted = " + rowsInserted +  "newMaster recsIn=" + F2.getRecsIn() ) ;
		}
    }

    public void     update() {
        writeFlatFile("UPD");
	
		rowsUpdated++ ;
		if (logger.isDebugEnabled() && rowsUpdated % frequency == 0) {
			logger.debug("rowsUpdated = " + rowsUpdated +  " recsIn=" + F1.getRecsIn() ) ;
		}
	
    }
    public void     delete() {

        writeFlatFile("DEL");
//	sequenceCounter++;

		
	        rowsDeleted++ ;
		if (logger.isDebugEnabled() && rowsDeleted % frequency == 0) {
			logger.debug("rowsDeleted = " + rowsDeleted +  "oldMaster recsIn=" + F1.getRecsIn() ) ;
		}
	
//	else {
//		r2Del++;
//	}
	
    }

	public static String now() {
		Calendar cal = Calendar.getInstance(TimeZone.getDefault());
		String DATE_FORMAT = "M/dd/yy hh:mm:ss a";
		java.text.SimpleDateFormat sdf =
				new java.text.SimpleDateFormat(DATE_FORMAT);
		sdf.setTimeZone(TimeZone.getDefault());
		return sdf.format(cal.getTime());
	}

	public static String convertDate(Date dateToConvert) {
		if (dateToConvert == null) {
			return "";
		}

		Calendar cal = Calendar.getInstance(TimeZone.getDefault());
		String DATE_FORMAT = "dd-MMM-yy";
		java.text.SimpleDateFormat sdf =
				new java.text.SimpleDateFormat(DATE_FORMAT);
		sdf.setTimeZone(TimeZone.getDefault());
		return sdf.format(dateToConvert);
	}



	// writes to flat file to indicate the SQL transaction needed
	public void writeFlatFile(String action)
	{
		long flatStart = System.currentTimeMillis();
			p.println("\"" + key.part_no + "\", \"" + key.location + "\", \"" + 
			key.on_hand_type + "\", " + body.quantity 
		       	 + ", \"" + action + "\", " + batchNum);
			flatFileTime = flatFileTime + (System.currentTimeMillis() - flatStart);
	}

}
