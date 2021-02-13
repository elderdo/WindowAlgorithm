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
   $Revision:   1.22  $
       $Date:   27 Mar 2009 16:42:08  $
   $Workfile:   LpOverride.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\LpOverride.java.-arc  $
/*
/*   Rev 1.22   27 Mar 2009 16:42:08   zf297a
/*Fixed writing of override_user to the flat  file, when it is null write out SPO otherwise write the bems_id.  SPO uses exists with an ID of zero, which is what lp_override.override_user gets set to when a user gets deleted from the system.
/*
/*   Rev 1.21   02 Dec 2008 11:38:56   zf297a
/*Fixed F1 (spo view) and F2 (amd x_ view) comments.  Removed s variable that was not being used.
/*
/*   Rev 1.20   21 Nov 2008 11:02:02   zf297a
/*Eliminated the superfulous boolean variable firstDiff and all associated code.
/*Removed display of buff load times and time to write flat file.
/*Fixed literal for the F1, master, view.
/*
/*   Rev 1.19   21 Nov 2008 10:39:26   zf297a
/*Formated numbers in warning message.
/*
/*   Rev 1.18   21 Nov 2008 09:00:30   zf297a
/*Use SpoParameter to get the MAX_OVERRIDE_VALUE.  Report when the quantity exceeds the MAX_OVERRIDE_VALUE , but change it to MAX_OVERRIDE_VALUE so the load will continue.
/*
/*   Rev 1.17   10 Jul 2008 08:38:58   zf297a
/*Fixed handling of exceptions for the FileOutputStream: issue exception message and exit with a retun code of 4.
/*
/*   Rev 1.16   01 May 2008 12:11:34   zf297a
/*Removed commented code which is just too much clutter.  Made spoc17v2 the default instance for SPO.  Fixed the compareTo and hashCode methods.
/*
/*   Rev 1.15   Apr 21 2008 16:25:44   vx917c
/*Records and displays the time taken to set up the buffers for SQL table (Read + set up buffer)
/*
/*   Rev 1.14   Apr 11 2008 14:52:40   vx917c
/*removed the checking of a '0' in front of override user.  The extra 0s have been taken care of in the views that form the table.
/*
/*   Rev 1.13   Apr 07 2008 16:30:26   vx917c
/*removed much of the code that is no longer relevent, and commented out the direct SQL updates that is currently not being used (but could be used in another time if we choose to).
/*
/*   Rev 1.12   Apr 04 2008 15:57:06   vx917c
/*commented out most SQL operations.  Changed quantity type from double to int.  Added quotes around action.
/*
/*   Rev 1.11   Mar 28 2008 10:05:28   vx917c
/*removed the second diff with TMP_A2A, changed a few SQL queries.
/*
/*   Rev 1.10   Mar 19 2008 11:56:34   vx917c
/*moved the filename for diff between TMP_A2A and Diff of (X_LP vs V_LP) to properties
/*
/*   Rev 1.8   Feb 08 2008 13:59:30   vx917c
/*added support to get batch numbers from SPOBatch.  Added more testing parameters such as number of rows selected (for shorter tests)
/*
/*   Rev 1.6   Feb 04 2008 11:09:14   vx917c
/*added better timing reporting mechanisms to determine the time taken for SQL transaction and flatfile creation.  
/*
/*   Rev 1.5   Jan 31 2008 14:05:38   vx917c
/*renamed the function that does the batch SQL so it makes more sense.  Currnetly disabled batch inserts since we're testing the flat file portion.
/*
/*   Rev 1.1   Oct 19 2007 11:32:22   c970984
/*new
/*
/*   Rev 1.0   15 Jun 2007 11:53:14   zf297a
/*Initial revision.

Rev 1.1   24 Sep 2002 07:47:38   c970984
Use SPO Connection and functions provided by SPO: xm_imp, etc.

	*/

public class LpOverride implements Rec {
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
    static String F1Instance = "spoc17v2";
    static String F1InstanceSQL = F1Instance + ".";
    static String F2Instance = "";
    static String F2InstanceSQL = "";

    static Logger logger = Logger.getLogger(LpOverride.class.getName());

    static FileOutputStream o1;
    static PrintStream p; 
    static long batchNum;
    static int maxOverrideValue = SpoParameter.getValue("MAX_OVERRIDE_VALUE") ;
    static int recsOverriden  = 0 ;

    static PreparedStatement pstmt;
    static long transTime = 0;        // keeps track of time spent in transactions
    static long flatFileTime = 0;     // keeps track of time spent writing flatfiles
    static String flatFileName;       // name of flat file to output   


    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // v_lp_override (spo)
    private static TableSnapshot F2 = null ; // x_lp_override_v (amd)

    // loads the parameter from this class's properties file if they exist
    static private void loadParams() {
	try {

		java.util.Properties p        = new AppProperties(LpOverride.class.getName()).getProperties() ;

		useTestBatch = p.getProperty("useTestBatch", "false").equals("true");
		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","200") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","10") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","10") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","200") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","200") ).intValue() ;
		flatFileName = String.valueOf( p.getProperty("dumpFile", "LpOverride.csv"));
		F1Instance = String.valueOf( p.getProperty("F1Instance", "spoc17v2"));
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
	} catch (java.lang.Exception e) {
		System.err.println("Warning: " + e.getMessage()) ;
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
	    batchNum = SpoBatch.createBatch("X_IMP_LP_OVERRIDE") ;			
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
            long readStart = System.currentTimeMillis();
	    F1 = new TableSnapshot(bufSize, new LpOverrideFactory("SELECT * FROM " + F1InstanceSQL + 
	    "V_LP_OVERRIDE LPO WHERE " + 
	    "EXISTS (SELECT NULL FROM " + F1InstanceSQL + "V_LP_OVERRIDE WHERE LOCATION " +
	    "= LPO.LOCATION AND PART = LPO.PART AND OVERRIDE_TYPE IN ('TSL Fixed', 'ROP Fixed', 'ROQ Fixed') " +
	    "GROUP BY LOCATION, PART HAVING SUM(QUANTITY) <> 0) " + 
	    "ORDER BY PART, LOCATION, OVERRIDE_TYPE", spo, prefetchSize)) ;
            F2 = new TableSnapshot(ageBufSize, new LpOverrideFactory("SELECT * FROM " + F2InstanceSQL + 
	    "x_lp_override_v LPO WHERE " + 
            "EXISTS (SELECT NULL FROM " + F2InstanceSQL + "X_LP_OVERRIDE_V WHERE LOCATION = LPO.LOCATION " +
	    "AND PART = LPO.PART AND OVERRIDE_TYPE IN ('TSL Fixed', 'ROP Fixed', 'ROQ Fixed') GROUP BY LOCATION, " +
	    "PART HAVING SUM(QUANTITY) <> 0) " + 
	    "ORDER BY PART, LOCATION, OVERRIDE_TYPE ", amd, prefetchSize)) ;
	    long readTime = System.currentTimeMillis() - readStart;
            w.diff(F1, F2) ;
	    p.close();
	    long batchStart = System.currentTimeMillis();
	    transTime = transTime + (System.currentTimeMillis() - batchStart);

        }
        catch (SQLException e) {
            System.out.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;	 
	    p.close();   
        }
        catch (ClassNotFoundException e) {
            System.out.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;	    
	    p.close();
        }

	finally {
            updateCounts() ;

        }
    }

    private static void updateCounts() {

	if (F1 != null) {
	   	System.out.println("v_lp_override_v=" + F1.getRecsIn()) ;
        	logger.info("v_lp_override_v=" + F1.getRecsIn()) ;
	}
	if (F2 != null) {
	        System.out.println("x_lp_override_v=" + F2.getRecsIn()) ;
        	logger.info("x_lp_override_v=" + F2.getRecsIn()) ;
	}
  
        System.out.println("rows inserted=" + rowsInserted) ;
        logger.info("rows inserted=" + rowsInserted) ;
        System.out.println("rows updated=" + rowsUpdated) ;
        logger.info("rows updated=" + rowsUpdated) ;
        System.out.println("rows deleted=" + rowsDeleted) ;
        logger.info("rows deleted=" + rowsDeleted) ;
	if (recsOverriden > 0) {
        	System.out.println("rows with overriden quantities=" + recsOverriden) ;
        	logger.info("rows with overriden quantities =" + recsOverriden) ;
	}

        System.out.println("end time: " + now()) ;

    }

    class Key implements Comparable {
        String      part_no ;
        String      location ;
        String      override_type;

        public boolean equals(Object o) {
            Key k = (LpOverride.Key) o ;
            return (k.part_no.equals(part_no) && k.location.equals(location) && k.override_type.equals(override_type));
        }
        public int hashCode() {
            return part_no.hashCode() + location.hashCode() + override_type.hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (part_no + location + override_type).compareTo(theKey.part_no + location + override_type) ;
        }
        public String toString() {
          return "part_no=" + part_no + " location=" + location + " override_type=" + override_type ;
        }
    }
    Key key ;
    Key keyToCompare;
    class Body {
        String      override_user ;
        String      override_reason ;
        int         quantity ;
        boolean     quantity_isnull ;
        java.sql.Date begin_date ;
        java.sql.Date end_date ;

        public String toString() {
            return "override_user=" + override_user + " " +
                   "override_reason=" + override_reason + " " +
                   "quantity=" + quantity ;
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
            result = b.override_user.equals(override_user);
            showDiff(result, "override_user", b.override_user + "", override_user + "") ;
            result = result && equal(b.quantity, b.quantity_isnull, quantity, quantity_isnull) ;
            showDiff(result, "quantity", b.quantity + "", quantity + "") ;
            return result ;
        }
    }
    Body body ;
    Body bodyToCompare;

    LpOverride(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.part_no = r.getString("PART") ;
        key.location = r.getString("LOCATION") ;
        key.override_type = r.getString("OVERRIDE_TYPE") ;

        body = new Body() ;
        body.override_user = r.getString("OVERRIDE_USER") ;
        body.override_reason = r.getString("OVERRIDE_REASON") ;
        body.quantity = r.getInt("QUANTITY") ;
        body.quantity_isnull = r.wasNull() ;
        body.begin_date = r.getDate("BEGIN_DATE") ;
        body.end_date = r.getDate("END_DATE") ;
        }
        catch (java.sql.SQLException e) {
            updateCounts() ;
            System.out.println(e.getMessage() ) ;
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
            System.out.println(e.getMessage()) ;
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
            System.out.println(e.getMessage()) ;
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
            System.out.println(e.getMessage()) ;
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

        rowsDeleted++ ;
	if (logger.isDebugEnabled() && rowsDeleted % frequency == 0) {
		logger.debug("rowsDeleted = " + rowsDeleted +  "oldMaster recsIn=" + F1.getRecsIn() ) ;
	}
	
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


	public void writeFlatFile(String action)
	{
		long flatStart = System.currentTimeMillis();
		if (body.quantity > maxOverrideValue) {
			java.text.NumberFormat numberFormatter = java.text.NumberFormat.getNumberInstance() ;
			System.out.println("Warning: part " + key.part_no 
					+ " at location " + key.location 
					+ " has a quantity that is too big: " 
					+ numberFormatter.format(body.quantity) + " value has been changed to " 
					+ numberFormatter.format(maxOverrideValue)) ;
			body.quantity = maxOverrideValue ;
			recsOverriden++ ;
		}
		p.println("\"" + key.part_no + "\", \"" 
				+ key.location + "\", \"" 
				+ key.override_type + "\", " 
				+ ((body.override_user == null) ? "SPO" : "\"" + body.override_user + "\"") + ", " 
				+ body.quantity + ", \"" 
				+ body.override_reason + "\", " 
				+ convertDate(body.begin_date) + ", " 
				+ convertDate(body.end_date) + ", \"" 
				+ action + "\", " 
				+ batchNum);
		flatFileTime = flatFileTime + (System.currentTimeMillis() - flatStart);
	}

}
