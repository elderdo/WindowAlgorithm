import java.sql.* ;
import java.math.BigDecimal ;
import org.apache.log4j.Logger ;
import org.apache.log4j.Level ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

/*   $Author:   zf297a  $
   $Revision:   1.6  $
       $Date:   31 Jan 2008 12:18:14  $
   $Workfile:   LPOverrideConsumables.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\LPOverrideConsumables.java.-arc  $
/*
/*   Rev 1.6   31 Jan 2008 12:18:14   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.5   20 Dec 2007 00:26:48   zf297a
/*Use AppProperties to get parameters.  Activate debug via the properties file or command line.
/*
/*   Rev 1.4   19 Dec 2007 14:31:18   zf297a
/*Added some logger.debug statements to make it easier to trace problems.  Removed the deprecated method setIniFile and added check that the connection object was instantiated and issued an error message and aborted the applicaiton if it was not instantiated.
/*
/*   Rev 1.3   14 Dec 2007 21:51:06   zf297a
/*Removed debug System.out and changed System.out to System.err where errors are being reported.  In updateCounts checked for a valid F1 and F2.
/*
/*   Rev 1.2   14 Dec 2007 21:10:48   zf297a
/*Use the new LPOverrideConsumablesFactory class to construct F1 and F2.  Use a properties file that gets loaded via the classpath to get the size of the aging buffer, the input buffer, the prefetch value and whether to turn on debugging.
/*
/*   Rev 1.1   17 Aug 2007 13:32:44   zf297a
/*Made sure deleted records are not retrieved for the old master file - F1.  Move the logger.info before the stored procedure is invoked.
/*
/*   Rev 1.0   18 Jul 2007 16:38:40   zf297a
/*Initial revision.
        */

public class LPOverrideConsumables implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug    = false;
    static boolean no_op    = false;
    static int bufSize      = 500000 ;
    static int ageBufSize   = 500000 ;
    static int prefetchSize = 5000 ;
    static int debugThreshold = 50000 ;
    static int showDiffThreshold = 5000 ;


    static Logger logger = Logger.getLogger(LPOverrideConsumables.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_locpart_overid_consumables
    private static TableSnapshot F2 = null ; // tmp_locpart_overid_consumables

    static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(LPOverrideConsumables.class.getName()).getProperties() ;

       		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","500000") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","50000") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","5000") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","500000") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","5000") ).intValue() ;
		logger.debug("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" + prefetchSize + " no_op=" + no_op
				+ " debug=" + debug + " debugThreshold=" + debugThreshold + " showDiffThreshold=" + showDiffThreshold) ;

		if (debug) {
			System.out.println("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" + prefetchSize + " no_op=" + no_op + " debugThreshold=" + debugThreshold + " showDiffThreshold=" + showDiffThreshold) ;
		}

	} catch (java.io.IOException e) {
		System.err.println("Warning: " + e.getMessage()) ;
	} catch (java.lang.Exception e) {
		System.err.println("Warning: " + e.getMessage()) ;
	}
    }

    public static void main(String[] args) {

	System.out.println(LPOverrideConsumables.class.getName() + ": $Revision:   1.6  $") ;
        System.out.println("start time: " + now()) ;

	loadParams() ;

        if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("-d")) {
                    debug = true ;
                } 
            }
        }

	if (debug) {
		logger.setLevel((Level) Level.DEBUG) ;
	}

	if (amd.c == null) {
		System.err.println("The DBConnection.properties file is required for DB connections.") ;
		System.exit(4) ;
	}

	logger.debug("WindowAlgo instantiate") ;

        WindowAlgo w = new WindowAlgo(/* input buf */ bufSize,
            /* aging buffer */ ageBufSize) ;

        w.setDebug(debug) ;

        try {
	    logger.debug("TableSnapshot setup") ;
            F1 = new TableSnapshot(bufSize, new LPOverrideConsumablesFactory("select * from amd_locpart_overid_consumables where action_code <> 'D' order by part_no, spo_location, tsl_override_type",AmdConnection.instance(), prefetchSize)) ;
	    logger.debug("F1 created") ;
            F2 = new TableSnapshot(bufSize, new LPOverrideConsumablesFactory("select * from tmp_locpart_overid_consumables order by part_no, spo_location, tsl_override_type",AmdConnection.instance(), prefetchSize)) ;
	    logger.debug("F2 created") ;
            logger.debug("start diff") ;

	    w.diff(F1, F2) ;
        }
        catch (SQLException e) {
            System.err.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;
	    System.exit(2) ;
        }
        catch (ClassNotFoundException e) {
            System.err.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;
	    System.exit(4) ;
        }
        catch (Exception e) {
		System.err.println(e.getMessage()) ;
		logger.error(e.getMessage()) ;
            	if (F1 == null) {
			System.err.println("F1 not initialized.") ;
			logger.error("F1 not initialized.") ;
	    	}
	            if (F2 == null) {
			System.err.println("F2 not initialize.") ;
			logger.error("F2 not initialized.") ;
		    }
		System.exit(6) ;
	} finally {
    		    updateCounts() ;
		    System.exit(0) ;
        }
    }

    private static void updateCounts() {
	if (F1 != null) {
		System.out.println("amd_locpart_overid_consumables=" + F1.getRecsIn()) ;
		logger.info("amd_locpart_overid_consumables=" + F1.getRecsIn()) ;
	} else {
		System.out.println("amd_locpart_overid_consumables: F1 (old master) not initialized") ;
		logger.info("amd_locpart_overid_consumables: F1 (old master) not initialized") ;
	}
	if (F2 != null) {
		System.out.println("tmp_locpart_overid_consumables=" + F2.getRecsIn()) ;
		logger.info("tmp_locpart_overid_consumables=" + F2.getRecsIn()) ;
	} else {
		System.out.println("tmp_locpart_overid_consumables: F2 (new master) not initialized") ;
		logger.info("tmp_locpart_overid_consumables: F2 (new master) not initialized") ;
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
		String      spo_location ;
		String	    tsl_override_type ;


        public boolean equals(Object o) {
            Key k = (Key) o ;
            return ( k.part_no.equals(part_no) 
			&& k.spo_location.equals(spo_location) 
			&& k.tsl_override_type.equals(tsl_override_type) ) ;

        }
        public int hashCode() {
            return part_no.hashCode()
            + spo_location.hashCode()
            + tsl_override_type.hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (theKey.part_no + theKey.spo_location + theKey.tsl_override_type).compareTo(
				part_no + spo_location + tsl_override_type) ;
        }
        public String toString() {
            return "LPOverrideConsumables =" + part_no + spo_location + tsl_override_type  ;
        }
    }
    Key key ;
    class Body {
		int	tsl_override_qty ;
		boolean	tsl_override_qty_isnull ;
		String	tsl_override_user ;
		boolean	tsl_override_user_isnull ;
		String	tsl_override_source ;
		boolean	tsl_override_source_isnull ;
		int	loc_sid ;
		boolean	loc_sid_isnull ;


        public String toString() {
            return "tsl_override_qty=" + tsl_override_qty + " " 
            + "tsl_override_user=" + tsl_override_user + " " 
            + "tsl_override_source=" + tsl_override_source + " " 
            + "loc_sid=" + loc_sid  ;
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

	boolean equal(String s1, boolean null1, String s2, boolean null2) {
		if (!null1)
               		if (!null2)
               			return s1.equals(s2) ; 
                	else
               	     		return false ;
            	else if (null2)
               	 	return true ; // both null
            	else
               	 	return false ; // s1 == null && s2 != null
        }

        public boolean equals(Object o) {
            Body b = (Body) o ;
            boolean result ;
            boolean resultField ;
	    result = equal(b.tsl_override_qty, b.tsl_override_qty_isnull,  tsl_override_qty, tsl_override_qty_isnull) ; 
	    result = result && equal(b.tsl_override_user, b.tsl_override_user_isnull,  tsl_override_user, tsl_override_user_isnull) ; 
	    result = result && equal(b.tsl_override_source, b.tsl_override_source_isnull,  tsl_override_source, tsl_override_source_isnull) ; 
	    result = result && equal(b.loc_sid, b.loc_sid_isnull,  loc_sid, loc_sid_isnull) ; 
            return result ;
        }
    }
    Body body ;

    LPOverrideConsumables(ResultSet r) throws SQLException {
        try {
        	key = new Key() ;
        	key.part_no = r.getString("PART_NO") ;
		key.spo_location = r.getString("SPO_LOCATION") ;
		key.tsl_override_type = r.getString("TSL_OVERRIDE_TYPE") ;

        	body = new Body() ;
        	body.tsl_override_qty = r.getInt("TSL_OVERRIDE_QTY");
        	body.tsl_override_qty_isnull = r.wasNull() ;
        	body.tsl_override_user = r.getString("TSL_OVERRIDE_USER");
        	body.tsl_override_user_isnull = r.wasNull() ;
        	body.tsl_override_source = r.getString("TSL_OVERRIDE_SOURCE");
        	body.tsl_override_source_isnull = r.wasNull() ;
        	body.loc_sid = r.getInt("LOC_SID");
        	body.loc_sid_isnull = r.wasNull() ;
        } catch (java.sql.SQLException e) {
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

    private void  doLPOverrideConsumablesDiff(String action_code) {
	if ((rowsInserted + rowsUpdated + rowsDeleted) % debugThreshold == 0) {
        	logger.debug(action_code + ": key=" + key + " part_no: " + key.part_no + " spo_location: " + key.spo_location + " type:" + key.tsl_override_type ) ;
		logger.debug("body=" + body) ;
	}
	if (!no_op) {
			try {
			    CallableStatement cstmt = amd.c.prepareCall(
				"{? = call amd_lp_override_consumabl_pkg.doLPOverrideConsumablesDiff("
				+ "?, ?, ?, ?, ?, ?, ?, ?)}") ;
			    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
			    cstmt.setString(2, key.part_no) ;
			    cstmt.setString(3, key.spo_location) ;
			    cstmt.setString(4, key.tsl_override_type) ;
			    cstmt.setString(5, body.tsl_override_user) ;
			    cstmt.setString(6, body.tsl_override_source) ;
			    setInt(cstmt, 7, body.tsl_override_qty, body.tsl_override_qty_isnull) ;
			    setInt(cstmt, 8, body.loc_sid, body.loc_sid_isnull) ;
			    cstmt.setString(9, action_code) ;
			    cstmt.execute() ;

			    int result = cstmt.getInt(1) ;
			    if (result > 0) {
				updateCounts() ;
				System.out.println("amd_lp_override_consumabl_pkg.doLPOverrideConsumablesDiff failed with result = " + result) ;
				logger.fatal("amd_lp_override_consumabl_pkg.doLPOverrideConsumablesDiff failed with result = " + result) ;
				System.exit(result) ;
			    }
			    cstmt.close() ;
			}
			catch (java.sql.SQLException e) {
			    updateCounts() ;
			    System.out.println(e.getMessage()) ;
			    logger.fatal(e.getMessage()) ;
			    System.exit(4) ;
			}
	}
    }
    public void  insert() {
	    doLPOverrideConsumablesDiff("A") ;
            rowsInserted++ ;
    }
    public void     update() {
	    doLPOverrideConsumablesDiff("C") ;
            rowsUpdated++ ;
    }
    public void     delete() {
		doLPOverrideConsumablesDiff("D") ;
		rowsDeleted++ ;
    }

   public static String now() {
	Calendar cal = Calendar.getInstance(TimeZone.getDefault());
	String DATE_FORMAT = "M/dd/yy hh:mm:ss a";
	java.text.SimpleDateFormat sdf =
			new java.text.SimpleDateFormat(DATE_FORMAT);
	sdf.setTimeZone(TimeZone.getDefault());
	return sdf.format(cal.getTime());
    }
}
