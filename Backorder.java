import java.sql.* ;
import java.math.BigDecimal ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

/*   $Author:   zf297a  $
   $Revision:   1.4  $
       $Date:   16 Jan 2009 00:03:08  $
   $Workfile:   Backorder.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\Backorder.java.-arc  $
/*
/*   Rev 1.4   16 Jan 2009 00:03:08   zf297a
/*Removed loc_sid as part of the key and changed it to spo_location.   Changed the F2 query to retrieve a NON-NULL spo_location using amd_utils.getSpoLocation(loc_sid).
/*
/*Improved error logging.
/*
/*   Rev 1.3   31 Jan 2008 12:18:14   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.2   Nov 29 2006 14:26:16   zf297a
/*Changed amd_backorder_sum column from part_no to spo_prime_part_no.  Changed query for F2 to use the spo_prime_part_no for summation.  Added some additional error checks - General Exception.
/*
/*   Rev 1.1   Sep 27 2006 10:06:00   c402417
/*Changed the compare method  of sum_qty from "==" to "compareTo"
/*
/*   Rev 1.0   Dec 06 2005 11:15:36   zf297a
/*Initial revision.
        */
public class Backorder implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug ;
    static boolean no_op    = false;
    static int bufSize      = 1500 ;
    static int ageBufSize   = 1500 ;
    static int prefetchSize = 1500 ;
    static int debugThreshold = 100 ;
    static int showDiffThreshold = 100 ;

    static Logger logger = Logger.getLogger(Backorder.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_backorder_sum
    private static TableSnapshot F2 = null ; // tmp_amd_reqs

    static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(Backorder.class.getName()).getProperties() ;

       		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","1500") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","100") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","100") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","1500") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","1500") ).intValue() ;
		logger.debug("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" + prefetchSize + " no_op=" + no_op
				+ " debug=" + debug + " debugThreshold=" + debugThreshold + " showDiffThreshold=" + showDiffThreshold) ;

		if (debug) {
			System.out.println("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" + prefetchSize + " no_op=" + no_op + " debugThreshold=" + debugThreshold + " showDiffThreshold=" + showDiffThreshold) ;
		}

	} catch (java.io.IOException e) {
		System.err.println("Backorder: warning: " + e.getMessage()) ;
	} catch (java.lang.Exception e) {
		System.err.println("Backorder: warning: " + e.getMessage()) ;
	}
    }

    public static void main(String[] args) {

	loadParams() ;
        System.out.println("start time: " + now()) ;
        if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("-d")) {
                    debug = true ;
                } 
            }
        }

        WindowAlgo w = new WindowAlgo(/* input buf */ bufSize,
            /* aging buffer */ ageBufSize) ;

        w.setDebug(debug) ;
        try {

	    logger.debug("Creating F1 & F2") ;

            F1 = new TableSnapshot(bufSize, new BackorderFactory
            ("Select spo_prime_part_no, spo_location, qty sum_qty from amd_backorder_sum where action_code != 'D' order by spo_prime_part_no,spo_location")) ;
	    logger.debug("F1 created ") ;
            F2 = new TableSnapshot(bufSize, new BackorderFactory
            ("Select Amd_Utils.getSpoPrimePartNo(part_no) spo_prime_part_no, amd_utils.getSpoLocation(loc_sid) spo_location, sum(quantity_due) sum_qty from tmp_amd_reqs where amd_utils.getSpoLocation(loc_sid) is not null group by Amd_Utils.getSpoPrimePartNo(part_no), amd_utils.getSpoLocation(loc_sid) having amd_utils.getSpoPrimePartNo(part_no) is not null order by Amd_Utils.getSpoPrimePartNo(part_no), amd_utils.getSpoLocation(loc_sid)")) ;
            logger.debug("start Backorder diff") ;

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
	    System.err.println("A General Exception occured.") ;
	    if (e != null) {
		logger.debug("General Exception occured.") ;
		String theError = e.getMessage() ;
		if (theError != null) {
			System.err.println(e.getMessage()) ;
			logger.error(e.getMessage()) ;
		} else {
			System.err.println("Unknown error") ;
		}
	    }
            if (F1 == null) {
			System.err.println("F1 not initialized.") ;
			logger.error("F1 not initialized.") ;
	    }
            if (F2 == null) {
			System.err.println("F2 not initialize.") ;
			logger.error("F2 not initialized.") ;
	    }
	    System.exit(6) ;
	}
        finally {
            updateCounts() ;
	    System.exit(0) ;
        }
    }

    private static void updateCounts() {
	if (F1 != null) {
		System.out.println("amd_backorder_sum_in=" + F1.getRecsIn()) ;
		logger.info("amd_backorder_sum_in=" + F1.getRecsIn()) ;
	}
	if (F2 != null) {
		System.out.println("tmp_amd_reqs=" + F2.getRecsIn()) ;
		logger.info("tmp_amd_reqs=" + F2.getRecsIn()) ;
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
		String      spo_prime_part_no ;
		String      spo_location ;



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


        public boolean equals(Object o) {
            Key k = (Key) o ;
			return ( (k.spo_prime_part_no.equals(spo_prime_part_no) )
			&& k.spo_location.equals(spo_location) ) ;
		}

        public int hashCode() {
            return spo_prime_part_no.hashCode()
		+ spo_location.hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (theKey.spo_prime_part_no + theKey.spo_location).compareTo(
				 spo_prime_part_no + spo_location) ;
        }
        public String toString() {
            return "part_no / spo_location =" + spo_prime_part_no + spo_location  ;
        }
    }

    Key key ;
    class Body {
		double sum_qty ;


        public String toString() {
            return "sum_qty=" + sum_qty + " " ;
        }

        boolean equal (String s1, String s2) {
			if (s1 != null)
				if (s2 != null)
					return s1.equals(s2) ;
				else
					return false ;
			else if (s2 == null)
				return true ;
			else
				return false ;
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

		boolean equal(java.sql.Date date1, java.sql.Date date2) {

				return date1.compareTo(date2) == 0;

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
            result = (new Double(b.sum_qty).compareTo (new Double(sum_qty)) == 0 );
	    	showDiff(result, "sum_qty", b.sum_qty + "", sum_qty + "") ;
            return result ;
        }
    }
    Body body ;

    Backorder(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
		key.spo_prime_part_no = r.getString("SPO_PRIME_PART_NO") ;
		key.spo_location = r.getString("SPO_LOCATION") ;
	logger.debug("spo_prime_part_no =" + key.spo_prime_part_no) ;
	logger.debug("spo_location =" + key.spo_location) ;
        body = new Body() ;
		logger.debug("Getting sum_qty") ;
        body.sum_qty = r.getDouble("SUM_QTY") ;
        }
        catch (java.sql.SQLException e) {
            updateCounts() ;
            System.err.println(e.getMessage() ) ;
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
            logger.fatal(e.getMessage()) ;
            System.exit(4) ;
        }
    }

    public void  insert() {
	logger.debug("insert") ;
	if (! no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_reqs_pkg.InsertRowBackorder("
			+ "?, ?, ?)}") ;
			cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
			logger.debug("key.spo_prime_part_no=*" + key.spo_prime_part_no + "*") ;
			cstmt.setString(2, key.spo_prime_part_no) ;
			logger.debug("key.spo_location=*" + key.spo_location + "*") ;
			cstmt.setString(3, key.spo_location) ;
			logger.debug("body.sum_qty=*" + body.sum_qty + "*") ;
			cstmt.setDouble(4, body.sum_qty) ;

		    cstmt.execute() ;

		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.err.println("amd_reqs_pkg.InsertRowBackorder failed with result = " + result) ;
			logger.fatal("amd_reqs_pkg.InsertRowBackorder failed with result = " + result) ;
			System.exit(result) ;
		    }
		    cstmt.close() ;
		}
		catch (java.sql.SQLException e) {
		    updateCounts() ;
		    System.err.println("amd_reqs_pkg.InsertRowBackorder failed to execute") ;
				logger.fatal("amd_reqs_pkg.InsertRowBackorder failed to execute") ;
		    System.err.println(e.getMessage()) ;
		    logger.fatal(e.getMessage()) ;
		    System.exit(4) ;
		}
	}

        logger.debug("Insert: key=" + key + " sum_qty=" + body.sum_qty) ;
        rowsInserted++ ;
    }

    public void     update() {
	 logger.debug("update") ;
	 if (! no_op) {
		 try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_reqs_pkg.UpdateRowBackorder("
			+ "?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.spo_prime_part_no) ;
		    cstmt.setString(3, key.spo_location) ;
		    cstmt.setDouble(4, body.sum_qty) ;
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.err.println("amd_reqs_pkg.UpdateRowBackorder failed with result = " + result) ;
			logger.fatal("amd_amd_reqs_pkg.UpdateRowBackorder failed with result = " + result) ;
			System.exit(result) ;
		    }
		    cstmt.close() ;
		}
		catch (java.sql.SQLException e) {
		    updateCounts() ;
		    System.err.println(e.getMessage()) ;
		    logger.fatal(e.getMessage()) ;
		    System.exit(4) ;
		}
	}
        logger.info("Update: key=" + key + " sum_qty=" + body.sum_qty) ;
        rowsUpdated++ ;
    }
    public void     delete() {
	 logger.debug("delete") ;
	if (! no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_reqs_pkg.DeleteRowBackorder(?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.spo_prime_part_no) ;
		    cstmt.setString(3, key.spo_location) ;

		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.err.println("amd_reqs_pkg.DeleteRowBackorder failed with result = " + result) ;
			logger.fatal("amd_reqs_pkg.DeleteRowBackorder failed with result = " + result) ;
			System.exit(result) ;
		    }
		    cstmt.close() ;
		}
		catch (java.sql.SQLException e) {
		    updateCounts() ;
		    System.err.println(e.getMessage()) ;
		    logger.fatal(e.getMessage()) ;
		    System.exit(4) ;
		}
	}
        logger.info("Delete: key=" + key + " sum_qty=" + body.sum_qty) ;
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
