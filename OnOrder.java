import java.sql.* ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

/*   $Author nz652c $
   $Revision:   1.13  $
       $Date 7/23/2004 15:00:00 $
   $Workfile  onOrder.java $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\OnOrder.java-arc  $
/*
/*   Rev 1.13   31 Jan 2008 12:18:16   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.12   20 Dec 2007 19:07:52   zf297a
/*Added some logger debug statements and checked for instantiated objects for F1 and F2 before accessing their record counts.
/*
/*   Rev 1.11   Dec 19 2007 13:33:32   c402417
/*Made Loc_sid a key .
/*
/*   Rev 1.10   Apr 27 2007 11:15:28   c402417
/*Add order by clause to F1  as in F2.
/*
/*   Rev 1.9   28 Feb 2007 10:23:38   zf297a
/*Fixed methods equals, hashCode, and compareTo for the Key nested class.
/*
/*   Rev 1.8   Feb 27 2007 13:07:24   c402417
/*Added LINE to F1 and F2
/*
/*   Rev 1.7   Jun 21 2006 09:44:26   zf297a
/*Make sure that F1 and F2 are order by gold_order_number, part_no, and order_date - any tmp_a2a_order_info_line rows will be created in that order too - consequently the assigned line number will increment in conjunction with the order_date
/*
/*   Rev 1.6   Dec 06 2005 12:38:54   zf297a
/*Changed the key: added order_date.  Changed the invocation of the PL/SQL deleteRow: added additional parameters: part_no, loc_sid, and order_date.
/*
/*   Rev 1.5   Oct 31 2005 11:45:20   zf297a
/*Fixed the equal method by using Double's compareTo method for the loc_sid field and the order_qty field, which are defined as doubles, instead of using the == operator.  Changed the amd_inventory.InsertRow to amd_inventory.InsertOnOrderRow and amd_inventory.updateOnOrderRow.
/*
/*   Rev 1.4   Aug 04 2005 07:58:00   zf297a
/*Added sched_receipt_date
/*
/*   Rev 1.3   May 04 2005 09:53:22   c970183
/*Added some logger debug's
/*
/*   Rev 1.2   10 Aug 2004 08:29:22   c970183
/*Added check for action_code != 'D' for all F1 (old master) TableSnapshots.
/*
/*   Rev 1.0   Jul 29 2004 14:54:54   c970183
/*Initial revision.
        */
public class OnOrder implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug ;
    static boolean no_op    = false;
    static int bufSize      = 200 ;
    static int ageBufSize   = 200 ;
    static int prefetchSize = 200 ;
    static int debugThreshold = 10 ;
    static int showDiffThreshold = 10 ;


    static Logger logger = Logger.getLogger(OnOrder.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_on_order
    private static TableSnapshot F2 = null ; // tmp_amd_on_order

   static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(OnOrder.class.getName()).getProperties() ;

       		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","200") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","10") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","10") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","200") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","200") ).intValue() ;
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
 

	logger.debug("in main");

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
	    logger.debug("Creating F1 and F2") ;
            F1 = new TableSnapshot(bufSize, new OnOrdersFactory("Select * from amd_on_order where action_code != 'D' order by gold_order_number, order_date, loc_sid")) ;
			logger.debug("F1 created") ;
            F2 = new TableSnapshot(bufSize, new OnOrdersFactory("Select * from tmp_amd_on_order order by gold_order_number, order_date,loc_sid")) ;
			logger.debug("F2 created") ;
            logger.debug("start diff") ;

            w.diff(F1, F2) ;
        }
        catch (SQLException e) {
            System.out.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;
			System.exit(2) ;
        }
        catch (ClassNotFoundException e) {
            System.out.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;
			System.exit(4) ;
        }
        catch (Exception e) {
			System.out.println(e.getMessage()) ;
			logger.error(e.getMessage()) ;
            if (F1 == null) {
				System.out.println("F1 not initialized.") ;
				logger.error("F1 not initialized.") ;
			}
            if (F2 == null) {
				System.out.println("F2 not initialize.") ;
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
	        System.out.println("amd_on_order_in=" + F1.getRecsIn()) ;
       		 logger.info("amd_on_order_in=" + F1.getRecsIn()) ;
	}
	if (F2 != null) {
	        System.out.println("tmp_amd_on_order_in=" + F2.getRecsIn()) ;
        	logger.info("tmp_amd_on_order_in=" + F2.getRecsIn()) ;
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
        String gold_order_number ;
	java.sql.Date order_date ;
	double loc_sid ;
        public boolean equals(Object o) {
            Key k = (Key) o ;
            return (k.gold_order_number.equals(gold_order_number) && k.order_date.equals(order_date) && ( new Double(k.loc_sid).compareTo(new Double(loc_sid)) == 0)) ;
        }
        public int hashCode() {
            return gold_order_number.hashCode() + order_date.hashCode() + new Double(loc_sid).hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (theKey.gold_order_number + theKey.order_date  + theKey.loc_sid).compareTo(gold_order_number + order_date + loc_sid ) ;
        }
        public String toString() {
            return "gold_order_number =" + gold_order_number 
		   + " order_date=" + order_date 
		   + " loc_sid=" + loc_sid ;
        }
    }
    Key key ;
    class Body {
        String part_no ;
	double order_qty ;
	java.sql.Date sched_receipt_date ;
	boolean sched_receipt_date_isnull ;

        public String toString() {
            return "part_no=" + part_no + " " +
                    "order_qty=" + order_qty + " " +
                    "sched_receipt_date=" + sched_receipt_date + " ";
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
        public boolean equals(Object o) {
            Body b = (Body) o ;
            boolean result ;
            result = equal(b.part_no, part_no) ;
            showDiff(result, "part_no", b.part_no + "", part_no + "") ;
            result = result && (new Double(b.order_qty).compareTo(new Double(order_qty)) == 0) ;
            showDiff(result, "order_qty", b.order_qty + "", order_qty + "") ;
            result = result && equal(b.sched_receipt_date, b.sched_receipt_date_isnull, sched_receipt_date, sched_receipt_date_isnull) ;
            showDiff(result, "sched_receipt_date", b.sched_receipt_date + "", sched_receipt_date + "") ;

            return result ;
        }
    }
    Body body ;

    OnOrder(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.gold_order_number = r.getString("gold_order_number") ;
        key.order_date = r.getDate("order_date") ;
        key.loc_sid = r.getDouble("loc_sid") ;

        body = new Body() ;
        logger.debug("Getting part_no") ;
        body.part_no = r.getString("part_no") ;
        logger.debug("Getting order_qty") ;
        body.order_qty = r.getDouble("order_qty") ;
        body.sched_receipt_date = r.getDate("sched_receipt_date") ;
        body.sched_receipt_date_isnull = r.wasNull() ;
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
	    if (!no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.insertOnOrderRow("
			+ "?, ?, ?, ?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, body.part_no) ;
		    cstmt.setDouble(3, key.loc_sid) ;
		    cstmt.setDate(4, key.order_date) ;
		    cstmt.setDouble(5, body.order_qty) ;
		    cstmt.setString(6, key.gold_order_number) ;
		    cstmt.setDate(7, body.sched_receipt_date) ;
		    logger.debug("Inserting " + key.gold_order_number) ;
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_inventory.InsertRow failed with result = " + result) ;
			logger.fatal("amd_inventory.InsertRow failed with result = " + result) ;
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
		if (debug) {
		    System.out.println("Insert: key=" + key + " body=" + body) ;
		}
		logger.info("Insert: key=" + key + " part_no=" + body.part_no) ;
	    }
        rowsInserted++ ;
    }

    public void     update() {
	    if (!no_op) {
		    
		 try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.updateOnOrderRow("
			+ "?, ?, ?, ?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, body.part_no) ;
		    cstmt.setDouble(3, key.loc_sid) ;
		    cstmt.setDate(4, key.order_date) ;
		    cstmt.setDouble(5, body.order_qty) ;
		    cstmt.setString(6, key.gold_order_number) ;
		    cstmt.setDate(7, body.sched_receipt_date) ;
		    logger.debug("Updating " + key.gold_order_number) ;
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_inventory.UpdateRow failed with result = " + result) ;
			logger.fatal("amd_inventory.UpdateRow failed with result = " + result) ;
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
		if (debug) {
		    System.out.println("Update: key=" + key + " body=" + body) ;
		}
		logger.info("Update: key=" + key + " part_no=" + body.part_no) ;
	    }
        rowsUpdated++ ;
    }
    public void     delete() {
	    if (!no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.DeleteRow(?, ?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, body.part_no) ;
		    cstmt.setDouble(3, key.loc_sid) ;
		    cstmt.setString(4, key.gold_order_number) ;
		    cstmt.setDate(5, key.order_date) ;
		    logger.debug("Deleting " + key.gold_order_number + ", " + key.order_date) ;
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_inventory.DeleteRow failed with result = " + result) ;
			logger.fatal("amd_inventory.DeleteRow failed with result = " + result) ;
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
		if (debug) {
		    System.out.println("Delete: key=" + key) ;
		}
		logger.info("Delete: key=" + key + " part_no=" + body.part_no) ;
	    }
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
