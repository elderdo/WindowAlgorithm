import java.sql.* ;
import java.math.BigDecimal ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

/*   $Author:   zf297a  $
   $Revision:   1.4  $
       $Date:   31 Jan 2008 12:18:14  $
   $Workfile:   InRepair.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\InRepair.java-arc  $
/*
/*   Rev 1.4   31 Jan 2008 12:18:14   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.3   Apr 02 2007 15:52:10   c402417
/*Changes the diff method on the repair_date using compareTo.
/*
/*   Rev 1.2   Oct 31 2005 12:35:24   c402417
/*Using compareTo method for loc_sid and repair_qty.
/*
/*   Rev 1.1   10 Aug 2004 08:29:24   c970183
/*Added check for action_code != 'D' for all F1 (old master) TableSnapshots.
        */
public class InRepair implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug ;
    static boolean no_op    = false;
    static int bufSize      = 5000 ;
    static int ageBufSize   = 5000 ;
    static int prefetchSize = 5000 ;
    static int debugThreshold = 1000 ;
    static int showDiffThreshold = 100 ;


    static Logger logger = Logger.getLogger(InRepair.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_in_repair
    private static TableSnapshot F2 = null ; // tmp_amd_in_repair

    static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(InRepair.class.getName()).getProperties() ;

       		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","5000") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","1000") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","100") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","5000") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","5000") ).intValue() ;
		logger.debug("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" + prefetchSize + " no_op=" + no_op
				+ " debug=" + debug + " debugThreshold=" + debugThreshold + " showDiffThreshold=" + showDiffThreshold) ;

		if (debug) {
			System.out.println("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" + prefetchSize + " no_op=" + no_op + " debugThreshold=" + debugThreshold + " showDiffThreshold=" + showDiffThreshold) ;
		}

	} catch (java.io.IOException e) {
		System.err.println("InRepair: warning: " + e.getMessage()) ;
	} catch (java.lang.Exception e) {
		System.err.println("InRepair: warning: " + e.getMessage()) ;
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
            F1 = new TableSnapshot(bufSize, new InRepairFactory("Select * from amd_in_repair where action_code != 'D' order by part_no, loc_sid, order_no")) ;
            F2 = new TableSnapshot(bufSize, new InRepairFactory("Select * from tmp_amd_in_repair order by part_no, loc_sid, order_no")) ;
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
		System.out.println("amd_in_repair_in=" + F1.getRecsIn()) ;
		logger.info("amd_in_repair_in=" + F1.getRecsIn()) ;
	}
	if (F2 != null) {
		System.out.println("tmp_amd_in_repair_in=" + F2.getRecsIn()) ;
		logger.info("tmp_amd_in_repair_in=" + F2.getRecsIn()) ;
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
		double 	    loc_sid ;
		String 		order_no ;


        public boolean equals(Object o) {
            Key k = (Key) o ;
            return ( (k.part_no.equals(part_no) )&& ( new Double(k.loc_sid).compareTo(new Double(loc_sid)) == 0 )&& (k.order_no.equals(order_no)));
        }
		public int hashCode() {
            return part_no.hashCode() + new Double(loc_sid).hashCode() + order_no.hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (theKey.part_no + theKey.loc_sid + theKey.order_no).compareTo(
				part_no + loc_sid + order_no) ;
        }
        public String toString() {
            return "part_no =" + part_no + " loc_sid=" +  new Double(loc_sid).toString() + " order_no=" + order_no  ;
        }
    }
    Key key ;
    class Body {
	double  repair_qty ;
	java.sql.Date	repair_date ;
	java.sql.Date  	repair_need_date ;
	boolean	repair_need_date_isnull;


        public String toString() {
            return "repair_qty=" + repair_qty + " " +
                    "repair_date=" + repair_date + " " +
                    "repair_need_date=" + repair_need_date + " ";
        }
        boolean equal(String s1, String s2) {
			if (s1 != null)
				if (s2 != null)
					return s1.equals(s2) ;
				else
					return false;
			else if (s2 == null)
				return true ; // both null
			else
				return false ; // s1 == null && s2 != null
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
            result = (new Double(b.repair_qty).compareTo (new Double(repair_qty)) == 0) ;
	    showDiff(result, "repair_qty", b.repair_qty + "", repair_qty + "") ;
	    result = result && equal(b.repair_date,repair_date) ;
            showDiff(result, "repair_date", b.repair_date + "", repair_date + "") ;
	    result = result && equal(b.repair_need_date, b.repair_need_date_isnull,repair_need_date, repair_need_date_isnull) ;
            showDiff(result, "repair_need_date", b.repair_need_date + "", repair_need_date + "") ;
            return result ;
        }
    }
    Body body ;

    InRepair(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.part_no = r.getString("part_no") ;
		key.loc_sid = r.getDouble("loc_sid") ;
		key.order_no = r.getString("order_no") ;

        body = new Body() ;
		logger.debug("Getting repair_qty") ;
        body.repair_qty = r.getDouble("repair_qty") ;
        body.repair_date = r.getDate("repair_date") ;
        body.repair_need_date = r.getDate("repair_need_date") ;
        body.repair_need_date_isnull = r.wasNull();
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
	if (! no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.InsertRow("
			+ "?, ?, ?, ?, ?,?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.part_no) ;
		    cstmt.setDouble(3, key.loc_sid) ;
		    cstmt.setDate(4, body.repair_date) ;
			cstmt.setDouble(5, body.repair_qty) ;
		    cstmt.setString(6, key.order_no) ;
		    cstmt.setDate(7,body.repair_need_date);
		    cstmt.execute() ;

		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_inventory_pkg.InsertRow failed with result = " + result) ;
			logger.fatal("amd_inventory_pkg.InsertRow failed with result = " + result) ;
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
        if (debug) {
            System.out.println("Insert: key=" + key + " body=" + body) ;
        }
        logger.info("Insert: key=" + key + " repair_qty=" + body.repair_qty) ;
		logger.info("Insert: key=" + key + " repair_date=" + body.repair_date) ;
        rowsInserted++ ;
    }

    public void     update() {
	 if (! no_op) {
		 try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.UpdateRow("
			+ "?, ?, ?, ?, ?, ? )}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.part_no) ;
		    cstmt.setDouble(3, key.loc_sid) ;
		    cstmt.setDate(4, body.repair_date) ;
			cstmt.setDouble(5, body.repair_qty) ;
		    cstmt.setString(6, key.order_no) ;
			cstmt.setDate(7, body.repair_need_date) ;
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
	}
        if (debug) {
            System.out.println("Update: key=" + key + " body=" + body) ;
        }
        logger.info("Update: key=" + key + " repair_qty=" + body.repair_qty) ;
		logger.info("Update: key=" + key + " repair_date=" + body.repair_date) ;
        rowsUpdated++ ;
    }
    public void     delete() {
	if (! no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.inRepairDeleteRow(?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.part_no) ;
		    cstmt.setDouble(3, key.loc_sid) ;
			cstmt.setString(4, key.order_no) ;
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_inventory.inRepairDeleteRow failed with result = " + result) ;
			logger.fatal("amd_inventory.inRepairDeleteRow failed with result = " + result) ;
			System.exit(result) ;
		    }
		    cstmt.close() ;
		}
		catch (java.sql.SQLException e) {
		    System.out.println("amd_inventory.inRepairDeleteRow failed") ;
		    updateCounts() ;
		    System.out.println(e.getMessage()) ;
		    logger.fatal(e.getMessage()) ;
		    System.exit(4) ;
		}
	}
        if (debug) {
            System.out.println("Delete: key=" + key) ;
        }
        logger.info("Delete: key=" + key + " repair_qty=" + body.repair_qty) ;
		logger.info("Delete: key=" + key + "repair_date=" + body.repair_date) ;
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
