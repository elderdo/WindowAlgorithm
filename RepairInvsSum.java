/*   $Author:   zf297a  $
   $Revision:   1.2  $
       $Date:   18 Jul 2008 11:33:58  $
   $Workfile:   RepairInvsSum.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\RepairInvsSum.java.-arc  $
/*
/*   Rev 1.2   18 Jul 2008 11:33:58   zf297a
/*Put paren's around order_no in where clause of F2
/*
/*   Rev 1.1   31 Jan 2008 12:18:16   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.0   Oct 31 2005 21:38:04   zf297a
/*Initial revision.
*/
import java.sql.* ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

public class RepairInvsSum implements Rec {
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

    static Logger logger = Logger.getLogger(RepairInvsSum.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_repair_invs_sum
    private static TableSnapshot F2 = null ; // tmp_amd_in_repair

    static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(RepairInvsSum.class.getName()).getProperties() ;

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


	logger.debug("in main version 1.0");
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
            F1 = new TableSnapshot(bufSize, new RepairInvsSumFactory("SELECT part_no, site_location, qty_on_hand FROM AMD_REPAIR_INVS_SUM WHERE action_code != 'D' ORDER BY part_no, site_location")) ;
            F2 = new TableSnapshot(bufSize, new RepairInvsSumFactory("SELECT part_no, Amd_Utils.getSpoLocation(loc_sid) site_location, SUM(repair_qty) qty_on_hand FROM TMP_AMD_IN_REPAIR WHERE   action_code != 'D' AND (order_no LIKE 'R%' OR order_no LIKE 'II%') GROUP BY part_no, Amd_Utils.getSpoLocation(loc_sid) HAVING Amd_Utils.getSpoLocation(loc_sid) IS NOT NULL ORDER BY part_no, Amd_Utils.getSpolocation(loc_sid)")) ;

            w.diff(F1, F2) ;
        }
        catch (SQLException e) {
            System.out.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;
			System.exit(4) ;
        }
        catch (ClassNotFoundException e) {
            System.out.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;
			System.exit(6) ;
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
			System.exit(8) ;
		}
        finally {
            updateCounts() ;
            System.exit(0) ;
        }
    }

    private static void updateCounts() {

	if (F1 == null) {
		System.out.println("F1 not initialized.") ;
		logger.error("F1 not initialized.") ;
	} else {
		System.out.println("amd_repair_invs_in=" + F1.getRecsIn()) ;
		logger.info("amd_repair_invs_in=" + F1.getRecsIn()) ;
	}
	if (F2 == null) {
	} else {
		System.out.println("tmp_amd_in_repair=" + F2.getRecsIn()) ;
		logger.info("tmp_amd_in_repair=" + F2.getRecsIn()) ;
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
        String part_no ;
        String site_location;

        public boolean equals(Object o) {
            Key k = (Key) o ;
            return (    (k.part_no.equals(part_no) )
                     && (k.site_location.equals(site_location)) ) ;
        }
        public int hashCode() {
            return  part_no.hashCode()
                   + site_location.hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (theKey.part_no + theKey.site_location).compareTo(
				    part_no + site_location) ;
        }
        public String toString() {
            return "part_no + site_location =" + part_no + site_location ;
        }
    }
    Key key ;
    class Body {
		double qty_on_hand ;

        public String toString() {
            return "qty_on_hand=" + qty_on_hand ;
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
            result = (new Double(b.qty_on_hand).compareTo(new Double (qty_on_hand))) == 0 ;
            showDiff(result, "qty_on_hand", b.qty_on_hand + "", qty_on_hand + "") ;

            return result ;
        }
    }
    Body body ;

    RepairInvsSum(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.part_no  = r.getString("part_no") ;
        key.site_location  = r.getString("site_location") ;

        body = new Body() ;
        body.qty_on_hand = r.getDouble("qty_on_hand") ;
        }
        catch (java.sql.SQLException e) {
            updateCounts() ;
            System.out.println(e.getMessage() ) ;
            logger.fatal(e.getMessage()) ;
            System.exit(10) ;
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
            System.exit(12) ;
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
            System.exit(14) ;
        }
    }

    private void doRepairInvsSumDiff(String action_code) {
	String storedProc =
		"amd_inventory.doRepairInvsSumDiff("
		+ key.part_no + ","
		+ key.site_location + ","
		+ body.qty_on_hand + ","
		+ action_code + ")" ;
        try {
            CallableStatement cstmt = amd.c.prepareCall(
                "{? = call amd_inventory.doRepairInvsSumDiff("
                + "?, ?, ?, ?)}") ;
            cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
            cstmt.setString(2, key.part_no) ;
            cstmt.setString(3, key.site_location) ;
            cstmt.setDouble(4, body.qty_on_hand) ;
	    cstmt.setString(5, action_code) ;
            cstmt.execute() ;

            int result = cstmt.getInt(1) ;
            if (result > 0) {
                updateCounts() ;
                System.out.println(storedProc + " failed with result = " + result) ;
         	logger.fatal(storedProc + " failed with result = " + result) ;
                System.exit(result) ;
            }
            cstmt.close() ;
        }
        catch (java.sql.SQLException e) {
            updateCounts() ;
            System.out.println(e.getMessage() + " for " + storedProc) ;
            logger.fatal(e.getMessage() + " for " + storedProc) ;
            System.exit(16) ;
        }
        if (debug) {
            System.out.println(storedProc) ;
        }
        logger.info(storedProc) ;
    }

    public void     insert() {
	    if (!no_op) {
		    doRepairInvsSumDiff("A") ;
	    }
	    rowsInserted++ ;
    }

    public void     update() {
	    if (!no_op) {
		    doRepairInvsSumDiff("C") ;
	    }
           rowsUpdated++ ;
    }
    public void     delete() {
	    if (!no_op) {
		doRepairInvsSumDiff("D") ;
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
