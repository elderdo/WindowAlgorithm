/*   $Author:   zf297a  $
   $Revision:   1.4  $
       $Date:   31 Jan 2008 12:18:16  $
   $Workfile:   RspSum.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\RspSum.java.-arc  $
/*
/*   Rev 1.4   31 Jan 2008 12:18:16   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.3   19 Oct 2007 11:25:18   zf297a
/*Added override_type to the key.  Modified F2 query to generate the override_type based on whether the spo_prime_part_no is repairable or consumable and gaurantee that only those types of parts are retrieved by adding those tests to the where clause.
/*
/*   Rev 1.2   Sep 27 2006 09:34:48   c402417
/*Changed "==" to "compareTo" when compare a  data type as DOUBLE .
/*
/*   Rev 1.1   Jun 05 2006 09:25:36   zf297a
/*For F2 added: spo_prime_part_no is not null
/*
/*   Rev 1.0   May 16 2006 12:15:16   c402417
/*Initial revision.
*/
import java.sql.* ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

public class RspSum implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug ;
    static boolean no_op    = false;
    static int bufSize      = 2500 ;
    static int ageBufSize   = 2500 ;
    static int prefetchSize = 200 ;
    static int debugThreshold = 10 ;
    static int showDiffThreshold = 10 ;

    static Logger logger = Logger.getLogger(RspSum.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_rsp_sum
    private static TableSnapshot F2 = null ; // tmp_amd_rsp

    static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(RspSum.class.getName()).getProperties() ;

       		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","2500") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","10") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","10") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","2500") ).intValue() ;
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
            F1 = new TableSnapshot(bufSize, new RspSumFactory("Select part_no, rsp_location, override_type, qty_on_hand, rsp_level from amd_rsp_sum where action_code != 'D' order by part_no, rsp_location, override_type", amd, prefetchSize)) ;
            F2 = new TableSnapshot(bufSize, new RspSumFactory("SELECT spo_prime_part_no part_no, (mob)||'_RSP' rsp_location , case when amd_utils.isPartRepairableYorN(spo_prime_part_no) = 'Y' then 'TSL Fixed' else 'ROP Fixed' end override_type, SUM(rsp_inv) qty_on_hand, SUM(rsp_level) rsp_level FROM AMD_RSP a , AMD_SENT_TO_A2A b , AMD_SPARE_NETWORKS c WHERE    a.loc_sid = c.loc_sid AND a.part_no = b.part_no AND a.action_code != 'D' AND b.action_code != 'D' AND  c.mob IS NOT NULL and spo_prime_part_no is not null and (amd_utils.isPartRepairableYorN(spo_prime_part_no) = 'Y' or amd_utils.isPartConsumableYorN(spo_prime_part_no) = 'Y') GROUP BY spo_prime_part_no, c.mob ORDER BY spo_prime_part_no", amd, prefetchSize)) ;
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
		System.out.println("amd_rsp_sum_in=" + F1.getRecsIn()) ;
		logger.info("amd_rsp_sum_in=" + F1.getRecsIn()) ;
	}
	if (F2 == null) {
	} else {
		System.out.println("tmp_amd_rsp=" + F2.getRecsIn()) ;
		logger.info("tmp_amd_rsp=" + F2.getRecsIn()) ;
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
        String rsp_location;
        String override_type ;

        public boolean equals(Object o) {
            Key k = (Key) o ;
            return (    k.part_no.equals(part_no) 
                     && k.rsp_location.equals(rsp_location)
		     && k.override_type.equals(override_type) ) ;
        }
        public int hashCode() {
            return  part_no.hashCode()
                   + rsp_location.hashCode() 
		   + override_type.hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (theKey.part_no + theKey.rsp_location + theKey.override_type).compareTo(
				    part_no + rsp_location + override_type) ;
        }
        public String toString() {
            return "part_no + rsp_location =" + part_no + rsp_location + override_type ;
		}

    }
    Key key ;
    class Body {
		double qty_on_hand ;
		double rsp_level ;

        public String toString() {
            return "qty_on_hand=" + qty_on_hand + " " +
                   "rsp_level=" + rsp_level + " " ;
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
            result = (new Double(b.qty_on_hand).compareTo (new Double(qty_on_hand)) == 0) ;
            showDiff(result, "qty_on_hand", b.qty_on_hand + "", qty_on_hand + "") ;
            result = result &&  (new Double(b.rsp_level).compareTo (new Double(rsp_level)) == 0) ;
            showDiff(result, "rsp_level", b.rsp_level + "" , rsp_level + "") ;

            return result ;
        }
    }
    Body body ;

    RspSum(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.part_no  = r.getString("part_no") ;
        key.rsp_location  = r.getString("rsp_location") ;
        key.override_type  = r.getString("override_type") ;

        body = new Body() ;
        body.qty_on_hand = r.getDouble("qty_on_hand") ;
        body.rsp_level = r.getDouble("rsp_level") ;
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

    private void doRspSumDiff(String action_code) {
	String storedProc =
		"amd_inventory.doRspSumDiff("
		+ key.part_no + ","
		+ key.rsp_location + ","
		+ key.override_type + ","
		+ body.qty_on_hand + ","
		+ body.rsp_level + ","
		+ action_code + ")" ;
        try {
            CallableStatement cstmt = amd.c.prepareCall(
                "{? = call amd_inventory.doRspSumDiff("
                + "?, ?, ?, ?, ?, ?)}") ;
            cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
            cstmt.setString(2, key.part_no) ;
            cstmt.setString(3, key.rsp_location) ;
            cstmt.setString(4, key.override_type) ;
            cstmt.setDouble(5, body.qty_on_hand) ;
            cstmt.setDouble(6, body.rsp_level) ;
	    	cstmt.setString(7, action_code) ;
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
		    doRspSumDiff("A") ;
	    }
	    rowsInserted++ ;
    }

    public void     update() {
	    if (!no_op) {
		    doRspSumDiff("C") ;
	    }
           rowsUpdated++ ;
    }
    public void     delete() {
	    if (!no_op) {
		doRspSumDiff("D") ;
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
