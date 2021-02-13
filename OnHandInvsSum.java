/*   $Author:   zf297a  $
   $Revision:   1.2  $
       $Date:   31 Jan 2008 12:18:14  $
   $Workfile:   OnHandInvsSum.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\OnHandInvsSum.java-arc  $
/*
/*   Rev 1.2   31 Jan 2008 12:18:14   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.1   Oct 31 2005 11:23:24   zf297a
/*Fixed the equal method by using Double's compareTo method for the qty_on_hand field, which is defined as a double, instead of using the == operator.
/*
/*   Rev 1.0   Sep 26 2005 11:47:32   zf297a
/*Initial revision.
*/
import java.sql.* ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

public class OnHandInvsSum implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug ;
    static boolean no_op    = false;
    static int bufSize      = 50000 ;
    static int ageBufSize   = 50000 ;
    static int prefetchSize = 1000 ;
    static int debugThreshold = 10 ;
    static int showDiffThreshold = 10 ;

    static Logger logger = Logger.getLogger(OnHandInvsSum.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_on_hand_invs_sum
    private static TableSnapshot F2 = null ; // tmp_on_hand_invs

    static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(OnHandInvsSum.class.getName()).getProperties() ;

       		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","100000") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","10") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","10") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","100000") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","5000") ).intValue() ;
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
            	F1 = new TableSnapshot(bufSize, new OnHandInvsSumFactory("Select part_no, spo_location, qty_on_hand from amd_on_hand_invs_sum where action_code != 'D' order by part_no, spo_location")) ;
            	F2 = new TableSnapshot(bufSize, new OnHandInvsSumFactory("Select part_no, amd_utils.getSpoLocation(loc_sid) spo_location, sum(inv_qty) qty_on_hand from tmp_amd_on_hand_invs where action_code != 'D' and amd_utils.getSpoLocation(loc_sid) is not null group by part_no, amd_utils.getSpoLocation(loc_sid) order by part_no, spo_location")) ;

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
		System.out.println("amd_on_hand_invs_in=" + F1.getRecsIn()) ;
		logger.info("amd_on_hand_invs_in=" + F1.getRecsIn()) ;
	}
	if (F2 == null) {
	} else {
		System.out.println("tmp_amd_on_hand_invs=" + F2.getRecsIn()) ;
		logger.info("tmp_amd_on_hand_invs=" + F2.getRecsIn()) ;
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
        String spo_location;

        public boolean equals(Object o) {
            Key k = (Key) o ;
            return (    (k.part_no.equals(part_no) )
                     && (k.spo_location.equals(spo_location)) ) ;
        }
        public int hashCode() {
            return  part_no.hashCode()
                   + spo_location.hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (theKey.part_no + theKey.spo_location).compareTo(
				    part_no + spo_location) ;
        }
        public String toString() {
            return "part_no + spo_location =" + part_no + spo_location ;
        }
    }
    Key key ;
    class Body {
		double qty_on_hand ;

        public String toString() {
            return "qty_on_hand=" + qty_on_hand ;
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
            result = new Double(b.qty_on_hand).compareTo(new Double(qty_on_hand)) == 0 ;
            showDiff(result, "qty_on_hand", b.qty_on_hand + "", qty_on_hand + "") ;

            return result ;
        }
    }
    Body body ;

    OnHandInvsSum(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.part_no  = r.getString("part_no") ;
        key.spo_location  = r.getString("spo_location") ;

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

    private void doOnHandInvsSumDiff(String action_code) {
	String storedProc =
		"amd_inventory.doOnHandInvsSumDiff(" 
		+ key.part_no + "," 
		+ key.spo_location + ","
		+ body.qty_on_hand + ","
		+ action_code + ")" ;  
        try {
            CallableStatement cstmt = amd.c.prepareCall(
                "{? = call amd_inventory.doOnHandInvsSumDiff("
                + "?, ?, ?, ?)}") ;
            cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
            cstmt.setString(2, key.part_no) ;
            cstmt.setString(3, key.spo_location) ;
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
		    doOnHandInvsSumDiff("A") ;
	    }
	    rowsInserted++ ;
    }

    public void     update() {
	    if (!no_op) {
	    doOnHandInvsSumDiff("C") ;	    
	    }
            rowsUpdated++ ;
    }
    public void     delete() {
	    if (!no_op) {
	doOnHandInvsSumDiff("D") ;
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
