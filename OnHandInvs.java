/*   $Author:   zf297a  $
   $Revision:   1.6  $
       $Date:   31 Jan 2008 12:18:14  $
   $Workfile:   OnHandInvs.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\OnHandInvs.java-arc  $
/*
/*   Rev 1.6   31 Jan 2008 12:18:14   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.5   19 Apr 2007 15:43:10   zf297a
/*Fixed F2 - added order by clause
/*Replaced System.out.println's with logger.debug's
/*Fixed the toString of the Key class
/*
/*   Rev 1.4   Oct 31 2005 12:05:52   zf297a
/*Fixed the equal method by using Double's compareTo method for the loc_sid field., which is defined as a double, instead of using the == operator.
/*
/*   Rev 1.3   Oct 31 2005 11:07:36   zf297a
/*Fixed the equal method by using Double's compareTo method for the sum_inv_qty field, which is defined as a double, instead of using the == operator.
/*
/*   Rev 1.2   10 Aug 2004 08:29:24   c970183
/*Added check for action_code != 'D' for all F1 (old master) TableSnapshots.
/*
/*   Rev 1.0   Jul 30 2004 12:29:34   c970183
/*Initial revision.
*/
import java.sql.* ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

public class OnHandInvs implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean no_op    = false;
    static int bufSize      = 40000 ;
    static int ageBufSize   = 40000 ;
    static int prefetchSize = 2000 ;
    static int debugThreshold = 10 ;
    static int showDiffThreshold = 10 ;


    static boolean debug ;

    static Logger logger = Logger.getLogger(OnHandInvs.class.getName());

    private static TableSnapshot F1 = null ; // amd_spare_parts
    private static TableSnapshot F2 = null ; // tmp_amd_spare_parts

    static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(OnHandInvs.class.getName()).getProperties() ;

       		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","40000") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","10") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","10") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","40000") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","2000") ).intValue() ;
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
            F1 = new TableSnapshot(bufSize, new OnHandInvsFactory("Select inv_qty sum_inv_qty, part_no, loc_sid from amd_on_hand_invs where action_code != 'D' order by part_no, loc_sid")) ;
            F2 = new TableSnapshot(bufSize, new OnHandInvsFactory("Select sum(inv_qty) sum_inv_qty, part_no, loc_sid from tmp_amd_on_hand_invs group by part_no, loc_sid order by part_no, loc_sid")) ;

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

        System.out.println("amd_on_hand_invs_in=" + F1.getRecsIn()) ;
        logger.info("amd_on_hand_invs_in=" + F1.getRecsIn()) ;
        System.out.println("tmp_amd_on_hand_invs=" + F2.getRecsIn()) ;
        logger.info("tmp_amd_on_hand_invs=" + F2.getRecsIn()) ;
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
        double loc_sid;

        public boolean equals(Object o) {
            Key k = (Key) o ;

            return (    (k.part_no.equals(part_no) )
                     && (new Double(k.loc_sid).compareTo(new Double(loc_sid)) == 0) ) ;
        }
        public int hashCode() {
            return  part_no.hashCode()
                   + new Double(loc_sid).hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (theKey.part_no + theKey.loc_sid).compareTo(
				    part_no + loc_sid) ;
        }
        public String toString() {
            return "part_no=" + part_no + " loc_sid="  + loc_sid ;
        }
    }
    Key key ;
    class Body {
		double sum_inv_qty ;

        public String toString() {
            return "sum_inv_qty=" + sum_inv_qty ;
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
          //  showOne = false ;
            result = new Double(b.sum_inv_qty).compareTo(new Double(sum_inv_qty)) == 0;
            showDiff(result, "sum_inv_qty", b.sum_inv_qty + "", sum_inv_qty + "") ;

            return result ;
        }
    }
    Body body ;

    OnHandInvs(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.part_no  = r.getString("part_no") ;
        key.loc_sid  = r.getDouble("loc_sid") ;

        body = new Body() ;
        logger.debug("Getting sum_inv_qty") ;
        body.sum_inv_qty = r.getDouble("sum_inv_qty") ;
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

    public void     insert() {
	if (!no_op) {
	    
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.InsertRow("
			+ "?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.part_no) ;
		    cstmt.setDouble(3, key.loc_sid) ;
		    cstmt.setDouble(4, body.sum_inv_qty) ;
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
		    System.exit(16) ;
		}
		logger.debug("Insert: key=" + key + " sum_inv_qty=" + body.sum_inv_qty) ;
	    }
        rowsInserted++ ;
    }

    public void     update() {
	if (!no_op) {
   	
	    try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.UpdateRow("
			+ "?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.part_no) ;
		    cstmt.setDouble(3, key.loc_sid) ;
		    cstmt.setDouble(4, body.sum_inv_qty) ;
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
		    System.exit(18) ;
		}
		logger.debug("Update: key=" + key + " sum_inv_qty=" + body.sum_inv_qty) ;
	}
        rowsUpdated++ ;
    }
    public void     delete() {
	if (!no_op) {
		
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.DeleteRow(?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.part_no) ;
		    cstmt.setDouble(3, key.loc_sid) ;
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
		    System.exit(20) ;
		}
		logger.debug("Delete: key=" + key + " sum_inv_qty=" + body.sum_inv_qty) ;
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
