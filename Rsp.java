/*   $Author:   zf297a  $
   $Revision:   1.2  $
       $Date:   31 Jan 2008 12:18:16  $
   $Workfile:   Rsp.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\Rsp.java.-arc  $
/*
/*   Rev 1.2   31 Jan 2008 12:18:16   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.1   Sep 27 2006 09:33:24   c402417
/*Changed "==" to "compareTo" when compare the rsp_qty and rsp_level.
/*
/*   Rev 1.0   May 16 2006 12:14:40   c402417
/*Initial revision.
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

public class Rsp implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug ;
    static boolean no_op    = false;
    static int bufSize      = 2000 ;
    static int ageBufSize   = 2000 ;
    static int prefetchSize = 200 ;
    static int debugThreshold = 10 ;
    static int showDiffThreshold = 10 ;
	    

    static Logger logger = Logger.getLogger(Rsp.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_rsp
    private static TableSnapshot F2 = null ; // tmp_amd_rsp

    static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(Rsp.class.getName()).getProperties() ;

       		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","2000") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","10") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","10") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","2000") ).intValue() ;
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
            F1 = new TableSnapshot(bufSize, new RspFactory("Select part_no, loc_sid, rsp_inv, rsp_level from amd_rsp where action_code != 'D' order by part_no, loc_sid", amd, prefetchSize)) ;
            F2 = new TableSnapshot(bufSize, new RspFactory("Select part_no, loc_sid, rsp_inv, rsp_level from tmp_amd_rsp order by part_no,loc_sid", amd, prefetchSize)) ;
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

        System.out.println("amd_rsp_in=" + F1.getRecsIn()) ;
        logger.info("amd_rsp_in=" + F1.getRecsIn()) ;
        System.out.println("tmp_amd_rsp=" + F2.getRecsIn()) ;
        logger.info("tmp_amd_rsp=" + F2.getRecsIn()) ;
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
                     &&  ( new Double(k.loc_sid).compareTo(new Double(loc_sid)) == 0 ))  ;
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
            return "part_no_loc_sid =" + part_no + loc_sid ;
        }
    }
    Key key ;
    class Body {
		double rsp_inv ;
		double rsp_level;

        public String toString() {
            return "rsp_inv=" + rsp_inv + " " +
            "rsp_level=" + rsp_level + " ";
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
            result = (new Double(b.rsp_inv).compareTo(new Double(rsp_inv)) == 0) ;
            showDiff(result, "rsp_inv", b.rsp_inv + "", rsp_inv + "") ;
            result = result && (new Double(b.rsp_level).compareTo(new Double(rsp_level)) == 0) ;
            showDiff(result, "rsp_level", b.rsp_level + "", rsp_level + "") ;

            return result ;
        }
    }
    Body body ;

    Rsp(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.part_no  = r.getString("part_no") ;
        key.loc_sid  = r.getDouble("loc_sid") ;

        body = new Body() ;
        logger.debug("Getting rsp_inv") ;
        body.rsp_inv = r.getDouble("rsp_inv") ;
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
	if (result) {
		logger.debug("keys equal  " + key + " " + r.getKey()) ;
	}
	else {
		logger.debug("keys not equal "  + key + " " + r.getKey()) ;
	}
        logger.debug("Getting rsp_inv") ;
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
			"{? = call amd_inventory.RspInsertRow("
			+ "?, ?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.part_no) ;
		    cstmt.setDouble(3, key.loc_sid) ;
		    cstmt.setDouble(4, body.rsp_inv) ;
		    cstmt.setDouble(5, body.rsp_level) ;
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
		if (debug) {
		    System.out.println("Insert: key=" + key + " body=" + body) ;
		}
		logger.info("Insert: key=" + key + " rsp_inv=" + body.rsp_inv) ;
	    }
        rowsInserted++ ;
    }

    public void     update() {
	if (!no_op) {
		 try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.RspUpdateRow("
			+ "?, ?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.part_no) ;
		    cstmt.setDouble(3, key.loc_sid) ;
		    cstmt.setDouble(4, body.rsp_inv) ;
		    cstmt.setDouble(5, body.rsp_level) ;
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
		if (debug) {
		    System.out.println("Update: key=" + key + " body=" + body) ;
		}
		logger.info("Update: key=" + key + " rsp_inv=" + body.rsp_inv) ;
	}
        rowsUpdated++ ;
    }
    public void     delete() {
	if (!no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.RspDeleteRow(?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.part_no) ;
		    cstmt.setDouble(3, key.loc_sid) ;
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_inventory.RspDeleteRow failed with result = " + result) ;
			logger.fatal("amd_inventory.RspDeleteRow failed with result = " + result) ;
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
		if (debug) {
		    System.out.println("Delete: key=" + key) ;
		}
		logger.info("Delete: key=" + key + " rsp_inv=" + body.rsp_inv) ;
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
