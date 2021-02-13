import java.sql.* ;
import java.math.BigDecimal ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

/*   $Author:   zf297a  $
   $Revision:   1.1  $
       $Date:   31 Jan 2008 12:18:14  $
   $Workfile:   AmdReqs.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\AmdReqs.java.-arc  $
/*
/*   Rev 1.1   31 Jan 2008 12:18:14   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.0   Dec 07 2005 14:30:52   zf297a
/*Initial revision.
        */
public class AmdReqs implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug ;
    static boolean no_op    = false;
    static int bufSize      = 5000 ;
    static int ageBufSize   = 5000 ;
    static int prefetchSize = 5000 ;
    static int debugThreshold = 100 ;
    static int showDiffThreshold = 100 ;


    static Logger logger = Logger.getLogger(AmdReqs.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_reqs
    private static TableSnapshot F2 = null ; // tmp_amd_reqs

    static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(AmdReqs.class.getName()).getProperties() ;

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
		System.err.println("AmdReqs: warning: " + e.getMessage()) ;
	} catch (java.lang.Exception e) {
		System.err.println("AmdReqs: warning: " + e.getMessage()) ;
	}
    }

    public static void main(String[] args) {
        final int bufSize = 73 ;
        int agingBufsize ;

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
            F1 = new TableSnapshot(bufSize, new AmdReqsFactory("Select req_id, part_no, loc_sid, quantity_due from amd_reqs where action_code != 'D' order by req_id, part_no,loc_sid ",AmdConnection.instance(),prefetchSize)) ;
            F2 = new TableSnapshot(bufSize, new AmdReqsFactory("Select req_id, part_no, loc_sid, quantity_due from tmp_amd_reqs order by req_id,part_no,loc_sid ",AmdConnection.instance(),prefetchSize)) ;
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
        System.out.println("amd_reqs_in=" + F1.getRecsIn()) ;
        logger.info("amd_reqs_in=" + F1.getRecsIn()) ;
        System.out.println("tmp_amd_reqs_in=" + F2.getRecsIn()) ;
        logger.info("tmp_amd_reqs_in=" + F2.getRecsIn()) ;
        System.out.println("rows inserted=" + rowsInserted) ;
        logger.info("rows inserted=" + rowsInserted) ;
        System.out.println("rows updated=" + rowsUpdated) ;
        logger.info("rows updated=" + rowsUpdated) ;
        System.out.println("rows deleted=" + rowsDeleted) ;
        logger.info("rows deleted=" + rowsDeleted) ;
        System.out.println("end time: " + now()) ;
    }

    class Key implements Comparable {
		String	    req_id ;
		String		part_no ;
		int			loc_sid ;


        public boolean equals(Object o) {
            Key k = (Key) o ;
            return ( (k.req_id.equals(req_id) )
            && (k.part_no.equals(part_no))
            && (k.loc_sid == loc_sid) ) ;

        }
        public int hashCode() {
            return req_id.hashCode()
        + new String(part_no).hashCode()
		+ new Double(loc_sid).hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (theKey.req_id + theKey.part_no + theKey.loc_sid).compareTo(
				req_id + part_no + loc_sid) ;
        }
        public String toString() {
            return "AmdReqs =" + req_id + part_no + loc_sid ;
        }
    }
    Key key ;
    class Body {
		int	quantity_due ;


        public String toString() {
            return "quantity_due=" + quantity_due + " " ;

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
            result = b.quantity_due == quantity_due ;
	    	showDiff(result, "quantity_due", b.quantity_due + "", quantity_due + "") ;
          return result ;
        }
    }
    Body body ;

    AmdReqs(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.req_id = r.getString("REQ_ID") ;
        key.part_no = r.getString("PART_NO") ;
		key.loc_sid = r.getInt("LOC_SID") ;


        body = new Body() ;
		logger.debug("Getting quantity") ;
        body.quantity_due = r.getInt("QUANTITY_DUE") ;
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

    public void  insert() {
        try {
            CallableStatement cstmt = amd.c.prepareCall(
                "{? = call amd_reqs_pkg.InsertRow("
                + "?, ?, ?, ?)}") ;
	    	cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
	    	logger.debug("key.req_id=*" + key.req_id + "*") ;
	        cstmt.setString(2, key.req_id) ;
	        logger.debug("key.part_no=*" + key.part_no + "*") ;
	        cstmt.setString(3,key.part_no) ;
	        logger.debug("key.loc_sid=*" + key.loc_sid + "*") ;
	    	cstmt.setInt(4, key.loc_sid) ;
	        logger.debug("body.quantity_due=*" + body.quantity_due + "*") ;
	        cstmt.setInt(5, body.quantity_due) ;
            cstmt.execute() ;

            int result = cstmt.getInt(1) ;
            if (result > 0) {
                updateCounts() ;
                System.out.println("amd_reqs_pkg.InsertRow failed with result = " + result) ;
                logger.fatal("amd_reqs_pkg.InsertRow failed with result = " + result) ;
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
        logger.info("Insert: key=" + key + " quantity_due=" + body.quantity_due) ;
        rowsInserted++ ;
    }

    public void     update() {
         try {
            CallableStatement cstmt = amd.c.prepareCall(
                "{? = call amd_reqs_pkg.UpdateRow("
                + "?, ?, ?, ?)}") ;
            cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
            cstmt.setString(2, key.req_id) ;
            cstmt.setString(3, key.part_no) ;
            cstmt.setInt(4, key.loc_sid) ;
            cstmt.setInt(5, body.quantity_due) ;

            cstmt.execute() ;
            int result = cstmt.getInt(1) ;
            if (result > 0) {
                updateCounts() ;
                System.out.println("amd_reqs.UpdateRow failed with result = " + result) ;
                logger.fatal("amd_reqs.UpdateRow failed with result = " + result) ;
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
        logger.info("Update: key=" + key + " quantity_due=" + body.quantity_due) ;
        rowsUpdated++ ;
    }
    public void     delete() {
        try {
            CallableStatement cstmt = amd.c.prepareCall(
                "{? = call amd_reqs_pkg.DeleteRow(?, ?, ?)}") ;
            cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
            cstmt.setString(2, key.req_id) ;
            cstmt.setString(3, key.part_no) ;
            cstmt.setInt(4, key.loc_sid) ;

            cstmt.execute() ;
            int result = cstmt.getInt(1) ;
            if (result > 0) {
                updateCounts() ;
                System.out.println("amd_reqs.DeleteRow failed with result = " + result) ;
                logger.fatal("amd_reqs.DeleteRow failed with result = " + result) ;
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
        logger.info("Delete: key=" + key + " quantity_due=" + body.quantity_due) ;
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
