import java.sql.* ;
import java.math.BigDecimal ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

/*   $Author:   zf297a  $
   $Revision:   1.2  $
       $Date:   31 Jan 2008 12:18:14  $
   $Workfile:   InTransit.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\InTransit.java-arc  $
/*
/*   Rev 1.2   31 Jan 2008 12:18:14   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.1   Oct 31 2005 12:41:44   c402417
/*using compareTo method for quantity and to_loc_sid .
        */
public class InTransit implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug ;
    static boolean no_op    = false;
    static int bufSize      = 2000 ;
    static int ageBufSize   = 2000 ;
    static int prefetchSize = 2000 ;
    static int debugThreshold = 1000 ;
    static int showDiffThreshold = 100 ;


    static Logger logger = Logger.getLogger(InTransit.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_in_transits
    private static TableSnapshot F2 = null ; // tmp_amd_in_transits

    static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(InTransit.class.getName()).getProperties() ;

       		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","2000") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","1000") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","100") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","2000") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","2000") ).intValue() ;
		logger.debug("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" + prefetchSize + " no_op=" + no_op
				+ " debug=" + debug + " debugThreshold=" + debugThreshold + " showDiffThreshold=" + showDiffThreshold) ;

		if (debug) {
			System.out.println("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" + prefetchSize + " no_op=" + no_op + " debugThreshold=" + debugThreshold + " showDiffThreshold=" + showDiffThreshold) ;
		}

	} catch (java.io.IOException e) {
		System.err.println("InTransit: warning: " + e.getMessage()) ;
	} catch (java.lang.Exception e) {
		System.err.println("InTransit: warning: " + e.getMessage()) ;
	}
    }

    public static void main(String[] args) {
        final int bufSize = 73 ;
        int agingBufsize ;

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
            F1 = new TableSnapshot(bufSize, new InTransitFactory("Select to_loc_sid, quantity, document_id, part_no, from_location, in_transit_date, serviceable_flag from amd_in_transits where action_code != 'D' order by document_id,part_no,to_loc_sid")) ;
            F2 = new TableSnapshot(bufSize, new InTransitFactory("Select to_loc_sid, quantity, document_id, part_no, from_location, in_transit_date, serviceable_flag from tmp_amd_in_transits order by document_id, part_no, to_loc_sid ")) ;
            logger.debug("start diff") ;

	    w.diff(F1, F2) ;
        } catch (SQLException e) {
            System.err.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;
			System.exit(2) ;
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;
			System.exit(4) ;
        } catch (Exception e) {
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
		System.out.println("amd_in_transits_in=" + F1.getRecsIn()) ;
		logger.info("amd_in_transits_in=" + F1.getRecsIn()) ;
	}
	if (F2 != null) {
		System.out.println("tmp_amd_in_transits_in=" + F2.getRecsIn()) ;
		logger.info("tmp_amd_in_transits_in=" + F2.getRecsIn()) ;
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
		String	    document_id ;
		String      part_no ;
		double  	to_loc_sid ;


        public boolean equals(Object o) {
            Key k = (Key) o ;
            return ( (k.document_id.equals(document_id) )
			&& (k.part_no.equals(part_no) )
			&& (new Double (k.to_loc_sid).compareTo(new Double(to_loc_sid)) == 0));

        }
        public int hashCode() {
            return part_no.hashCode()
            + new Double(to_loc_sid).hashCode()
		+ document_id.hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (theKey.document_id + theKey.part_no + theKey.to_loc_sid).compareTo(
				document_id + part_no + to_loc_sid) ;
        }
        public String toString() {
            return "InTransit =" + document_id + part_no + to_loc_sid  ;
        }
    }
    Key key ;
    class Body {
		double	quantity ;
		String	from_location;
		java.sql.Date 	in_transit_date ;
		String   serviceable_flag ;


        public String toString() {
            return "quantity=" + quantity + " " +
                    "from_location=" + from_location + " " +
                    "in_transit_date=" + in_transit_date + " " +
                    "serviceable_flag=" + serviceable_flag + " ";
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
            boolean resultField ;
            result = (new Double(b.quantity).compareTo (new Double(quantity)) == 0) ;
	    	showDiff(result, "quantity", b.quantity + "", quantity + "") ;
            resultField = b.from_location.equals(from_location) ;
            	showDiff(resultField, "from_location", b.from_location + "", from_location + "") ;
	    result = resultField && result ;
	    resultField = (b.in_transit_date.compareTo (in_transit_date) == 0 ) ;
	    	showDiff(resultField, "in_transit_date", b.in_transit_date + "", in_transit_date + "") ;
	    /*
	    	result = result && b.serviceable_flag == serviceable_flag ;
	    */
	    result = resultField && result ;
	    resultField = b.serviceable_flag.equals(serviceable_flag) ;
	    	showDiff(result, "serviceable_flag", b.serviceable_flag + "", serviceable_flag + "") ;
	    result = resultField && result ;
            return result ;
        }
    }
    Body body ;

    InTransit(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.document_id = r.getString("DOCUMENT_ID") ;
		key.part_no = r.getString("PART_NO") ;
		key.to_loc_sid = r.getDouble("TO_LOC_SID") ;

        body = new Body() ;
		logger.debug("Getting quantity") ;
        body.quantity = r.getDouble("QUANTITY") ;
		logger.debug("Getting from_location") ;
        body.from_location = r.getString("FROM_LOCATION") ;
        logger.debug("Getting in_transit_date") ;
        body.in_transit_date = r.getDate("IN_TRANSIT_DATE") ;
        logger.debug("Getting serviceable_flag") ;
        body.serviceable_flag = r.getString("SERVICEABLE_FLAG") ;
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
	if (! no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.InsertRow("
			+ "?, ?, ?, ?, ?, ?, ?)}") ;
			cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
			logger.debug("key.to_loc_sid=*" + key.to_loc_sid + "*") ;
			cstmt.setDouble(2, key.to_loc_sid) ;
			logger.debug("body.quantity=*" + body.quantity + "*") ;
			cstmt.setDouble(3, body.quantity) ;
			logger.debug("key.document_id=*" + key.document_id + "*") ;
			cstmt.setString(4, key.document_id) ;
			logger.debug("key.part_no=*" + key.part_no + "*") ;
			cstmt.setString(5, key.part_no) ;
			logger.debug("body.from_location=*" + body.from_location + "*") ;
			cstmt.setString(6, body.from_location) ;
				cstmt.setDate(7, body.in_transit_date) ;
				logger.debug("body.in_transitDate=*" + body.in_transit_date + "*") ;
				cstmt.setString(8, body.serviceable_flag) ;

		    cstmt.execute() ;

		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.err.println("amd_inventory_pkg.InsertRow failed with result = " + result) ;
			logger.fatal("amd_inventory_pkg.InsertRow failed with result = " + result) ;
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
        if (debug) {
            logger.debug("Insert: key=" + key + " body=" + body) ;
        }
        rowsInserted++ ;
    }

    public void     update() {
	 if (! no_op) {
		 try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.UpdateRow("
			+ "?, ?, ?, ?, ?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setDouble(2, key.to_loc_sid) ;
		    cstmt.setDouble(3, body.quantity) ;
		    cstmt.setString(4, key.document_id) ;
		    cstmt.setString(5, key.part_no) ;
		    cstmt.setString(6, body.from_location) ;
		    cstmt.setDate(7, body.in_transit_date) ;
		    cstmt.setString(8, body.serviceable_flag) ;

		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.err.println("amd_inventory.UpdateRow failed with result = " + result) ;
			logger.fatal("amd_inventory.UpdateRow failed with result = " + result) ;
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
        if (debug) {
            logger.debug("Update: key=" + key + " body=" + body) ;
        }
        rowsUpdated++ ;
    }
    public void     delete() {
	if (! no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.DeleteRow(?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.document_id) ;
		    cstmt.setString(3, key.part_no) ;
		    cstmt.setDouble(4, key.to_loc_sid) ;

		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.err.println("amd_inventory.DeleteRow failed with result = " + result) ;
			logger.fatal("amd_inventory.DeleteRow failed with result = " + result) ;
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
        if (debug) {
            logger.debug("Delete: key=" + key) ;
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
