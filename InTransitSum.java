import java.sql.* ;
import java.math.BigDecimal ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

/*   $Author:   zf297a  $
   $Revision:   1.3  $
       $Date:   31 Jan 2008 12:18:14  $
   $Workfile:   InTransitSum.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\InTransitSum.java.-arc  $
/*
/*   Rev 1.3   31 Jan 2008 12:18:14   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.2   Apr 24 2007 15:56:44   c402417
/*Remove order by clause in F1.
/*
/*   Rev 1.1   Jul 06 2006 12:11:12   zf297a
/*Fixed logger.  Fixed queries for F1 and F2.  Added debug code and enhanced generic error info that gets displayed.
/*
/*   Rev 1.0   Oct 31 2005 21:36:34   zf297a
/*Initial revision.
        */
public class InTransitSum implements Rec {
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
    static int showDiffThreshold = 1000 ;


    static Logger logger = Logger.getLogger(InTransitSum.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_in_transits_sum
    private static TableSnapshot F2 = null ; // tmp_amd_in_transits
    
    static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(InTransitSum.class.getName()).getProperties() ;

       		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","50000") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","1000") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","1000") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","50000") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","10") ).intValue() ;
		logger.debug("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" + prefetchSize + " no_op=" + no_op
				+ " debug=" + debug + " debugThreshold=" + debugThreshold + " showDiffThreshold=" + showDiffThreshold) ;

		if (debug) {
			System.out.println("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" + prefetchSize + " no_op=" + no_op + " debugThreshold=" + debugThreshold + " showDiffThreshold=" + showDiffThreshold) ;
		}

	} catch (java.io.IOException e) {
		System.err.println("InTransitSum: warning: " + e.getMessage()) ;
	} catch (java.lang.Exception e) {
		System.err.println("InTransitSum: warning: " + e.getMessage()) ;
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
            F1 = new TableSnapshot(bufSize, new InTransitSumFactory
            ("SELECT part_no, site_location, quantity sum_qty , serviceable_flag FROM AMD_IN_TRANSITS_SUM WHERE action_code != 'D' and site_location is not null")) ;
            F2 = new TableSnapshot(bufSize, new InTransitSumFactory
            ("Select part_no, Amd_Utils.getSpoLocation(to_loc_sid)site_location, sum(quantity) sum_qty, serviceable_flag  from tmp_amd_in_transits where amd_utils.getSpoLocation(to_loc_sid) is not null group by part_no,Amd_Utils.getSpoLocation(to_loc_sid), serviceable_flag")) ;

            logger.debug("start diff") ;
	    w.diff(F1, F2) ;
            logger.debug("end diff") ;
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
		System.err.println("Caught Generic Error") ;
		logger.error("Caught Generic Error") ;
		if (e.getMessage() != null) {
			System.err.println(e.getMessage()) ;
			logger.error(e.getMessage()) ;
		}
		e.printStackTrace() ;
           	if (F1 == null) {
			System.err.println("F1 not initialized.") ;
			logger.error("F1 not initialized.") ;
		}
            	if (F2 == null) {
			System.err.println("F2 not initialize.") ;
			logger.error("F2 not initialized.") ;
		}
	    	if (F1 != null && F2 != null) {
		    updateCounts() ;
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
		System.out.println("amd_in_transits_sum_in=" + F1.getRecsIn()) ;
		logger.info("amd_in_transits_sum_in=" + F1.getRecsIn()) ;
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
		String      part_no ;
		String  		site_location ;
		String		serviceable_flag ;


        public boolean equals(Object o) {
            Key k = (Key) o ;
			return ( (k.part_no.equals(part_no) )
			&& (k.site_location.equals(site_location)) )
			&& (k.serviceable_flag.equals(serviceable_flag));
		}

        public int hashCode() {
            return part_no.hashCode()
		+ site_location.hashCode()
		+ serviceable_flag.hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (theKey.part_no + theKey.site_location + theKey.serviceable_flag).compareTo(
				 part_no + site_location + serviceable_flag) ;
        }
        public String toString() {
            return "part_no_site_location_serviceable_flag =" + part_no + site_location + serviceable_flag  ;
        }
    }

    Key key ;
    class Body {
		double sum_qty ;


        public String toString() {
            return "sum_qty=" + sum_qty + " " ;
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
            result = new Double(b.sum_qty).compareTo(new Double (sum_qty)) == 0 ;
	    	showDiff(result, "sum_qty", b.sum_qty + "", sum_qty + "") ;
            return result ;
        }
    }

    Body body ;

    InTransitSum(ResultSet r) throws SQLException {
        try {
        	key = new Key() ;
		key.part_no = r.getString("PART_NO") ;
		key.site_location = r.getString("SITE_LOCATION") ;
		key.serviceable_flag = r.getString("SERVICEABLE_FLAG") ;

        	body = new Body() ;
        	body.sum_qty = r.getDouble("SUM_QTY") ;

        } catch (java.sql.SQLException e) {
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
	if ((rowsInserted + rowsDeleted + rowsUpdated) % debugThreshold == 0) {
	    	logger.debug("key.part_no=*" + key.part_no + "*".replace('\n','_').replace('\r','_')) ;
	        logger.debug("key.site_location=*" + key.site_location + "*".replace('\n','_').replace('\r','_')) ;
	        logger.debug("body.sum_qty=*" + body.sum_qty + "*".replace('\n','_').replace('\r','_')) ;
	        logger.debug("key.serviceable_flag=*" + key.serviceable_flag + "*".replace('\n','_').replace('\r','_')) ;
	}
	if (! no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.InsertRow("
			+ "?, ?, ?, ?)}") ;
			cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
			cstmt.setString(2, key.part_no) ;
			cstmt.setString(3, key.site_location) ;
			cstmt.setDouble(4, body.sum_qty) ;
			cstmt.setString(5, key.serviceable_flag) ;


		    cstmt.execute() ;

		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.err.println("amd_inventory_pkg.InsertRow failed with result = " + result) ;
			logger.fatal("amd_inventory_pkg.InsertRow failed with result = " + result) ;
			System.exit(result) ;
		    }
		    cstmt.close() ;
		} catch (java.sql.SQLException e) {
		    updateCounts() ;
		    System.err.println("amd_inventory_pkg.InsertRow failed to execute") ;
		    logger.fatal("amd_inventory_pkg.InsertRow failed to execute") ;
		    System.err.println(e.getMessage()) ;
		    logger.fatal(e.getMessage()) ;
		    System.exit(4) ;
		}
	}
        rowsInserted++ ;
    }

    public void     update() {
	 if (! no_op) {
		 try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.UpdateRow("
			+ "?, ?, ?,?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.part_no) ;
		    cstmt.setString(3, key.site_location) ;
		    cstmt.setDouble(4, body.sum_qty) ;
		    cstmt.setString(5,key.serviceable_flag) ;
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.err.println("amd_inventory_pkg.UpdateRow failed with result = " + result) ;
			logger.fatal("amd_inventory_reqs_pkg.UpdateRow failed with result = " + result) ;
			System.exit(result) ;
		    }
		    cstmt.close() ;
		} catch (java.sql.SQLException e) {
		    updateCounts() ;
		    System.err.println(e.getMessage()) ;
		    logger.fatal(e.getMessage()) ;
		    System.exit(4) ;
		}
	}
        rowsUpdated++ ;
    }
    public void     delete() {
	if (! no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_inventory.DeleteRow(?,?,?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.part_no) ;
		    cstmt.setString(3, key.site_location) ;
		    cstmt.setString(4,key.serviceable_flag) ;

		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.err.println("amd_inventory_pkg.DeleteRow failed with result = " + result) ;
			logger.fatal("amd_inventory_pkg.DeleteRow failed with result = " + result) ;
			System.exit(result) ;
		    }
		    cstmt.close() ;
		} catch (java.sql.SQLException e) {
		    updateCounts() ;
		    System.err.println(e.getMessage()) ;
		    logger.fatal(e.getMessage()) ;
		    System.exit(4) ;
		}
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
