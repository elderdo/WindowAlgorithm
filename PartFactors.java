/*   $Author:   zf297a  $
   $Revision:   1.1  $
       $Date:   12 Aug 2008 13:29:20  $
   $Workfile:   PartFactors.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\PartFactors.java.-arc  $
/*
/*   Rev 1.1   12 Aug 2008 13:29:20   zf297a
/*Added properties file and used the new PartFactorsFactory constructor that take the connection object and prefetch value.
/*
/*   Rev 1.0   Mar 07 2006 23:55:26   zf297a
/*Initial revision.
*/
import java.sql.* ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

public class PartFactors implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug ;
    static boolean no_op    = false;
    static int bufSize      = 500000 ;
    static int ageBufSize   = 500000 ;
    static int prefetchSize = 200 ;
    static int debugThreshold = 10 ;
    static int showDiffThreshold = 500 ;
    static java.sql.Date runStartDate ;
	   

    static Logger logger = Logger.getLogger(PartFactors.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_part_factors
    private static TableSnapshot F2 = null ; // tmp_amd_part_factors

// loads the parameter from this class's properties file if they exist
    static private void loadParams() {
	try {

		java.util.Properties p        = new AppProperties(PartFactors.class.getName()).getProperties() ;

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
		CallableStatement cstmt = amd.c.prepareCall(
				"{? = call amd_batch_pkg.getLastStartTime}") ;
            	cstmt.registerOutParameter(1, Types.DATE) ;
		cstmt.execute() ;
	        runStartDate = cstmt.getDate(1) ;
		
		logger.debug("Creating F1 and F2") ;
		String sqlA = "select a2a.*, ansi.criticality_cleaned, ansi.criticality, ansi.criticality_changed," + 
			  "ansi.last_update_dt as last_update_date from " ; 
		String sqlC = " a2a, amd_national_stock_items ansi, amd_spare_parts asp " +
			  "where a2a.part_no = asp.part_no and asp.nsn = ansi.nsn " +
			  "and ansi.action_code != 'D' and a2a.action_code != 'D' and asp.action_code != 'D' " +
			  " order by a2a.part_no " ;
		String sqlOld= sqlA + "amd_part_factors" + sqlC ; 
		String sqlNew= sqlA + "tmp_amd_part_factors" + sqlC ; 
		if (debug) {
			System.out.println("sqlOld:" + sqlOld) ;
			System.out.println("sqlNew:" + sqlNew) ;
		}
            	F1 = new TableSnapshot(bufSize, new PartFactorsFactory(sqlOld, amd, prefetchSize) ) ;
            	F2 = new TableSnapshot(bufSize, new PartFactorsFactory(sqlNew, amd, prefetchSize) ) ;
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
		System.out.println("Exception e: " + e.getMessage()) ;
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
        System.out.println("amd_part_factors_in=" + F1.getRecsIn()) ;
        logger.info("amd_part_factors_in=" + F1.getRecsIn()) ;
        System.out.println("tmp_amd_part_factors=" + F2.getRecsIn()) ;
        logger.info("tmp_amd_part_factors=" + F2.getRecsIn()) ;
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
            return (    (k.part_no.equals(part_no))
                     && (k.loc_sid == loc_sid) ) ;
        }
        public int hashCode() {
            return  	new String(part_no).hashCode()
                   + new Double(loc_sid).hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return new String(theKey.part_no + theKey.loc_sid).compareTo(
				    new String(part_no + loc_sid)) ;
        }
        public String toString() {
            return "part_no + loc_sid =" + part_no + " + " + loc_sid ;
        }
    }
    Key key ;
    class Body {
	double cmdmd_rate ;
	boolean cmdmd_rate_isnull ;
	double pass_up_rate ;
	boolean pass_up_rate_isnull ;
	double rts ;
	boolean rts_isnull ;
	double criticality ;
	boolean criticality_isnull ;
	double criticality_cleaned ;
	boolean criticality_cleaned_isnull ;
	String criticality_changed ;
	java.sql.Date last_update_date ;

		
        public String toString() {
		return " cmdmd_rate=" + cmdmd_rate + 
		" pass_up_rate=" + pass_up_rate + 
		" rts=" + rts + 
		" criticality=" + criticality + 
		" criticality_changed=" + criticality_changed + 
		" criticality_cleaned=" + criticality_cleaned + 
		" last_update_date=" + last_update_date ;
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
		//date include time in comparison
	boolean equal(java.sql.Date date1, java.sql.Date date2) {
             	return date1.compareTo(date2) == 0;
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
				
	/* test */
        boolean showOne = true;
	private void showDiff(boolean result, String fieldName, String field1, String field2, String field3, String field4) {
	        if (debug) {
        	       	if (!result) {
	        	    System.out.println("key = " + key + " field: " + fieldName + "\t<" + field1 + "><" + field2 + "> isNull:<" + field3 + "><" + field4 + ">") ;
		            logger.info("key = " + key + " field: " + fieldName + "\t<" + field1 + "><" + field2 + "> isNull:<" + field3 + "><" + field4 + ">") ;
                	}
	        }
        	if (!result && !showOne) {
                	showOne = true ;
		        logger.info("key = " + key + " field: " + fieldName + "\t<" + field1 + "><" + field2 + "> isNull:<" + field3 + "><" + field4 + ">") ;
		}  
        }
	private void showDiff(boolean result, String fieldName, String field1, String field2) {
	        if (debug) {
        	       	if (!result) {
	        	    System.out.println("key = " + key + " field: " + fieldName + "\t<" + field1 + "><" + field2 + ">") ;
		            logger.info("key = " + key + " field: " + fieldName + "\t<" + field1 + "><" + field2 + ">") ;
                	}
	        }
        	if (!result && !showOne) {
                	showOne = true ;
	                logger.info("key = " + key + " field: " + fieldName + "\t<" + field1 + "><" + field2 + ">") ;
		}  
        }

	public double nvl(boolean isNull, double in, double newOut) {
		if (isNull) {
			return newOut ;
		}else {
			return in ;
		}
	}

	public String nvl(String in, String newOut) {
		return newOut ;
	}



	// revisit
	public boolean isLatestRun(java.sql.Date pDate) {
		if (debug) {
			System.out.println("<" + pDate + "><" + runStartDate + ">") ;
		}
		if (runStartDate == null) {
		    return false ;
		}else if (pDate.compareTo(runStartDate) >= 0){
		    return true ;
		}else {
		    return false ;
		}    
	}

	public String getPreferred(String first, String second) {
		if (first != null && first != ""){
			return  first ;
		}else {
			return second;
		}
	}
	public double getPreferred(double first, boolean firstNull, double second, boolean secondNull) {
		if (!firstNull){
			return  first ;
		}else if (!secondNull) { 
			return second;
		}else {
			return -1.0;
		}
	}
        public boolean equals(Object o) {
            	Body b = (Body) o ;
            	boolean result = true ;
		boolean resultField = true;
		String YES = "Y" ;	
		String NO = "N" ;	
            	showOne = true;

		resultField = equal(b.cmdmd_rate, b.cmdmd_rate_isnull, cmdmd_rate, cmdmd_rate_isnull) ;
			showDiff(resultField, "cmdmd_rate", b.cmdmd_rate + "", cmdmd_rate + "", b.cmdmd_rate_isnull + "", cmdmd_rate_isnull + "" ) ;
		result = resultField && result ;
		resultField = equal(b.pass_up_rate, b.pass_up_rate_isnull, pass_up_rate, pass_up_rate_isnull) ;
			showDiff(resultField, "pass_up_rate", b.pass_up_rate + "", pass_up_rate + "", b.pass_up_rate_isnull + "", pass_up_rate_isnull + "" ) ;
		result = resultField && result ;
		resultField = equal(b.rts, b.rts_isnull, rts, rts_isnull) ;
			showDiff(resultField, "rts", b.rts + "", rts + "", b.rts_isnull + "", rts_isnull + "" ) ;
		result = resultField && result ;
		
		if (
		       (
		        criticality_changed.toUpperCase().equals(YES)
		       ) && isLatestRun(last_update_date)  
		   ) 
	       	{
			result = false ;
		}
            	return result ;
        }
    }
    Body body ;

    PartFactors(ResultSet r) throws SQLException {
        try {
	        key = new Key() ;
	        key.part_no  = r.getString("part_no") ;
        	key.loc_sid  = r.getDouble("loc_sid") ;

	        body = new Body() ;
		body.cmdmd_rate=r.getDouble("cmdmd_rate") ;
		body.cmdmd_rate_isnull=r.wasNull() ;
		body.pass_up_rate=r.getDouble("pass_up_rate") ;
		body.pass_up_rate_isnull=r.wasNull() ;
		body.rts=r.getDouble("rts") ;
		body.rts_isnull=r.wasNull() ;
		body.criticality=r.getDouble("criticality") ;
		body.criticality_isnull=r.wasNull() ;
		body.criticality_changed=r.getString("criticality_changed") ;
		body.criticality_cleaned=r.getDouble("criticality_cleaned") ;
		body.criticality_cleaned_isnull=r.wasNull() ;		
		body.last_update_date=r.getDate("last_update_date") ;
        	
        }
        catch (java.sql.SQLException e) {
		updateCounts() ;
		System.out.println("Result Set: " + e.getMessage() ) ;
		logger.fatal(e.getMessage()) ;
		System.exit(10) ;
        }
    }

    public Comparable getKey() {
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
            int parameterIndex, double value, boolean isNull) {
        try {
            if (isNull) {
                cstmt.setNull(parameterIndex, java.sql.Types.DOUBLE) ;
            }
            else {
                cstmt.setDouble(parameterIndex, value) ;
            }
        } catch (java.sql.SQLException e) {
            updateCounts() ;
            System.out.println("Set double: " + e.getMessage()) ;
            logger.fatal(e.getMessage()) ;
            System.exit(12) ;
        }
    }


    private void setInt(CallableStatement cstmt,
        int parameterIndex, int value, boolean isNull) {
        try {
            if (isNull)
                cstmt.setNull(parameterIndex, java.sql.Types.NUMERIC) ;
            else
                cstmt.setInt(parameterIndex, value) ;
        } catch (java.sql.SQLException e) {
            updateCounts() ;
            System.out.println("Set Int: " + e.getMessage()) ;
            logger.fatal(e.getMessage()) ;
            System.exit(14) ;
        }
    }

    public void insert() {
        try {
            	CallableStatement cstmt = amd.c.prepareCall(
                "{? = call amd_part_factors_pkg.InsertRow("
                + "?, ?, ?, ?, ?, ?, ?, ?"
		+ ")}") ;
            	cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
            	cstmt.setString(2, key.part_no) ;
            	cstmt.setDouble(3, key.loc_sid) ;

		setDouble(cstmt, 4, body.pass_up_rate, body.pass_up_rate_isnull) ; 
		setDouble(cstmt, 5, body.rts, body.rts_isnull) ; 
		setDouble(cstmt, 6, body.cmdmd_rate, body.cmdmd_rate_isnull) ; 
		
	
		setDouble(cstmt, 7, body.criticality, body.criticality_isnull) ; 
		cstmt.setString(8, body.criticality_changed) ; 
		setDouble(cstmt, 9, body.criticality_cleaned, body.criticality_cleaned_isnull) ; 
		cstmt.execute() ;
            int result = cstmt.getInt(1) ;
            if (result > 0) {
                updateCounts() ;
                System.out.println("amd_part_factors_pkg.InsertRow failed with result = " + result) ;
                logger.fatal("amd_part_factors_pkg.InsertRow failed with result = " + result) ;
                System.exit(result) ;
            }
            cstmt.close() ;
        }
        catch (java.sql.SQLException e) {
            updateCounts() ;
            System.out.println("insert: " + e.getMessage()) ;
            logger.fatal(e.getMessage()) ;
            System.exit(16) ;
        }
        if (debug) {
            System.out.println("Insert: key=" + key + " body=" + body) ;
        }
        logger.info("Insert: key=" + key + " body=" + body) ;
        rowsInserted++ ;
    }

    public void update() {
         try {
            	CallableStatement cstmt = amd.c.prepareCall(
                "{? = call amd_part_factors_pkg.UpdateRow("
                + "?, ?, ?, ?, ?, ?, ?, ?"
		+ ")}") ;
            	cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
            	cstmt.setString(2, key.part_no) ;
            	cstmt.setDouble(3, key.loc_sid) ;

		setDouble(cstmt, 4, body.pass_up_rate, body.pass_up_rate_isnull) ; 
		setDouble(cstmt, 5, body.rts, body.rts_isnull) ; 
		setDouble(cstmt, 6, body.cmdmd_rate, body.cmdmd_rate_isnull) ; 

		setDouble(cstmt, 7, body.criticality, body.criticality_isnull) ; 
		cstmt.setString(8, body.criticality_changed) ; 
		setDouble(cstmt, 9, body.criticality_cleaned, body.criticality_cleaned_isnull) ; 
		cstmt.execute() ;
            	int result = cstmt.getInt(1) ;
            	if (result > 0) {
               		updateCounts() ;
	                System.out.println("amd_part_factors_pkg.UpdateRow failed with result = " + result) ;
        	        logger.fatal("amd_part_factors_pkg.UpdateRow failed with result = " + result) ;
                	System.exit(result) ;
            	}
            	cstmt.close() ;
        }
        catch (java.sql.SQLException e) {
            updateCounts() ;
            System.out.println("update: " + e.getMessage()) ;
            logger.fatal(e.getMessage()) ;
            System.exit(18) ;
        }
        if (debug) {
            System.out.println("Update: key=" + key + " body=" + body) ;
        }
        logger.info("Update: key=" + key + " body=" + body) ;
        rowsUpdated++ ;
    }
    public void delete() {
        try {
		CallableStatement cstmt = amd.c.prepareCall(
                "{? = call amd_part_factors_pkg.DeleteRow(" 
	        + "?, ?, ?, ?, ?, ?, ?, ?"
		+ ")}") ;
            	cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
            	cstmt.setString(2, key.part_no) ;
            	cstmt.setDouble(3, key.loc_sid) ;

		setDouble(cstmt, 4, body.pass_up_rate, body.pass_up_rate_isnull) ; 
		setDouble(cstmt, 5, body.rts, body.rts_isnull) ; 
		setDouble(cstmt, 6, body.cmdmd_rate, body.cmdmd_rate_isnull) ; 

	
		setDouble(cstmt, 7, body.criticality, body.criticality_isnull) ; 
		cstmt.setString(8, body.criticality_changed) ; 
		setDouble(cstmt, 9, body.criticality_cleaned, body.criticality_cleaned_isnull) ; 
	
		cstmt.execute() ;
            	int result = cstmt.getInt(1) ;
            	if (result > 0) {
               		updateCounts() ;
                	System.out.println("amd_part_factors_pkg.DeleteRow failed with result = " + result) ;
                	logger.fatal("amd_part_factors_pkg.DeleteRow failed with result = " + result) ;
                	System.exit(result) ;
            	}
            	cstmt.close() ;
        }
        catch (java.sql.SQLException e) {
            updateCounts() ;
            System.out.println("delete: " + e.getMessage()) ;
            logger.fatal(e.getMessage()) ;
            System.exit(20) ;
        }
        if (debug) {
            System.out.println("Delete: key=" + key) ;
        }
        logger.info("Delete: key=" + key ) ;
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


	public static String nowNoTime() {
		Calendar cal = Calendar.getInstance(TimeZone.getDefault());
/*		String DATE_FORMAT = "M/dd/yy hh:mm:ss a"; */
		String DATE_FORMAT = "M/dd/yy"; 
		java.text.SimpleDateFormat sdf =
				new java.text.SimpleDateFormat(DATE_FORMAT);
		sdf.setTimeZone(TimeZone.getDefault());
		return sdf.format(cal.getTime());
	}

	public String getDateString (java.sql.Date pDate) {
		String DATE_FORMAT = "M/dd/yy"; 
		java.text.SimpleDateFormat sdf =
				new java.text.SimpleDateFormat(DATE_FORMAT);
		sdf.setTimeZone(TimeZone.getDefault());
		return sdf.format(pDate);
	}
}
