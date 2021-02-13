import java.sql.* ;
import java.math.BigDecimal ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;
import java.util.Properties ;
import java.io.FileInputStream ;
import java.io.*;



/*   $Author:   zf297a  $
   $Revision:   1.5  $
       $Date:   10 Jan 2009 17:46:28  $
   $Workfile:   ConfirmedRequestLine.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\ConfirmedRequestLine.java.-arc  $
/*
/*   Rev 1.5   10 Jan 2009 17:46:28   zf297a
/*For equal method exit routine as soon as any field is not equal.  Change convertDate's  date formating to dd-MMM-yyyy.  
/*
/*   Rev 1.4   09 Jan 2009 12:40:20   zf297a
/*Fixed the SpoBAtch instance by fixing the table name and made it  X_IMP_CONFIRMED_REQUEST_LINE.  Fixed the writing of proposed_request to the flat file by checking to is if the value is null and ouputing "null" if it is null.
/*
/*   Rev 1.3   02 Dec 2008 11:23:48   zf297a
/*Removed s variable that was not be used.
/*
/*   Rev 1.2   10 Jul 2008 20:47:00   zf297a
/*Fixed format of csv file - added double quotes and formated 2 date columns.
/*
/*   Rev 1.1   10 Jul 2008 08:38:56   zf297a
/*Fixed handling of exceptions for the FileOutputStream: issue exception message and exit with a retun code of 4.
/*
/*   Rev 1.0   13 May 2008 21:54:34   zf297a
/*Initial revision.

	*/

public class ConfirmedRequestLine implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static SpoConnection spo = SpoConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;
    static int frequency = 100 ;

    static boolean debug ;
    static boolean useTestBatch = false;
    static boolean no_op    = false;
    static int bufSize      = 500000 ;
    static int ageBufSize   = 500000 ;
    static int prefetchSize = 200 ;
    static int debugThreshold = 10 ;
    static int showDiffThreshold = 500 ;
    static String F1Instance = "SPOC17V2";
    static String F1InstanceSQL = F1Instance + ".";
    static String F2Instance = "";
    static String F2InstanceSQL = "";

    static Logger logger = Logger.getLogger(ConfirmedRequestLine.class.getName());

    static FileOutputStream o1;
    static PrintStream p; // declare a print stream object  
    static long batchNum;
    static PreparedStatement pstmt;
    static long transTime = 0;        // keeps track of time spent in transactions
    static long flatFileTime = 0;     // keeps track of time spent writing flatfiles
    static String flatFileName;       // name of flat file to output   


    private static TableSnapshot F1 = null ; // v_confirmed_request_line
    private static TableSnapshot F2 = null ; // x_confirmed_request_line_v

    // loads the parameter from this class's properties file if they exist
    static private void loadParams() {
	try {

		java.util.Properties p        = new AppProperties(ConfirmedRequestLine.class.getName()).getProperties() ;

		useTestBatch = p.getProperty("useTestBatch", "false").equals("true");
		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","200") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","10") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","10") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","200") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","200") ).intValue() ;
		flatFileName = String.valueOf( p.getProperty("dumpFile", "ConfirmedRequestLine.csv"));
		F1Instance = String.valueOf( p.getProperty("F1Instance", "SPOC17V2"));
		F2Instance = String.valueOf( p.getProperty("F2Instance", ""));
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
		e.printStackTrace() ;
	} catch (java.lang.Exception e) {
		System.err.println("Warning: " + e.getMessage()) ;
		e.printStackTrace() ;
	}
    }


    public static void main(String[] args) {
        loadParams();
	// adds an appropraite . after the instance if the instance is not empty to be used in SQL statement
	if (F1Instance == "") {
		F1InstanceSQL = "";
	}
	else {
		F1InstanceSQL = F1Instance + ".";
	}
	if (F2Instance == "") {
		F2InstanceSQL = "";
	}
	else {
		F2InstanceSQL = F1Instance + ".";
	}


	if (useTestBatch) {
		batchNum = 9999;
	}
	else	{
	    SpoBatch.setOracleInstance(F1Instance);
	    batchNum = SpoBatch.createBatch("X_IMP_CONFIRMED_REQUEST_LINE") ;
	}


	System.out.println("start time: " + now()) ;
        if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("-d")) {
                    debug = true ;
                }
            }
        }

	try {
		o1 = new FileOutputStream(flatFileName);
		System.out.println("flatfile: " + flatFileName);
		p = new PrintStream( o1 );
	} catch (java.io.FileNotFoundException e) {
		System.err.println(e.getMessage()) ;
		e.printStackTrace() ;
		System.exit(4) ;
	} catch (java.lang.SecurityException e) {
		System.err.println(e.getMessage()) ;
		e.printStackTrace() ;
		System.exit(4) ;
	} catch (Exception e) {
		System.err.println(e.getMessage()) ;
		e.printStackTrace() ;
		System.exit(4) ;
	}

        WindowAlgo w = new WindowAlgo(/* input buf */ bufSize,
            /* aging buffer */ ageBufSize) ;

        w.setDebug(debug) ;
        try {  
// keeps track of how long it took to read the 2 tables and form the arrays
            long readStart = System.currentTimeMillis();
	    F1 = new TableSnapshot(bufSize, new ConfirmedRequestLineFactory("SELECT * FROM " + F1InstanceSQL + 
	    "v_confirmed_request_line order by confirmed_request, line", spo, prefetchSize)) ;
	    if (F1 == null) {
		    System.out.println("F1: the old master was not set") ;
		    System.out.println("SELECT * FROM " + F1InstanceSQL + 
			    "v_confirmed_request_line order by confirmed_request, line") ;
		    System.exit(4) ;
	    }
            F2 = new TableSnapshot(ageBufSize, new ConfirmedRequestLineFactory("SELECT * FROM " + F2InstanceSQL + 
	    "x_confirmed_request_line_v order by confirmed_request, line" , amd, prefetchSize)) ;
	    if (F2 == null) {
		    System.out.println("F2: the new master was not set") ;
		    System.out.println("SELECT * FROM " + F2InstanceSQL + 
	    "x_confirmed_request_line_v order by confirmed_request, line" ) ;
		    System.exit(4) ;
	    }

	    logger.debug("F1 & F2 set") ;
	    long readTime = System.currentTimeMillis() - readStart;
	    logger.debug("start w.diff") ;
            w.diff(F1, F2) ;
	    logger.debug("end w.diff") ;
	    p.close();
	    long batchStart = System.currentTimeMillis();
	    transTime = transTime + (System.currentTimeMillis() - batchStart);

	    System.out.println("time to write flat file:" + flatFileTime/1000 + "seconds");
	    System.out.println("time to read SQL and put in buffer:" + readTime/1000 + "seconds");
        }
        catch (SQLException e) {
            System.err.println(e.getMessage()) ;
	    e.printStackTrace() ;
            logger.error(e.getMessage()) ;	 
	    p.close();   
            System.exit(4) ;
        }
        catch (ClassNotFoundException e) {
            System.err.println(e.getMessage()) ;
	    e.printStackTrace() ;
            logger.error(e.getMessage()) ;	    
	    p.close();
            System.exit(4) ;
        }
	catch (java.lang.NullPointerException e) {
		if (e != null) {
			e.printStackTrace() ;		
			if (e.getMessage() != null) {
				System.err.println(e.getMessage()) ;		
			}
		}
		System.exit(4) ;
	}

	finally {
            updateCounts() ;

        }
    }

    private static void updateCounts() {

	if (F1 != null) {
	   	System.out.println("v_confirmed_request_line=" + F1.getRecsIn()) ;
        	logger.info("v_confirmed_request_line=" + F1.getRecsIn()) ;
	}
	if (F2 != null) {
	        System.out.println("x_confirmed_request_line_v=" + F2.getRecsIn()) ;
        	logger.info("x_confirmed_request_line_v=" + F2.getRecsIn()) ;
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
        String      confirmed_request ;
        int      line ;

        public boolean equals(Object o) {
            Key k = (ConfirmedRequestLine.Key) o ;
            return (k.confirmed_request.equals(confirmed_request) && k.line == line );
        }
        public int hashCode() {
            return confirmed_request.hashCode() + new Integer(line).hashCode()  ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (theKey.confirmed_request + theKey.line).compareTo(confirmed_request + line) ;
        }
        public String toString() {
          return "confirmed_request=" + confirmed_request + " line=" + line   ;
        }
    }
    Key key ;
    Key keyToCompare;
    class Body {
        String         location ;
        String         part ;
        int            proposed_request ;
        boolean        proposed_request_isnull ;
        java.sql.Date  request_date ;
        java.sql.Date  due_date ;
        int	       quantity_ordered ;
        int	       quantity_received ;
        String	       request_status ;
        String	       supplier_location ;
        String         attribute_1 ;
        String         attribute_2 ;
        String         attribute_3 ;
        String         attribute_4 ;
        String         attribute_5 ;
        String         attribute_6 ;
        String         attribute_7 ;
        String         attribute_8 ;
	

        public String toString() {
            return "location=" + location + " part=" + part
		   + " proposed_request=" + proposed_request 
		   + " request_date=" + request_date 
		   + " due_date=" + due_date 
		   + " quantity_ordered=" + quantity_ordered 
		   + " quantity_received=" + quantity_received 
		   + " request_status=" + request_status 
		   + " supplier_location=" + supplier_location 
		   + " attribute_1=" + attribute_1 
		   + " attribute_2=" + attribute_2 
		   + " attribute_3=" + attribute_3 
		   + " attribute_4=" + attribute_4 
		   + " attribute_5=" + attribute_5 
		   + " attribute_6=" + attribute_6 
		   + " attribute_7=" + attribute_7 
		   + " attribute_8=" + attribute_8 ; 
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
        boolean equal(int i1, boolean null1, int i2, boolean null2) {
            if (!null1)
                if (!null2)
                    return i1 == i2 ;
                else
                    return false ;
            else if (null2)
                return true ; // both null
            else
                return false ; // i1 == null && i2 != null
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
        boolean equal(BigDecimal d1, boolean null1, BigDecimal d2, boolean null2) {
            if (!null1)
                if (!null2)
                    return d1.compareTo(d2) == 0 ;
                else
                    return false ;
            else if (null2)
                return true ; // both null
            else
                return false ; // i1 == null && i2 != null
        }
        boolean equal(float f1, boolean null1, float f2, boolean null2) {
            if (!null1)
                if (!null2)
                    return f1 == f2 ;
                else
                    return false ;
            else if (null2)
                return true ; // both null
            else
                return false ; // i1 == null && i2 != null
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
        int showDiffCnt = 0 ;
        private void showDiff(boolean result, String fieldName, String field1, String field2) {
                if (!result) {
		    showDiffCnt++ ;
		    //if (showDiffCnt % showDiffThreshold == 0) {
                    	logger.debug("key = " + key + " field: " + fieldName + " *" + field1 + "* *" + field2 + "*") ;
		    //}
                }
        }

        public boolean equals(Object o) {
            Body b = (Body) o ;
            boolean result ;
            result = b.location.equals(location) ;
            showDiff(result, "location", b.location + "", location + "") ;
	    if (!result) return result ;
            result = result && b.part.equals(part) ;
            showDiff(result, "part", b.part + "", part + "") ;
	    if (!result) return result ;
            result = result &&  equal(b.proposed_request, b.proposed_request_isnull, proposed_request, proposed_request_isnull) ;
            showDiff(result, "proposed_request", b.proposed_request + "", proposed_request + "") ;
	    if (!result) return result ;
	    result = result && b.request_date.equals(request_date) ;
            showDiff(result, "request_date", b.request_date + "", request_date + "") ;
	    if (!result) return result ;
	    result = result && b.due_date.equals(due_date) ;
            showDiff(result, "due_date", b.due_date + "", due_date + "") ;
	    if (!result) return result ;
	    result = result && b.quantity_ordered == quantity_ordered ;
            showDiff(result, "quantity_ordered", b.quantity_ordered + "", quantity_ordered + "") ;
	    if (!result) return result ;
	    result = result && b.quantity_received == quantity_received ;
            showDiff(result, "quantity_received", b.quantity_received + "", quantity_received + "") ;
	    if (!result) return result ;
	    result = result && b.request_status.equals(request_status) ;
            showDiff(result, "request_status", b.request_status + "", request_status + "") ;
	    if (!result) return result ;
	    result = result && equal(b.supplier_location,supplier_location) ;
            showDiff(result, "supplier_location", b.supplier_location + "", supplier_location + "") ;
	    if (!result) return result ;
	    result = result && equal(b.attribute_1,attribute_1) ;
            showDiff(result, "attribute_1", b.attribute_1 + "", attribute_1 + "") ;
	    if (!result) return result ;
	    result = result && equal(b.attribute_2,attribute_2) ;
            showDiff(result, "attribute_2", b.attribute_2 + "", attribute_2 + "") ;
	    if (!result) return result ;
	    result = result && equal(b.attribute_3,attribute_3) ;
            showDiff(result, "attribute_3", b.attribute_3 + "", attribute_3 + "") ;
	    if (!result) return result ;
	    result = result && equal(b.attribute_4,attribute_4) ;
            showDiff(result, "attribute_4", b.attribute_4 + "", attribute_4 + "") ;
	    if (!result) return result ;
	    result = result && equal(b.attribute_5,attribute_5) ;
            showDiff(result, "attribute_5", b.attribute_5 + "", attribute_5 + "") ;
	    if (!result) return result ;
	    result = result && equal(b.attribute_6,attribute_6) ;
            showDiff(result, "attribute_6", b.attribute_6 + "", attribute_6 + "") ;
	    if (!result) return result ;
	    result = result && equal(b.attribute_7,attribute_7) ;
            showDiff(result, "attribute_7", b.attribute_7 + "", attribute_7 + "") ;
	    if (!result) return result ;
	    result = result && equal(b.attribute_8, attribute_8) ;
            showDiff(result, "attribute_8", b.attribute_8 + "", attribute_8 + "") ;
	    if (!result) return result ;
            return result ;
        }
    }
    Body body ;
    Body bodyToCompare;

    ConfirmedRequestLine(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.confirmed_request = r.getString("CONFIRMED_REQUEST") ;
        key.line = r.getInt("LINE") ;

        body = new Body() ;
        body.location = r.getString("LOCATION") ;
        body.part = r.getString("PART") ;
        body.proposed_request = r.getInt("PROPOSED_REQUEST") ;
        body.proposed_request_isnull = r.wasNull() ;
        body.request_date = r.getDate("REQUEST_DATE") ;
        body.due_date = r.getDate("DUE_DATE") ;
        body.quantity_ordered = r.getInt("QUANTITY_ORDERED") ;
        body.quantity_received = r.getInt("QUANTITY_RECEIVED") ;
        body.request_status = r.getString("REQUEST_STATUS") ;
        body.supplier_location = r.getString("SUPPLIER_LOCATION") ;
        body.attribute_1 = r.getString("ATTRIBUTE_1") ;
        body.attribute_2 = r.getString("ATTRIBUTE_2") ;
        body.attribute_3 = r.getString("ATTRIBUTE_3") ;
        body.attribute_4 = r.getString("ATTRIBUTE_4") ;
        body.attribute_5 = r.getString("ATTRIBUTE_5") ;
        body.attribute_6 = r.getString("ATTRIBUTE_6") ;
        body.attribute_7 = r.getString("ATTRIBUTE_7") ;
        body.attribute_8 = r.getString("ATTRIBUTE_8") ;
        }
        catch (java.sql.SQLException e) {
            updateCounts() ;
            System.err.println(e.getMessage() ) ;
	    e.printStackTrace() ;
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
	    e.printStackTrace() ;
            logger.fatal(e.getMessage()) ;
            System.exit(4) ;
        }
    }

    private void setBigDecimal(CallableStatement cstmt,
            int paramaterIndex, BigDecimal value, boolean isNull) {
        try {
            if (isNull) {
                cstmt.setNull(paramaterIndex, java.sql.Types.DOUBLE) ;
            }
            else {
                cstmt.setBigDecimal(paramaterIndex, value) ;
            }
        } catch (java.sql.SQLException e) {

            updateCounts() ;
            System.err.println(e.getMessage()) ;
	    e.printStackTrace() ;
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
	    e.printStackTrace() ;
            logger.fatal(e.getMessage()) ;
            System.exit(4) ;
        }
    }

    public void     insert() {
	writeFlatFile("INS");

		rowsInserted++ ;
		if (logger.isDebugEnabled() && rowsInserted % frequency == 0) {
			logger.info("rowsInserted = " + rowsInserted +  "newMaster recsIn=" + F2.getRecsIn() ) ;
		}
    }

    public void     update() {
        writeFlatFile("UPD");
	
		rowsUpdated++ ;
		if (logger.isDebugEnabled() && rowsUpdated % frequency == 0) {
			logger.debug("rowsUpdated = " + rowsUpdated +  " recsIn=" + F1.getRecsIn() ) ;
		}
	
    }
    public void     delete() {

        writeFlatFile("DEL");
//	sequenceCounter++;

		
	        rowsDeleted++ ;
		if (logger.isDebugEnabled() && rowsDeleted % frequency == 0) {
			logger.debug("rowsDeleted = " + rowsDeleted +  "oldMaster recsIn=" + F1.getRecsIn() ) ;
		}
	
//	else {
//		r2Del++;
//	}
	
    }

	public static String now() {
		Calendar cal = Calendar.getInstance(TimeZone.getDefault());
		String DATE_FORMAT = "M/dd/yy hh:mm:ss a";
		java.text.SimpleDateFormat sdf =
				new java.text.SimpleDateFormat(DATE_FORMAT);
		sdf.setTimeZone(TimeZone.getDefault());
		return sdf.format(cal.getTime());
	}

	public static String convertDate(Date dateToConvert) {
		if (dateToConvert == null) {
			return "";
		}

		Calendar cal = Calendar.getInstance(TimeZone.getDefault());
		String DATE_FORMAT = "dd-MMM-yyyy";
		java.text.SimpleDateFormat sdf =
				new java.text.SimpleDateFormat(DATE_FORMAT);
		sdf.setTimeZone(TimeZone.getDefault());
		return sdf.format(dateToConvert);
	}



	// writes to flat file to indicate the SQL transaction needed
	public void writeFlatFile(String action)
	{
		long flatStart = System.currentTimeMillis();
		p.println("\"" + key.confirmed_request + "\", " 
		 	+ key.line + ", \"" 
			+ body.location + "\", \""
			+ body.part + "\", "
			+ (body.proposed_request_isnull ? "\"null\"" : body.proposed_request + "") + ", " 
			+ convertDate(body.request_date) + ", "
			+ convertDate(body.due_date) + ", \""
			+ body.quantity_ordered + "\", \""
			+ body.quantity_received + "\", \""
			+ body.request_status + "\", \""
			+ body.supplier_location + "\", \""
			+ body.attribute_1 + "\", \""
			+ body.attribute_2 + "\", \""
			+ body.attribute_3 + "\", \""
			+ body.attribute_4 + "\", \""
			+ body.attribute_5 + "\", \""
			+ body.attribute_6 + "\", \""
			+ body.attribute_7 + "\", \""
			+ body.attribute_8 + "\", \""
			+ action + "\", " 
			+ batchNum);
		flatFileTime = flatFileTime 
			+ (System.currentTimeMillis() - flatStart);
	}

}
