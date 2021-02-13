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
   $Revision:   1.4  $
       $Date:   21 Jan 2009 08:34:24  $
   $Workfile:   SpoUser.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\SpoUser.java.-arc  $
/*
/*   Rev 1.4   21 Jan 2009 08:34:24   zf297a
/*Fixed the counting of delete's.
/*
/*   Rev 1.3   02 Dec 2008 21:31:48   zf297a
/*Removed erroneous return from delete method
/*
/*   Rev 1.2   02 Dec 2008 12:07:14   zf297a
/*Fixed F1 (spo view) and F2 (amd x_ view) comments.  
/*
/*Added enable_delete variable and allow it to be set via the properties file using enable_delete = true.
/*The default for enable_delete is false.
/*
/*Changed the delete() method to work with the new enable_delete switch.
/*
/*   Rev 1.1   19 Sep 2008 12:02:48   zf297a
/*Don't deleted users for now until my UimsAndUse1Update.sql script is approved or some alternate data source for users is approved.
/*
/*   Rev 1.0   05 Sep 2008 21:39:00   zf297a
/*Initial revision.

	*/

public class SpoUser implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static SpoConnection spo = SpoConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;
    static int frequency = 100 ;

    static boolean debug = false ;
    static boolean useTestBatch = false;
    static boolean enable_delete = false;
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

    static Logger logger = Logger.getLogger(SpoUser.class.getName());

    static FileOutputStream o1;
    static PrintStream p; // declare a print stream object  
    static long batchNum;
    static PreparedStatement pstmt;
    static long transTime = 0;        // keeps track of time spent in transactions
    static long flatFileTime = 0;     // keeps track of time spent writing flatfiles
    static String flatFileName;       // name of flat file to output   


    private static TableSnapshot F1 = null ; // v_spo_user (spo)
    private static TableSnapshot F2 = null ; // x_user_v (amd)

    // loads the parameter from this class's properties file if they exist
    static private void loadParams() {
	try {

		java.util.Properties p        = new AppProperties(SpoUser.class.getName()).getProperties() ;

		enable_delete = p.getProperty("enable_delete", "false").equals("true");
		useTestBatch = p.getProperty("useTestBatch", "false").equals("true");
		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","200") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","10") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","10") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","200") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","200") ).intValue() ;
		flatFileName = String.valueOf( p.getProperty("dumpFile", "SpoUser.csv"));
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
	System.out.println("start time: " + now()) ;
        if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("-d")) {
                    debug = true ;
                }
            }
        }

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
	    batchNum = SpoBatch.createBatch("X_IMP_SPO_USER") ;			
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
	    F1 = new TableSnapshot(bufSize, new SpoUserFactory("SELECT * FROM " + 
	    F1InstanceSQL + "v_spo_user where name not in ('ADMIN','SPO') order by name", spo, prefetchSize)) ;
	    if (F1 == null) {
		    System.out.println("F1: the old master was not set") ;
		    System.out.println("SELECT * FROM " + F1InstanceSQL + 
			    "V_spo_user order by name") ;
		    System.exit(4) ;
	    }
            F2 = new TableSnapshot(ageBufSize, new SpoUserFactory("SELECT * FROM " + F2InstanceSQL + 
	    "x_user_v order by name" , amd, prefetchSize)) ;
	    if (F2 == null) {
		    System.out.println("F2: the new master was not set") ;
		    System.out.println("SELECT * FROM " + F2InstanceSQL + 
	    "x_user_v order by name" ) ;
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
	   	System.out.println("v_spo_user=" + F1.getRecsIn()) ;
        	logger.info("v_spo_user=" + F1.getRecsIn()) ;
	}
	if (F2 != null) {
	        System.out.println("x_user_v=" + F2.getRecsIn()) ;
        	logger.info("x_user_v=" + F2.getRecsIn()) ;
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
        String      name ;

        public boolean equals(Object o) {
            Key k = (SpoUser.Key) o ;
            return (k.name.equals(name) );
        }
        public int hashCode() {
            return name.hashCode()  ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return theKey.name.compareTo(name) ;
        }
        public String toString() {
          return "name=" + name   ;
        }
    }
    Key key ;
    Key keyToCompare;
    class Body {
        String     email_address ;
        boolean    email_address_is_null ;
        String     attribute_1 ;
        boolean    attribute_1_is_null ;
        String     attribute_2 ;
        boolean    attribute_2_is_null ;
        String     attribute_3 ;
        boolean    attribute_3_is_null ;
        String     attribute_4 ;
        boolean    attribute_4_is_null ;
        String     attribute_5 ;
        boolean    attribute_5_is_null ;
        String     attribute_6 ;
        boolean    attribute_6_is_null ;
        String     attribute_7 ;
        boolean    attribute_7_is_null ;
        String     attribute_8 ;
        boolean    attribute_8_is_null ;

        public String toString() {
            return "email_address=" + email_address
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
	    // logger.debug("String: s1=" + s1 + " s2=" + s2) ;
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
        int showDiffCnt = 0 ;
        private void showDiff(boolean result, String fieldName, String field1, String field2) {
                if (!(field1 + "").equals((field2 + ""))) {
		    showDiffCnt++ ;
		    if (showDiffCnt % showDiffThreshold == 0) {
                    	logger.debug("key = " + key + " field: " + fieldName + " *" + field1 + "* *" + field2 + "*") ;
		    }
                }
        }

        public boolean equals(Object o) {
 		Body b = (Body) o ;
		boolean result = true ;
		result = equal(b.email_address, email_address) ;
		showDiff(result, "email_address", b.email_address, email_address) ;
		if (!result) {logger.debug("result:" + result) ; return result ; }
		result = result && equal(b.attribute_1, attribute_1) ;
		showDiff(result, "attribute_1", b.attribute_1, attribute_1) ;
		if (!result) {logger.debug("result:" + result) ; return result ; }
		result = result && equal(b.attribute_2, attribute_2) ;
		showDiff(result, "attribute_2", b.attribute_2, attribute_2) ;
		if (!result) {logger.debug("result:" + result) ; return result ; }
		result = result && equal(b.attribute_3, attribute_3) ;
		showDiff(result, "attribute_3", b.attribute_3, attribute_3) ;
		if (!result) {logger.debug("result:" + result) ; return result ; }
		result = result && equal(b.attribute_4, attribute_4) ;
		showDiff(result, "attribute_4", b.attribute_4, attribute_4) ;
		if (!result) {logger.debug("result:" + result) ; return result ; }
		result = result && equal(b.attribute_5, attribute_5) ;
		showDiff(result, "attribute_5", b.attribute_5, attribute_5) ;
		if (!result) {logger.debug("result:" + result) ; return result ; }
		result = result && equal(b.attribute_6, attribute_6) ;
		showDiff(result, "attribute_6", b.attribute_6, attribute_6) ;
		if (!result) {logger.debug("result:" + result) ; return result ; }
		result = result && equal(b.attribute_7, attribute_7) ;
		showDiff(result, "attribute_7", b.attribute_7, attribute_7) ;
		if (!result) {logger.debug("result:" + result) ; return result ; }
		result = result && equal(b.attribute_8, attribute_8) ;
		showDiff(result, "attribute_8", b.attribute_8, attribute_8) ;
		if (!result) {logger.debug("result:" + result) ; return result ; }
		return result ;
        }
    }
    Body body ;
    Body bodyToCompare;

	 public static String replace(String source, String pattern, String replace) {
		if (source!=null) {
			final int len = pattern.length();
			StringBuffer sb = new StringBuffer();
			int found = -1;
			int start = 0;

			while( (found = source.indexOf(pattern, start) ) != -1) {
				sb.append(source.substring(start, found));
				sb.append(replace);
				start = found + len;
			}

			sb.append(source.substring(start));

			return sb.toString();
	        } else 
			return "";
	}


    SpoUser(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.name = r.getString("NAME") ;

        body = new Body() ;
        body.email_address = replace(r.getString("EMAIL_ADDRESS")," ","") ;
        body.email_address_is_null = r.wasNull() ;
        body.attribute_1 = r.getString("attribute_1") ;
        body.attribute_1_is_null = r.wasNull() ;
        body.attribute_2 = r.getString("attribute_2") ;
        body.attribute_2_is_null = r.wasNull() ;
        body.attribute_3 = r.getString("attribute_3") ;
        body.attribute_3_is_null = r.wasNull() ;
        body.attribute_4 = r.getString("attribute_4") ;
        body.attribute_4_is_null = r.wasNull() ;
        body.attribute_5 = r.getString("attribute_5") ;
        body.attribute_5_is_null = r.wasNull() ;
        body.attribute_6 = r.getString("attribute_6") ;
        body.attribute_6_is_null = r.wasNull() ;
        body.attribute_7 = r.getString("attribute_7") ;
        body.attribute_7_is_null = r.wasNull() ;
        body.attribute_8 = r.getString("attribute_8") ;
        body.attribute_8_is_null = r.wasNull() ;
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
		if (enable_delete) {
			try {
			    CallableStatement cstmt = spo.c.prepareCall(
				"{? = call c17pgm.deleteUser(?)}") ;
			    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
			    cstmt.setString(2, key.name) ;
			    cstmt.execute() ;
			    if (debug) {
			      System.out.println("Delete: key=" + key) ;
			    }
			    logger.info("Delete: key=" + key ) ;
			    int result = cstmt.getInt(1) ;
			    if (result != 0) {
				updateCounts() ;
				System.out.println("c17pgm.deleteUser failed with result = " + result) ;
				logger.fatal("c17pgm.deleteUser failed with result = " + result) ;
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

			
			rowsDeleted++ ;
			if (logger.isDebugEnabled() && rowsDeleted % frequency == 0) {
				logger.debug("rowsDeleted = " + rowsDeleted +  "oldMaster recsIn=" + F1.getRecsIn() ) ;
			}
		}
	
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
		String DATE_FORMAT = "dd-MMM-yy";
		java.text.SimpleDateFormat sdf =
				new java.text.SimpleDateFormat(DATE_FORMAT);
		sdf.setTimeZone(TimeZone.getDefault());
		return sdf.format(dateToConvert);
	}



	// writes to flat file to indicate the SQL transaction needed
	public void writeFlatFile(String action)
	{
		long flatStart = System.currentTimeMillis();
			p.println("\"" + key.name + "\", \"" + 
					 body.email_address  + "\", \"" + 
					 body.attribute_1  + "\", \"" + 
					 body.attribute_2  + "\", \"" + 
					 body.attribute_3  + "\", \"" + 
					 body.attribute_4  + "\", \"" + 
					 body.attribute_5  + "\", \"" + 
					 body.attribute_6  + "\", \"" + 
					 body.attribute_7  + "\", \"" + 
					 body.attribute_8  + "\", \"" + 
		       	 		 action       + "\", " + 
					 batchNum);
			flatFileTime = flatFileTime + (System.currentTimeMillis() - flatStart);
	}

}
