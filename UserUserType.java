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
   $Revision:   1.1  $
       $Date:   05 Feb 2009 09:11:04  $
   $Workfile:   UserUserType.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\UserUserType.java.-arc  $
/*
/*   Rev 1.1   05 Feb 2009 09:11:04   zf297a
/*Fixed table name literals
/*
/*   Rev 1.0   27 Jan 2009 21:05:50   zf297a
/*Initial revision.

	*/

public class UserUserType implements Rec {
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

    static Logger logger = Logger.getLogger(UserUserType.class.getName());

    static FileOutputStream o1;
    static PrintStream p; // declare a print stream object  
    static long batchNum;
    static PreparedStatement pstmt;
    static long transTime = 0;        // keeps track of time spent in transactions
    static long flatFileTime = 0;     // keeps track of time spent writing flatfiles
    static String flatFileName;       // name of flat file to output   


    private static TableSnapshot F1 = null ; // v_user_user_type (spo)
    private static TableSnapshot F2 = null ; // x_user_user_type_v (amd)

    // loads the parameter from this class's properties file if they exist
    static private void loadParams() {
	try {

		java.util.Properties p        = new AppProperties(UserUserType.class.getName()).getProperties() ;

		enable_delete = p.getProperty("enable_delete", "false").equals("true");
		useTestBatch = p.getProperty("useTestBatch", "false").equals("true");
		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","200") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","10") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","10") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","200") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","200") ).intValue() ;
		flatFileName = String.valueOf( p.getProperty("dumpFile", "UserUserType.csv"));
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
	    batchNum = SpoBatch.createBatch("X_IMP_USER_USER_TYPE") ;			
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
	    F1 = new TableSnapshot(bufSize, new UserUserTypeFactory("SELECT * FROM " + 
	    F1InstanceSQL + "v_user_user_type order by spo_user, user_type", spo, prefetchSize)) ;
	    if (F1 == null) {
		    System.out.println("F1: the old master was not set") ;
		    System.out.println("SELECT * FROM " + F1InstanceSQL + 
			    "V_user_user_type order by spo_user, user_type") ;
		    System.exit(4) ;
	    }
            F2 = new TableSnapshot(ageBufSize, new UserUserTypeFactory("SELECT * FROM " + F2InstanceSQL + 
	    "x_user_user_type_v order by spo_user, user_type" , amd, prefetchSize)) ;
	    if (F2 == null) {
		    System.out.println("F2: the new master was not set") ;
		    System.out.println("SELECT * FROM " + F2InstanceSQL + 
	    "x_user_user_type_v order by spo_user, user_type" ) ;
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
	   	System.out.println("v_user_user_type=" + F1.getRecsIn()) ;
        	logger.info("v_user_user_type=" + F1.getRecsIn()) ;
	}
	if (F2 != null) {
	        System.out.println("x_user_user_type_v=" + F2.getRecsIn()) ;
        	logger.info("x_user_user_type_v=" + F2.getRecsIn()) ;
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
        String      spo_user ;
        String      user_type ;

        public boolean equals(Object o) {
            Key k = (UserUserType.Key) o ;
            return (k.spo_user.equals(spo_user) && k.user_type.equals(user_type) );
        }
        public int hashCode() {
            return spo_user.hashCode() + user_type.hashCode()  ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (theKey.spo_user + theKey.user_type).compareTo(spo_user + user_type) ;
        }
        public String toString() {
          return "spo_user=" + spo_user + " user_type=" + user_type   ;
        }
    }
    Key key ;
    Key keyToCompare;
    class Body {

        public String toString() {
            return "" ;
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


    UserUserType(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.spo_user = r.getString("SPO_USER") ;
        key.user_type = r.getString("USER_TYPE") ;

        body = new Body() ;
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
	rowsDeleted++ ;
	return ;
	/*
		if (enable_delete) {
			try {
			    CallableStatement cstmt = spo.c.prepareCall(
				"{? = call c17pgm.deleteUser(?)}") ;
			    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
			    cstmt.setString(2, key.spo_user) ;
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
		*/
	
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
			p.println("\"" + key.spo_user + "\", \"" + 
					 key.user_type  + "\", \"" + 
		       	 		 action       + "\", " + 
					 batchNum);
			flatFileTime = flatFileTime + (System.currentTimeMillis() - flatStart);
	}

}
