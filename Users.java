import java.sql.* ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

/*   $Author:   zf297a  $
   $Revision:   1.7  $
       $Date:   21 Jul 2008 16:52:20  $
   $Workfile:   Users.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\Users.java-arc  $
/*
/*   Rev 1.7   21 Jul 2008 16:52:20   zf297a
/*Fixed the initialization of paramDataSource: made sure it was set to a value so that if there was no properties file it would still have a value.
/*
/*   Rev 1.6   09 Jul 2008 15:51:10   zf297a
/*Use the new amd_default_users_v view as an additional data source for F2.
/*
/*   Rev 1.5   31 Jan 2008 12:21:18   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.4   08 Jan 2008 22:59:18   zf297a
/*Removed use of command line ini file
/*
/*   Rev 1.3   14 Dec 2007 20:39:36   zf297a
/*Used the new UsersFactory to create the F1 (old master) and F2 (new master) objects.  Fix updateCounts so that it won't reference a null F1 or a null F2. 
/*
/*   Rev 1.2   Oct 31 2005 12:46:26   zf297a
/*Fixed the equal routine by adding  "result &&" to the test of last_name and first_name to made all tests "AND'ed" with each other.
/*
/*   Rev 1.1   Aug 15 2005 12:37:30   zf297a
/*Added table uims to F2
   */
public class Users implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug ;
    static String paramDataSource = "select bems_id, stable_email, last_name, first_name from amd_default_users_v" ;
    static boolean no_op    = false;
    static int bufSize      = 200 ;
    static int ageBufSize   = 200 ;
    static int prefetchSize = 200 ;
    static int debugThreshold = 10 ;
    static int showDiffThreshold = 10 ;

    static Logger logger = Logger.getLogger(Users.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_spare_parts
    private static TableSnapshot F2 = null ; // tmp_amd_spare_parts

    static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(Users.class.getName()).getProperties() ;

       		paramDataSource = p.getProperty("paramDataSource",paramDataSource) ;
       		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","200") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","10") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","10") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","200") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","200") ).intValue() ;


	} catch (java.io.IOException e) {
		System.err.println("Warning: " + e.getMessage()) ;
	} catch (java.lang.Exception e) {
		System.err.println("Warning: " + e.getMessage()) ;
	}
    }

    public static void main(String[] args) {
        final int MAXBUFSIZE = 100 ;
        int agingBufsize ;

	loadParams() ;
	logger.debug("bufSize=" + bufSize + " ageBufSize = " + ageBufSize 
			+ " prefetchSize=" + prefetchSize + " no_op=" + no_op
			+ " debug=" + debug + " debugThreshold=" + debugThreshold 
			+ " showDiffThreshold=" + showDiffThreshold
			+ " paramDataSource=" + paramDataSource) ;

        System.out.println("start time: " + now()) ;
        if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("-d")) {
                    debug = true ;
                }
            }
        }

	if (debug) {
		System.out.println("bufSize=" + bufSize + " ageBufSize = " + ageBufSize 
				+ " prefetchSize=" + prefetchSize + " no_op=" + no_op 
				+ " debugThreshold=" + debugThreshold 
				+ " showDiffThreshold=" + showDiffThreshold
				+ " paramDataSource=" + paramDataSource) ;
	}

        logger.debug("instantiating WindowAlgo") ;
        WindowAlgo w = new WindowAlgo(/* input buf */ MAXBUFSIZE,
            /* aging buffer */ MAXBUFSIZE) ;

	    String theQuery = "Select distinct amd_load.getBemsId(employee_NO) bems_id, stable_email, last_name, first_name from amd_use1, amd_people_all_v where employee_status = 'A'  and length(ims_designator_code) = 3 and amd_load.getBemsId(employee_no) = bems_id union select amd_load.getBemsId(userid) bems_id, stable_email, last_name, first_name from uims, amd_people_all_v where length(designator_code) = 3  and amd_load.getBemsId(userid) = bems_id union " + paramDataSource + " order by bems_id" ;
        w.setDebug(debug) ;
        try {
            F1 = new TableSnapshot(MAXBUFSIZE, new UsersFactory("Select * from amd_users where action_code != 'D' order by bems_id", amd, 100)) ;
            F2 = new TableSnapshot(MAXBUFSIZE, new UsersFactory(theQuery, amd, 100) ) ;
				    
            w.diff(F1, F2) ;
        }
        catch (SQLException e) {
            System.out.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;
	    System.out.println(theQuery) ;
        }
        catch (ClassNotFoundException e) {
            System.out.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;
        }
        finally {
            updateCounts() ;
        }
    }

    private static void updateCounts() {
	if (F1 != null ) {
	        System.out.println("amd_users =" + F1.getRecsIn()) ;
       	 	logger.info("amd_users =" + F1.getRecsIn()) ;
	} else {
	        System.out.println("amd_users: F1 is null") ;
	        logger.info("amd_users: F1 is null") ;
	}

	if (F2 != null ) {
        	System.out.println("amd_use1=" + F2.getRecsIn()) ;
	        logger.info("amd_use1=" + F2.getRecsIn()) ;
	} else {
	        System.out.println("amd_use1: F2 is null") ;
	        logger.info("amd_use1: F2 is null") ;
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
        String      bems_id ;
        public boolean equals(Object o) {
            Key k = (Key) o ;
            return (k.bems_id.equals(bems_id) ) ;
        } 
	public int hashCode() {
            return bems_id.hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return bems_id.compareTo(theKey.bems_id) ;
        }
        public String toString() {
            return "bems_id=" + bems_id  ;
        }
    }
    Key key ;
    class Body {
        String 	    stable_email ;
        String      last_name ;
        String      first_name ;



        public String toString() {
            return "stable_email=" + stable_email + " " +
                    "last_name=" + last_name + " " +
                    "first_name=" + first_name ;
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
            result = equal(b.stable_email, stable_email) ;
            showDiff(result, "stable_email", b.stable_email + "", stable_email + "") ;
            result = result && equal(b.last_name, last_name) ;
            showDiff(result, "last_name", b.last_name + "", last_name + "") ;
            result = result && equal(b.first_name, first_name) ;
            showDiff(result, "first_name", b.first_name + "", first_name + "") ;
            return result ;
        }
    }

    Body body ;

    Users(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.bems_id = r.getString("bems_id") ;

        body = new Body() ;
        body.stable_email = r.getString("stable_email") ;
        body.last_name = r.getString("last_name") ;
        body.first_name = r.getString("first_name") ;
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

    public void     insert() {
	if (!no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_load.insertUsersRow("
			+ "?, ?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.bems_id) ;
		    cstmt.setString(3, body.stable_email) ;
		    cstmt.setString(4, body.last_name) ;
		    cstmt.setString(5, body.first_name) ;
		    logger.info("Insert: key=" + key + " body=" + body) ;
		    if (debug) {
			System.out.println("Insert: key=" + key + " body=" + body) ;
		    }
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_load.insertUsersRow failed with result = " + result) ;
			logger.fatal("amd_load.insertUsersRow failed with result = " + result) ;
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
	}
        rowsInserted++ ;
    }

    public void     update() {
	 if (!no_op) {
		 try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_load.updateUsersRow("
			+ "?, ?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.bems_id) ;
		    cstmt.setString(3, body.stable_email) ;
		    cstmt.setString(4, body.last_name) ;
		    cstmt.setString(5, body.first_name) ;
		    if (debug) {
			System.out.println("Update: key=" + key + " body=" + body) ;
		    }
		    logger.info("Update: key=" + key + " body=" + body) ;
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_load.updateUsersRow failed with result = " + result) ;
			logger.fatal("amd_load.updateUsersRow failed with result = " + result) ;
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
	}
        rowsUpdated++ ;
    }
    public void     delete() {
	if (!no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_load.deleteUsersRow(?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.bems_id) ;
		    if (debug) {
		      System.out.println("Delete: key=" + key) ;
		    }
		    logger.info("Delete: key=" + key + " body=" + body) ;
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_load.deleteUsersRow failed with result = " + result) ;
			logger.fatal("amd_load.deleteUsersRow failed with result = " + result) ;
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
