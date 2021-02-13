import java.sql.* ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

/*   $Author:   zf297a  $
   $Revision:   1.4  $
       $Date:   21 Jul 2008 16:52:20  $
   $Workfile:   Planner.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\Planner.java-arc  $
/*
/*   Rev 1.4   21 Jul 2008 16:52:20   zf297a
/*Fixed the initialization of paramDataSource: made sure it was set to a value so that if there was no properties file it would still have a value.
/*
/*   Rev 1.3   09 Jul 2008 15:53:16   zf297a
/*Use the new amd_default_planners_v view as an additional data source for F2.
/*
/*   Rev 1.2   31 Jan 2008 12:18:16   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.1   Aug 15 2005 12:38:18   zf297a
/*Added table uims
/*
/*   Rev 1.0   Jun 15 2005 14:25:36   c970183
/*Initial revision.
   
  */
public class Planner implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug ;
    static String paramDataSource = "select planner_code from amd_default_planners_v";
    static boolean no_op    = false;
    static int bufSize      = 73 ;
    static int ageBufSize   = 73 ;
    static int prefetchSize = 10 ;
    static int debugThreshold = 10 ;

    static Logger logger = Logger.getLogger(Planner.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_planners 
    private static TableSnapshot F2 = null ; // amd_use1

    static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(Planner.class.getName()).getProperties() ;

       		paramDataSource = p.getProperty("paramDataSource",paramDataSource ) ;
       		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","73") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","10") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","73") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","10") ).intValue() ;

	} catch (java.io.IOException e) {
		System.err.println("Planner: warning: " + e.getMessage()) ;
	} catch (java.lang.Exception e) {
		System.err.println("Planner: warning: " + e.getMessage()) ;
	}
    }

    public static void main(String[] args) {
        final int bufSize = 73 ;
        int agingBufsize ;

	loadParams() ;
	logger.debug("bufSize=" + bufSize + " ageBufSize = " + ageBufSize 
			+ " prefetchSize=" + prefetchSize + " no_op=" + no_op
			+ " debug=" + debug + " debugThreshold=" + debugThreshold 
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
				+ " paramDataSource=" + paramDataSource) ;
	}

        WindowAlgo w = new WindowAlgo(/* input buf */ bufSize,
            /* aging buffer */ ageBufSize) ;

        w.setDebug(debug) ;
        try {
            F1 = new TableSnapshot(bufSize, new PlannersFactory("Select planners.planner_code from amd_planners planners where planners.action_code  != 'D' order by planner_code" )) ;
            F2 = new TableSnapshot(bufSize, new PlannersFactory("Select distinct ims_designator_code planner_code from amd_use1 where employee_status = 'A' and substr(employee_no,1,1) in ('1','2','3','4','5','6','7','8','9','0') and ims_designator_code is not null and length(ims_designator_code) = 3 union select uims.DESIGNATOR_CODE planner_code from uims where length(designator_code) = 3 union " + paramDataSource + " order by planner_code")) ;
            w.diff(F1, F2) ;
        }
        catch (SQLException e) {
            System.out.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;
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
	if (F1 != null) {
		System.out.println("amd_planners in=" + F1.getRecsIn()) ;
		logger.info("amd_planners in=" + F1.getRecsIn()) ;
	}
	if (F2 != null) {
		System.out.println("amd_use1 in=" + F2.getRecsIn()) ;
		logger.info("amd_use1 in=" + F2.getRecsIn()) ;
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
        String      planner_code ;
        public boolean equals(Object o) {
            Key k = (Key) o ;
            return (k.planner_code.equals(planner_code) ) ;
        }
        public int hashCode() {
            return planner_code.hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return planner_code.compareTo(theKey.planner_code) ;
        }
        public String toString() {
            return "planner_code=" + planner_code  ;
        }
    }
    Key key ;

    Planner(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.planner_code = r.getString("PLANNER_CODE") ;

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
        return null ;
    }

    public boolean  keysEqual(Rec r) {
        boolean result = key.equals(r.getKey()) ;
        return (key.equals( r.getKey() )) ;
    }
    public boolean  bodiesEqual(Rec r) {
        return true ;
    }

    public void     insert() {
	if (! no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_load.InsertRow("
			+ "?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.planner_code) ;
		    logger.info("Insert: key=" + key ) ;
		    if (debug) {
			System.out.println("Insert: key=" + key) ;
		    }
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_load.InsertRow failed with result = " + result) ;
			logger.fatal("amd_load.InsertRow failed with result = " + result) ;
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
	 if (! no_op) {
		 try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_load.UpdateRow("
			+ "? )}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.planner_code) ;
		    if (debug) {
			System.out.println("Update: key=" + key) ;
		    }
		    logger.info("Update: key=" + key ) ;
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_load.UpdateRow failed with result = " + result) ;
			logger.fatal("amd_load.UpdateRow failed with result = " + result) ;
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
	if (! no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_load.DeleteRow(?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.planner_code) ;
		    if (debug) {
		      System.out.println("Delete: key=" + key) ;
		    }
		    logger.info("Delete: key=" + key ) ;
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_load.DeleteRow failed with result = " + result) ;
			logger.fatal("amd_load.DeleteRow failed with result = " + result) ;
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
