import java.sql.* ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

/*   $Author:   zf297a  $
   $Revision:   1.7  $
       $Date:   21 Jul 2008 16:52:20  $
   $Workfile:   PlannerLogons.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\PlannerLogons.java-arc  $
/*
/*   Rev 1.7   21 Jul 2008 16:52:20   zf297a
/*Fixed the initialization of paramDataSource: made sure it was set to a value so that if there was no properties file it would still have a value.
/*
/*   Rev 1.6   09 Jul 2008 15:52:32   zf297a
/*Use the new amd_default_planner_logons_v as an additional data source for F2.
/*
/*   Rev 1.5   29 May 2007 21:44:20   zf297a
/*Fixed query for F1: added data_source to the order by clause.  Fixed query for F2: added the check of amd_owner.uims.ALT_IMS_DES_CODE_B equal to a T and added order by clause.
/*
/*   Rev 1.4   Jun 04 2006 23:10:16   zf297a
/*Fix the literal being displayed for the input table.
/*
/*   Rev 1.3   Jun 04 2006 12:38:36   zf297a
/*Added data_source as part of the key
/*
/*   Rev 1.2   Aug 15 2005 12:38:18   zf297a
/*Added table uims
/*
/*   Rev 1.0   Jun 15 2005 14:25:36   c970183
/*Initial revision.
   
  */
public class PlannerLogons implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug ;
    static String paramDataSource = "select planner_code, logon_id, data_source from amd_default_planner_logons_v";
    static boolean no_op    = false;
    static int bufSize      = 73 ;
    static int ageBufSize   = 73 ;
    static int prefetchSize = 10 ;
    static int debugThreshold = 10 ;

	static Logger logger = Logger.getLogger(PlannerLogons.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_planner_logons
    private static TableSnapshot F2 = null ; // amd_use1

    static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(PlannerLogons.class.getName()).getProperties() ;

       		paramDataSource = p.getProperty("paramDataSource", paramDataSource) ;
       		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","73") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","73") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","10") ).intValue() ;


	} catch (java.io.IOException e) {
		System.err.println("Planner: warning: " + e.getMessage()) ;
	} catch (java.lang.Exception e) {
		System.err.println("Planner: warning: " + e.getMessage()) ;
	}
    }

    public static void main(String[] args) {
        final int MAXBUFSIZE = 73 ;
        int agingBufsize ;

	loadParams() ;
	logger.debug("bufSize=" + bufSize + " ageBufSize = " + ageBufSize 
			+ " prefetchSize=" + prefetchSize 
			+ " no_op=" + no_op
			+ " debug=" + debug 
			+ " debugThreshold=" + debugThreshold
			+ " paramDataSource=" + paramDataSource) ;

        System.out.println("start time: " + now()) ;
        if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("-d")) {
                    debug = true ;
                } else {
                    AmdConnection amd = AmdConnection.instance() ;
                    amd.setIniFile(args[i]) ;
                }
            }
        }

	if (debug) {
		System.out.println("bufSize=" + bufSize + " ageBufSize = " + ageBufSize 
				+ " prefetchSize=" + prefetchSize 
				+ " no_op=" + no_op 
				+ " debug=" + debug 
				+ " debugThreshold=" + debugThreshold
				+ " paramDataSource=" + paramDataSource) ;
	}

        WindowAlgo w = new WindowAlgo(/* input buf */ MAXBUFSIZE,
            /* aging buffer */ MAXBUFSIZE) ;

        w.setDebug(debug) ;
        try {
            F1 = new TableSnapshot(MAXBUFSIZE, new PlannerLogonsFactory("Select planner_code, logon_id, data_source from amd_planner_logons where action_code != 'D'  order by planner_code, logon_id, data_source" )) ;
            F2 = new TableSnapshot(MAXBUFSIZE, new PlannerLogonsFactory("select distinct ims_designator_code planner_code, amd_load.getBemsId(employee_no) logon_id, '1' data_source from amd_use1 where employee_status = 'A' and length(ims_designator_code) = 3 and amd_load.getBemsId(employee_no) is not null union select designator_code planner_code, amd_load.getBemsId(userid) logon_id, '2' data_source from uims where length(designator_code) = 3 and amd_load.getBemsId(userid) is not null and upper(ALT_IMS_DES_CODE_B) = 'T' union " + paramDataSource + " order by planner_code, logon_id, data_source")) ;
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
        System.out.println("amd_planner_logons in=" + F1.getRecsIn()) ;
        logger.info("amd_planner_logons in=" + F1.getRecsIn()) ;
        System.out.println("use1 / uims in=" + F2.getRecsIn()) ;
        logger.info("tmp_use1 in=" + F2.getRecsIn()) ;
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
        String      logon_id ;
        String      data_source ;
        public boolean equals(Object o) {
            Key k = (Key) o ;
            return (k.planner_code.equals(planner_code) && k.logon_id.equals(logon_id) && k.data_source.equals(data_source) ) ;
        }
        public int hashCode() {
            return planner_code.hashCode() + logon_id.hashCode() + data_source.hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (planner_code + logon_id + data_source).compareTo(theKey.planner_code + theKey.logon_id + theKey.data_source) ;
        }
        public String toString() {
            return "planner_code=" + planner_code + " logon_id=" + logon_id  + " data_source=" + data_source;
        }
    }
    Key key ;

    PlannerLogons(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.planner_code = r.getString("PLANNER_CODE") ;
        key.logon_id = r.getString("LOGON_ID") ;
        key.data_source = r.getString("DATA_SOURCE") ;

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
        try {
            CallableStatement cstmt = amd.c.prepareCall(
                "{? = call amd_load.insertPlannerLogons("
                + "?, ?, ?)}") ;
            cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
            cstmt.setString(2, key.planner_code) ;
            cstmt.setString(3, key.logon_id) ;
            cstmt.setString(4, key.data_source) ;
            logger.info("Insert: key=" + key ) ;
	    if (debug) {
                System.out.println("Insert: key=" + key) ;
            }
            cstmt.execute() ;
            int result = cstmt.getInt(1) ;
            if (result > 0) {
                updateCounts() ;
                System.out.println("amd_load.InsertPlannerLogons failed with result = " + result) ;
                logger.fatal("amd_load.insertPlannerLogons failed with result = " + result) ;
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
        rowsInserted++ ;
    }

    public void     update() {
         try {
            CallableStatement cstmt = amd.c.prepareCall(
                "{? = call amd_load.updatePlannerLogons("
                + "?, ?, ?)}") ;
            cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
            cstmt.setString(2, key.planner_code) ;
            cstmt.setString(3, key.logon_id) ;
            cstmt.setString(4, key.data_source) ;
            if (debug) {
            	System.out.println("Update: key=" + key) ;
            }
            logger.info("Update: key=" + key ) ;
	    cstmt.execute() ;
            int result = cstmt.getInt(1) ;
            if (result > 0) {
                updateCounts() ;
                System.out.println("amd_load.updatePlannerLogons failed with result = " + result) ;
                logger.fatal("amd_load.updatePlannerLogons failed with result = " + result) ;
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
        rowsUpdated++ ;
    }
    public void     delete() {
        try {
            CallableStatement cstmt = amd.c.prepareCall(
                "{? = call amd_load.deletePlannerLogons(?, ?, ?)}") ;
            cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
            cstmt.setString(2, key.planner_code) ;
            cstmt.setString(3, key.logon_id) ;
            cstmt.setString(4, key.data_source) ;
            if (debug) {
              System.out.println("Delete: key=" + key) ;
            }
            logger.info("Delete: key=" + key ) ;
            cstmt.execute() ;
            int result = cstmt.getInt(1) ;
            if (result > 0) {
                updateCounts() ;
                System.out.println("amd_load.deletePlannerLogons failed with result = " + result) ;
                logger.fatal("amd_load.deletePlannerLogons failed with result = " + result) ;
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
