/*   $Author:   zf297a  $
$Revision:   1.3  $
$Date:   18 Feb 2009 08:18:48  $
$Workfile:   LocationPartOverride.java  $
$Log:   I:Program FilesMerantvmwin32binpdsarchivesSDS-AMDComponents-ClientServerWindowAlgorithmLocationPartOverride.java.-arc  $
/*
/*   Rev 1.3   18 Feb 2009 08:18:48   zf297a
/*Add some debug for the sql being executed by the insert method.
/*
/*   Rev 1.2   31 Jan 2008 12:18:14   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.1   20 Dec 2007 00:16:08   zf297a
/*Get the connection info via DBConnection.  Get parameters via the properties file.
/*
/*   Rev 1.0   Mar 08 2006 00:02:02   zf297a
/*Initial revision.
*/
import java.sql.* ;
import java.io.* ;
import org.apache.log4j.Logger ;
import org.apache.log4j.Level ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

public class LocationPartOverride implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;
    
    static boolean simulate ;
    static boolean debug ;
    static boolean no_op    = false;
    static int bufSize      = 150000 ;
    static int ageBufSize   = 150000 ;
    static int prefetchSize = 5000 ;
    static int debugThreshold = 1000 ;
    static int showDiffThreshold = 1000 ;
    static String sqlFile = "../data/LocationPartOverride.sql" ;
    static FileOutputStream out ;
    static PrintStream sqlPrint ;
    
    
    static Logger logger = Logger.getLogger(LocationPartOverride.class.getName());
    
    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_location_part_override
    private static TableSnapshot F2 = null ; // tmp_amd_location_part_override
    
    static private void loadParams() {
        try {
            java.util.Properties p        = new AppProperties(LocationPartOverride.class.getName()).getProperties() ;
            
            sqlFile = p.getProperty("sqlFile","../data/LocationPartOverride.sql") ;
            simulate = p.getProperty("simulate","false").equals("true") ;
            debug = p.getProperty("debug","false").equals("true") ;
            debug = p.getProperty("debug","false").equals("true") ;
            debug = p.getProperty("debug","false").equals("true") ;
            no_op = p.getProperty("no_op","false").equals("true") ;
            bufSize = Integer.valueOf( p.getProperty("bufSize","150000") ).intValue() ;
            debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","1000") ).intValue() ;
            showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","1000") ).intValue() ;
            ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","150000") ).intValue() ;
            prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","5000") ).intValue() ;
            logger.debug("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" + prefetchSize +
            " no_op =" + no_op + " debug=" + debug + " debugThreshold=" + debugThreshold + " showDiffThreshold=" +
            showDiffThreshold) ;
            
            if (debug) {
                System.out.println("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" +
                prefetchSize + " no_op=" + no_op + " debugThreshold=" + debugThreshold + " showDiffThreshold=" +
                showDiffThreshold + " simulate=" + simulate) ;
                out = new FileOutputStream(sqlFile) ;
                sqlPrint = new PrintStream(out) ;
                sqlPrint.println("variable rc numbernn") ;
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
            logger.debug("Creating F1 and F2") ;
            /* add round */
            String sqlA = "select * from " ;
            String sqlC = " where action_code != 'D' order by part_no, loc_sid " ;
            String sqlOld= sqlA + "amd_location_part_override" + sqlC ;
            String sqlNew= sqlA + "tmp_amd_location_part_override" + sqlC ;
            System.out.println("Creating F1 and F2") ;
            F1 = new TableSnapshot(bufSize, new LocationPartOverrideFactory(sqlOld, amd, prefetchSize) ) ;
            System.out.println("F1 created") ;
            F2 = new TableSnapshot(bufSize, new LocationPartOverrideFactory(sqlNew, amd, prefetchSize) ) ;
            if (debug) {
                System.out.println("sqlOld:" + sqlOld) ;
                System.out.println("sqlNew:" + sqlNew) ;
            }
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
            System.exit(8) ;
        }
        finally {
            updateCounts() ;
            if (sqlPrint != null) {
                sqlPrint.close() ;
            }
            System.exit(0) ;
        }
    }
    
    private static void updateCounts() {
        if (F1 != null) {
            System.out.println("amd_location_part_override_in=" + F1.getRecsIn()) ;
            logger.info("amd_location_part_override_in=" + F1.getRecsIn()) ;
        }
        if (F2 != null) {
            System.out.println("tmp_amd_location_part_override=" + F2.getRecsIn()) ;
            logger.info("tmp_amd_location_part_override=" + F2.getRecsIn()) ;
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
        String part_no ;
        double loc_sid;
        
        public boolean equals(Object o) {
            Key k = (Key) o ;
            return (    (k.part_no.equals(part_no))
            && (k.loc_sid == loc_sid) ) ;
        }
        public int hashCode() {
            return      new String(part_no).hashCode()
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
        double tsl_override_qty ;
        boolean tsl_override_qty_isnull ;
        String tsl_override_user ;
        
        
        public String toString() {
            return " tsl_override_qty=" + tsl_override_qty +
            " tsl_override_user=" + tsl_override_user ;
            
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
        int showDiffCnt = 0 ;
        private void showDiff(boolean result, String fieldName, String field1, String field2) {
            if (!result) {
                showDiffCnt++ ;
                if (showDiffCnt % showDiffThreshold == 0) {
                    logger.debug("key = " + key + " field: " + fieldName + " *" + field1 + "* *" + field2 + "*") ;
                }
            }
        }
        
        private void showDiff(boolean result, String fieldName, String field1, String field2, String field3, String field4) {
            if (!result) {
                showDiffCnt++ ;
                if (showDiffCnt % showDiffThreshold == 0) {
                    logger.debug("key = " + key + " field: " + fieldName + " *" + field1 + "* *" + field2 + "* *" +
                    field3 + " * *" + field4 + "*") ;
                }
            }
        }
        
        public double nvl(boolean isNull, double in, double newOut) {
            if (isNull) {
                return newOut ;
                }else {
                return in ;
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
            boolean resultField = true ;
            
            resultField = equal(b.tsl_override_qty, b.tsl_override_qty_isnull, tsl_override_qty, tsl_override_qty_isnull) ;
            showDiff(resultField, "tsl_override_qty", b.tsl_override_qty + "", tsl_override_qty + "", b.tsl_override_qty_isnull + "", tsl_override_qty_isnull + "" ) ;
            result = resultField && result ;
            resultField = equal(b.tsl_override_user, tsl_override_user) ;
            showDiff(resultField, "tsl_override_user", b.tsl_override_user + "", tsl_override_user + "") ;
            result = resultField && result ;
            
            return result ;
        }
    }
    Body body ;
    
    LocationPartOverride(ResultSet r) throws SQLException {
        try {
            key = new Key() ;
            key.part_no  = r.getString("part_no") ;
            key.loc_sid  = r.getDouble("loc_sid") ;
            
            body = new Body() ;
            
            body.tsl_override_qty=r.getDouble("tsl_override_qty") ;
            body.tsl_override_qty_isnull=r.wasNull() ;
            body.tsl_override_user=r.getString("tsl_override_user") ;
            
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
        if (!no_op) {
            try {
                CallableStatement cstmt = amd.c.prepareCall(
                "{? = call amd_location_part_override_pkg.InsertRow("
                    + "?, ?, ?, ?"
                + ")}") ;
                
                if (debug) {
                    sqlPrint.println("exec :rc := amd_location_part_override_pkg.insertRow('"
                    + key.part_no + "',"
                    + (int) key.loc_sid + ","
                    + (int) body.tsl_override_qty + ",'"
                    + body.tsl_override_user + "');" ) ;
                }
                
                cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
                cstmt.setString(2, key.part_no) ;
                cstmt.setDouble(3, key.loc_sid) ;
                setDouble(cstmt, 4, body.tsl_override_qty, body.tsl_override_qty_isnull) ;
                cstmt.setString(5, body.tsl_override_user) ;
                
                int result;
                if (simulate)
                result = 0;
                else {
                    cstmt.execute() ;
                    result = cstmt.getInt(1) ;
                }
                if (result > 0) {
                    updateCounts() ;
                    System.out.println("amd_location_part_override_pkg.InsertRow failed with result = " + result) ;
                    logger.fatal("amd_location_part_override_pkg.InsertRow failed with result = " + result) ;
                    System.exit(result) ;
                }
                cstmt.close() ;
                rowsInserted++ ;
            }
            catch (java.sql.SQLException e) {
                updateCounts() ;
                System.out.println("insert: " + e.getMessage()) ;
                logger.fatal(e.getMessage()) ;
                System.exit(16) ;
            }
            logger.info("Insert: key=" + key + " body=" + body) ;
        }
    }
    
    public void update() {
        if (!no_op) {
            try {
                CallableStatement cstmt = amd.c.prepareCall(
                "{? = call amd_location_part_override_pkg.UpdateRow("
                    + "?, ?, ?, ?"
                + ")}") ;
                cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
                cstmt.setString(2, key.part_no) ;
                cstmt.setDouble(3, key.loc_sid) ;
                setDouble(cstmt, 4, body.tsl_override_qty, body.tsl_override_qty_isnull) ;
                cstmt.setString(5, body.tsl_override_user) ;
                int result;
                if (simulate)
                result = 0;
                else {
                    cstmt.execute() ;
                    result = cstmt.getInt(1) ;
                }
                if (result > 0) {
                    updateCounts() ;
                    System.out.println("amd_location_part_override_pkg.UpdateRow failed with result = " + result) ;
                    logger.fatal("amd_location_part_override_pkg.UpdateRow failed with result = " + result) ;
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
            logger.info("Update: key=" + key + "body=" + body) ;
        }
        rowsUpdated++ ;
    }
    public void delete() {
        if (!no_op) {
            try {
                CallableStatement cstmt = amd.c.prepareCall(
                "{? = call amd_location_part_override_pkg.DeleteRow("
                    + "?, ?, ?, ?"
                + ")}") ;
                cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
                cstmt.setString(2, key.part_no) ;
                cstmt.setDouble(3, key.loc_sid) ;
                setDouble(cstmt, 4, body.tsl_override_qty, body.tsl_override_qty_isnull) ;
                cstmt.setString(5, body.tsl_override_user) ;
                int result;
                if (simulate)
                result = 0;
                else {
                    cstmt.execute() ;
                    result = cstmt.getInt(1) ;
                }
                if (result > 0) {
                    updateCounts() ;
                    System.out.println("amd_location_part_override_pkg.DeleteRow failed with result = " + result) ;
                    logger.fatal("amd_location_part_override_pkg.DeleteRow failed with result = " + result) ;
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
            logger.info("Delete: key=" + key ) ;
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
