/*   $Author:   zf297a  $
   $Revision:   1.2  $
       $Date:   31 Jan 2008 12:18:16  $
   $Workfile:   PartLocs.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\PartLocs.java-arc  $
/*
/*   Rev 1.2   31 Jan 2008 12:18:16   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.1   Oct 31 2005 12:13:46   zf297a
/*Fixed the equal method by using Double's compareTo method for the loc_sid field and the nsi_sid field, which are defined as doubles, instead of using the == operator.
/*
/*   Rev 1.0   30 Aug 2004 08:10:54   c970183
/*Initial revision.
*/
import java.sql.* ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

public class PartLocs implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug ;
    static boolean no_op    = false;
    static int bufSize      = 200 ;
    static int ageBufSize   = 200 ;
    static int prefetchSize = 200 ;
    static int debugThreshold = 10 ;
    static int showDiffThreshold = 10 ;
 

    static Logger logger = Logger.getLogger(PartLocs.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_part_locs
    private static TableSnapshot F2 = null ; // tmp_amd_part_locs

    static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(PartLocs.class.getName()).getProperties() ;

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
    
	logger.debug("in main version 1.0");

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
            F1 = new TableSnapshot(bufSize, new PartLocsFactory("Select * from amd_part_locs where action_code != 'D' order by nsi_sid, loc_sid")) ;
            F2 = new TableSnapshot(bufSize, new PartLocsFactory("Select * from tmp_amd_part_locs order by nsi_sid, loc_sid")) ;

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
            System.exit(0) ;
        }
    }

    private static void updateCounts() {

        System.out.println("amd_on_hand_invs_in=" + F1.getRecsIn()) ;
        logger.info("amd_on_hand_invs_in=" + F1.getRecsIn()) ;
        System.out.println("tmp_amd_on_hand_invs=" + F2.getRecsIn()) ;
        logger.info("tmp_amd_on_hand_invs=" + F2.getRecsIn()) ;
        System.out.println("rows inserted=" + rowsInserted) ;
        logger.info("rows inserted=" + rowsInserted) ;
        System.out.println("rows updated=" + rowsUpdated) ;
        logger.info("rows updated=" + rowsUpdated) ;
        System.out.println("rows deleted=" + rowsDeleted) ;
        logger.info("rows deleted=" + rowsDeleted) ;
        System.out.println("end time: " + now()) ;
    }

    class Key implements Comparable {
        double nsi_sid ;
        double loc_sid;

        public boolean equals(Object o) {
            Key k = (Key) o ;
            return (    (new Double(k.nsi_sid).compareTo(new Double(nsi_sid)) == 0)
                     && (new Double(k.loc_sid).compareTo(new Double(loc_sid)) == 0) ) ;
        }
        public int hashCode() {
            return  	new Double(nsi_sid).hashCode()
                   + new Double(loc_sid).hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return new Double(theKey.nsi_sid + theKey.loc_sid).compareTo(
				    new Double(nsi_sid + loc_sid)) ;
        }
        public String toString() {
            return "nsi_sid_loc_sid =" + nsi_sid + loc_sid ;
        }
    }
    Key key ;
    class Body {
		double awt ;
		boolean awt_isnull ;
		double awt_defaulted ;
		boolean awt_defaulted_isnull ;
		double cost_to_repair ;
		boolean cost_to_repair_isnull ;
		double cost_to_repair_defaulted ;
		boolean cost_to_repair_defaulted_isnull ;
		double mic ;
		boolean mic_isnull ;
		double mic_defaulted ;
		boolean mic_defaulted_isnull ;
		String removal_ind ;
		String removal_ind_defaulted ;
		String removal_ind_cleaned ;
		String repair_level_code ;
		String repair_level_code_cleaned ;
		String repair_level_code_defaulted ;
		double time_to_repair ;
		boolean time_to_repair_isnull ;
		double time_to_repair_defaulted ;
		boolean time_to_repair_defaulted_isnull ;
		String tactical ;
		double rsp_on_hand ;
		boolean rsp_on_hand_isnull ;
		double rsp_objective ;
		boolean rsp_objective_isnull ;
		double order_cost ;
		boolean order_cost_isnull ;
		double holding_cost ;
		boolean holding_cost_isnull ;
		double backorder_fixed_cost ;
		boolean backorder_fixed_cost_isnull ;
		double backorder_variable_cost ;
		boolean backorder_variable_cost_isnull ;

        public String toString() {
            return "awt=" + awt +
            	" awt_defaulted=" + awt_defaulted + " cost_to_repair=" + cost_to_repair +
            	" cost_to_repair_defaulted=" + cost_to_repair_defaulted + " mic=" + mic +
            	" mic_defaulted=" + mic_defaulted + " removal_ind=" + removal_ind +
            	" removal_ind_cleaned=" + removal_ind_cleaned + " repair_leve_code=" + repair_level_code +
				" repair_leve_code_cleaned=" + repair_level_code_cleaned + " repair_level_code_defaulted=" + repair_level_code_defaulted +
				" time_to_repair=" + time_to_repair + " time_to_repair_defaulted=" + time_to_repair_defaulted +
				" tactical=" + tactical + " rsp_on_hand=" + rsp_on_hand + " rsp_objective=" + rsp_objective +
				" order_cost=" + order_cost + " holding_cost=" + holding_cost +
				" backorder_fixed_cost=" + backorder_fixed_cost + " backorder_variable_cost=" + backorder_variable_cost;
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
            result = equal(b.awt, b.awt_isnull, awt, awt_isnull) ;
            showDiff(result, "awt", b.awt + "", awt + "") ;
	    	result = result && equal(b.awt_defaulted, b.awt_defaulted_isnull, awt_defaulted, awt_defaulted_isnull) ;
            showDiff(result, "awt_defaulted", b.awt_defaulted + "", awt_defaulted + "") ;
	    	result = result && equal(b.cost_to_repair, b.cost_to_repair_isnull, cost_to_repair, cost_to_repair_isnull) ;
            showDiff(result, "cost_to_repair", b.cost_to_repair + "", cost_to_repair + "") ;
	    	result = result && equal(b.cost_to_repair_defaulted, b.cost_to_repair_defaulted_isnull, cost_to_repair_defaulted, cost_to_repair_defaulted_isnull) ;
            showDiff(result, "cost_to_repair_defaulted", b.cost_to_repair_defaulted + "", cost_to_repair_defaulted + "") ;
	    	result = result && equal(b.mic, b.mic_isnull, mic, mic_isnull) ;
            showDiff(result, "mic", b.mic + "", mic + "") ;
	    	result = result && equal(b.mic_defaulted, b.mic_defaulted_isnull, mic_defaulted, mic_defaulted_isnull) ;
            showDiff(result, "mic_defaulted", b.mic_defaulted + "", mic_defaulted + "") ;
	    	result = result && equal(b.removal_ind,  removal_ind) ;
            showDiff(result, "removal_ind)", b.removal_ind + "", removal_ind + "") ;
	    	result = result && equal(b.removal_ind_defaulted,  removal_ind_defaulted) ;
            showDiff(result, "removal_ind_defaulted)", b.removal_ind_defaulted + "", removal_ind_defaulted + "") ;
	    	result = result && equal(b.removal_ind_cleaned,  removal_ind_cleaned) ;
            showDiff(result, "removal_ind_cleaned)", b.removal_ind_cleaned  + "", removal_ind_cleaned + "") ;
	    	result = result && equal(b.repair_level_code,  repair_level_code) ;
            showDiff(result, "repair_level_code)", b.repair_level_code + "", repair_level_code + "") ;
	    	result = result && equal(b.repair_level_code_defaulted,  repair_level_code_defaulted) ;
            showDiff(result, "repair_level_code_defaulted)", b.repair_level_code_defaulted + "", repair_level_code_defaulted + "") ;
	    	result = result && equal(b.repair_level_code_cleaned,  repair_level_code_cleaned) ;
            showDiff(result, "repair_level_code_cleaned)", b.repair_level_code_cleaned + "", repair_level_code_cleaned + "") ;
	   	 	result = result && equal(b.time_to_repair, b.time_to_repair_isnull, time_to_repair, time_to_repair_isnull) ;
            showDiff(result, "time_to_repair", b.time_to_repair + "", time_to_repair + "") ;
	    	result = result && equal(b.time_to_repair_defaulted, b.time_to_repair_defaulted_isnull, time_to_repair_defaulted, time_to_repair_defaulted_isnull) ;
            showDiff(result, "time_to_repair_defaulted", b.time_to_repair_defaulted + "", time_to_repair_defaulted + "") ;
	   		result = result && equal(b.tactical,  tactical) ;
            showDiff(result, "tactical)", b.tactical + "", tactical + "") ;
	    	result = result && equal(b.rsp_on_hand, b.rsp_on_hand_isnull, rsp_on_hand, rsp_on_hand_isnull) ;
            showDiff(result, "rsp_on_hand", b.rsp_on_hand + "", rsp_on_hand + "") ;
	    	result = result && equal(b.rsp_objective, b.rsp_objective_isnull, rsp_objective, rsp_objective_isnull) ;
            showDiff(result, "rsp_objective", b.rsp_objective + "", rsp_objective + "") ;
	    	result = result && equal(b.order_cost, b.order_cost_isnull, order_cost, order_cost_isnull) ;
            showDiff(result, "order_cost", b.order_cost + "", order_cost + "") ;
	    	result = result && equal(b.holding_cost, b.holding_cost_isnull, holding_cost, holding_cost_isnull) ;
            showDiff(result, "holding_cost", b.holding_cost + "", holding_cost + "") ;
	    	result = result && equal(b.backorder_fixed_cost, b.backorder_fixed_cost_isnull, backorder_fixed_cost, backorder_fixed_cost_isnull) ;
            showDiff(result, "backorder_fixed_cost", b.backorder_fixed_cost + "", backorder_fixed_cost + "") ;
	    	result = result && equal(b.backorder_variable_cost, b.backorder_variable_cost_isnull, backorder_variable_cost, backorder_variable_cost_isnull) ;
            showDiff(result, "backorder_variable_cost", b.backorder_variable_cost + "", backorder_variable_cost + "") ;

            return result ;
        }
    }
    Body body ;

    PartLocs(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.nsi_sid  = r.getDouble("nsi_sid") ;
        key.loc_sid  = r.getDouble("loc_sid") ;

        body = new Body() ;
        body.awt = r.getDouble("awt") ;
		body.awt_isnull = r.wasNull() ;
        body.awt_defaulted = r.getDouble("awt_defaulted") ;
        body.awt_defaulted_isnull = r.wasNull() ;
        body.cost_to_repair = r.getDouble("cost_to_repair") ;
        body.cost_to_repair_isnull = r.wasNull() ;
        body.cost_to_repair_defaulted = r.getDouble("cost_to_repair_defaulted") ;
        body.cost_to_repair_defaulted_isnull = r.wasNull() ;
        body.mic = r.getDouble("mic") ;
        body.mic_isnull = r.wasNull() ;
        body.mic_defaulted = r.getDouble("mic_defaulted") ;
        body.mic_defaulted_isnull = r.wasNull() ;
        body.removal_ind = r.getString("removal_ind") ;
        body.removal_ind_defaulted = r.getString("removal_ind_defaulted") ;
        body.removal_ind_cleaned = r.getString("removal_ind_cleaned") ;
        body.repair_level_code = r.getString("repair_level_code") ;
        body.repair_level_code_defaulted = r.getString("repair_level_code_defaulted") ;
        body.repair_level_code_cleaned = r.getString("repair_level_code_cleaned") ;
        body.time_to_repair = r.getDouble("time_to_repair") ;
        body.time_to_repair_isnull = r.wasNull() ;
        body.time_to_repair_defaulted = r.getDouble("time_to_repair_defaulted") ;
        body.time_to_repair_defaulted_isnull = r.wasNull() ;
        body.tactical = r.getString("tactical") ;
        body.rsp_on_hand = r.getDouble("rsp_on_hand") ;
        body.rsp_on_hand_isnull = r.wasNull() ;
        body.rsp_objective = r.getDouble("rsp_objective") ;
        body.rsp_objective_isnull = r.wasNull() ;
        body.order_cost = r.getDouble("order_cost") ;
        body.order_cost_isnull = r.wasNull() ;
        body.holding_cost = r.getDouble("holding_cost") ;
        body.holding_cost_isnull = r.wasNull() ;
        body.backorder_fixed_cost = r.getDouble("backorder_fixed_cost") ;
        body.backorder_fixed_cost_isnull = r.wasNull() ;
        body.backorder_variable_cost = r.getDouble("backorder_variable_cost") ;
        body.backorder_variable_cost_isnull = r.wasNull() ;
        }
        catch (java.sql.SQLException e) {
            updateCounts() ;
            System.out.println(e.getMessage() ) ;
            logger.fatal(e.getMessage()) ;
            System.exit(10) ;
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
            System.exit(12) ;
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
            System.exit(14) ;
        }
    }

    public void     insert() {
	    if (!no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_part_locs_load_pkg.InsertRow("
			+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setDouble(2, key.nsi_sid) ;
		    cstmt.setDouble(3, key.loc_sid) ;
		    setDouble(cstmt, 4, body.awt, body.awt_isnull) ;
		    setDouble(cstmt, 5, body.awt_defaulted, body.awt_defaulted_isnull) ;
		    setDouble(cstmt, 6, body.cost_to_repair, body.cost_to_repair_isnull) ;
		    setDouble(cstmt, 7, body.cost_to_repair_defaulted, body.cost_to_repair_isnull) ;
		    setDouble(cstmt, 8, body.mic, body.mic_isnull) ;
		    setDouble(cstmt, 9, body.mic_defaulted, body.mic_defaulted_isnull) ;
		    cstmt.setString(10, body.removal_ind) ;
		    cstmt.setString(11, body.removal_ind_defaulted) ;
		    cstmt.setString(12, body.removal_ind_cleaned) ;
		    cstmt.setString(13, body.repair_level_code) ;
		    cstmt.setString(14, body.repair_level_code_defaulted) ;
		    cstmt.setString(15, body.repair_level_code_cleaned) ;
		    setDouble(cstmt, 16, body.time_to_repair, body.time_to_repair_isnull) ;
		    setDouble(cstmt, 17, body.time_to_repair_defaulted, body.time_to_repair_defaulted_isnull) ;
		    cstmt.setString(18, body.tactical) ;
		    setDouble(cstmt, 19, body.rsp_on_hand, body.rsp_on_hand_isnull) ;
		    setDouble(cstmt, 20, body.rsp_objective, body.rsp_objective_isnull) ;
		    setDouble(cstmt, 21, body.order_cost, body.order_cost_isnull) ;
		    setDouble(cstmt, 22, body.holding_cost, body.holding_cost_isnull) ;
		    setDouble(cstmt, 23, body.backorder_fixed_cost, body.backorder_fixed_cost_isnull) ;
		    setDouble(cstmt, 24, body.backorder_variable_cost, body.backorder_variable_cost_isnull) ;
		    cstmt.execute() ;

		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_part_locs_pkg.InsertRow failed with result = " + result) ;
			logger.fatal("amd_part_locs_pkg.InsertRow failed with result = " + result) ;
			System.exit(result) ;
		    }
		    cstmt.close() ;
		}
		catch (java.sql.SQLException e) {
		    updateCounts() ;
		    System.out.println(e.getMessage()) ;
		    logger.fatal(e.getMessage()) ;
		    System.exit(16) ;
		}
		if (debug) {
		    System.out.println("Insert: key=" + key + " body=" + body) ;
		}
		logger.info("Insert: key=" + key + " awt=" + body.awt) ;
    	}
        rowsInserted++ ;
    }

    public void     update() {
	if (!no_op) {
	    try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_part_locs_load_pkg.UpdateRow("
			+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setDouble(2, key.nsi_sid) ;
		    cstmt.setDouble(3, key.loc_sid) ;
		    setDouble(cstmt, 4, body.awt, body.awt_isnull) ;
		    setDouble(cstmt, 5, body.awt_defaulted, body.awt_defaulted_isnull) ;
		    setDouble(cstmt, 6, body.cost_to_repair, body.cost_to_repair_isnull) ;
		    setDouble(cstmt, 7, body.cost_to_repair_defaulted, body.cost_to_repair_defaulted_isnull) ;
		    setDouble(cstmt, 8, body.mic, body.mic_isnull) ;
		    setDouble(cstmt, 9, body.mic_defaulted, body.mic_defaulted_isnull) ;
		    cstmt.setString(10, body.removal_ind) ;
		    cstmt.setString(11, body.removal_ind_defaulted) ;
		    cstmt.setString(12, body.removal_ind_cleaned) ;
		    cstmt.setString(13, body.repair_level_code) ;
		    cstmt.setString(14, body.repair_level_code_defaulted) ;
		    cstmt.setString(15, body.repair_level_code_cleaned) ;
		    setDouble(cstmt, 16, body.time_to_repair, body.time_to_repair_isnull) ;
		    setDouble(cstmt, 17, body.time_to_repair_defaulted, body.time_to_repair_defaulted_isnull) ;
		    cstmt.setString(18, body.tactical) ;
		    setDouble(cstmt, 19, body.rsp_on_hand, body.rsp_on_hand_isnull) ;
		    setDouble(cstmt, 20, body.rsp_objective, body.rsp_objective_isnull) ;
		    setDouble(cstmt, 21, body.order_cost, body.order_cost_isnull) ;
		    setDouble(cstmt, 22, body.holding_cost, body.holding_cost_isnull) ;
		    setDouble(cstmt, 23, body.backorder_fixed_cost, body.backorder_fixed_cost_isnull) ;
		    setDouble(cstmt, 24, body.backorder_variable_cost, body.backorder_variable_cost_isnull) ;
		    cstmt.execute() ;

		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_part_locs_pkg.UpdateRow failed with result = " + result) ;
			logger.fatal("amd_part_locs_pkg.UpdateRow failed with result = " + result) ;
			System.exit(result) ;
		    }
		    cstmt.close() ;
		}
		catch (java.sql.SQLException e) {
		    updateCounts() ;
		    System.out.println(e.getMessage()) ;
		    logger.fatal(e.getMessage()) ;
		    System.exit(18) ;
		}
		if (debug) {
		    System.out.println("Update: key=" + key + " body=" + body) ;
		}
		logger.info("Update: key=" + key + " awt=" + body.awt) ;
	}
        rowsUpdated++ ;
    }
    public void     delete() {
	if (!no_op) {
	    try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_part_locs_load_pkg.DeleteRow(?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setDouble(2, key.nsi_sid) ;
		    cstmt.setDouble(3, key.loc_sid) ;
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_part_locs_pkg.DeleteRow failed with result = " + result) ;
			logger.fatal("amd_part_locs_pkg.DeleteRow failed with result = " + result) ;
			System.exit(result) ;
		    }
		    cstmt.close() ;
		}
		catch (java.sql.SQLException e) {
		    updateCounts() ;
		    System.out.println(e.getMessage()) ;
		    logger.fatal(e.getMessage()) ;
		    System.exit(20) ;
		}
		if (debug) {
		    System.out.println("Delete: key=" + key) ;
		}
		logger.info("Delete: key=" + key + " awt=" + body.awt) ;
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
