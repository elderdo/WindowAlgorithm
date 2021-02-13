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
   $Revision:   1.8  $
       $Date:   17 Sep 2009 16:13:46  $
   $Workfile:   SpoPart.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\SpoPart.java.-arc  $
/*
/*   Rev 1.8   17 Sep 2009 16:13:46   zf297a
/*Added stack trace when there is an sql error
/*
/*   Rev 1.7   10 Jul 2009 12:39:22   zf297a
/*Added new columns for Spo 7.0.
/*
/*   Rev 1.6   08 Jul 2009 14:05:30   zf297a
/*Changed timestamp to last_modified for Spo 7.0
/*
/*   Rev 1.5   06 Apr 2009 21:46:38   zf297a
/*Substitue zero for unit_cost if it is null
/*
/*   Rev 1.4   06 Apr 2009 21:28:16   zf297a
/*Write out a zero for a null unit cost.
/*
/*   Rev 1.3   27 Mar 2009 10:35:00   zf297a
/*Fixed writeFlatFile to handle embedded double quotes for the part's description (aka nomenclature).
/*
/*   Rev 1.2   02 Dec 2008 11:49:52   zf297a
/*Fixed F1 (spo view) and F2 (amd x_ view) comments.  Removed s variable that was not being used.
/*
/*   Rev 1.1   02 Dec 2008 11:20:46   zf297a
/*Fixed F1 (spo view) and F2 (amd x_ view) comments.
/*
/*   Rev 1.0   05 Sep 2008 21:41:12   zf297a
/*Initial revision.
/*
/*   Rev 1.2   10 Jul 2008 08:38:58   zf297a
/*Fixed handling of exceptions for the FileOutputStream: issue exception message and exit with a retun code of 4.
/*
/*   Rev 1.1   13 May 2008 21:46:26   zf297a
/*Fixed sort order and table names.
/*
/*   Rev 1.0   05 May 2008 11:45:20   zf297a
/*Initial revision.

	*/

public class SpoPart implements Rec {
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

    static Logger logger = Logger.getLogger(SpoPart.class.getName());

    static FileOutputStream o1;
    static PrintStream p; // declare a print stream object  
    static long batchNum;
    static PreparedStatement pstmt;
    static long transTime = 0;        // keeps track of time spent in transactions
    static long flatFileTime = 0;     // keeps track of time spent writing flatfiles
    static String flatFileName;       // name of flat file to output   


    private static TableSnapshot F1 = null ; // v_part (spo)
    private static TableSnapshot F2 = null ; // x_part_v (amd)

    // loads the parameter from this class's properties file if they exist
    static private void loadParams() {
	try {

		java.util.Properties p        = new AppProperties(SpoPart.class.getName()).getProperties() ;

		useTestBatch = p.getProperty("useTestBatch", "false").equals("true");
		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","200") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","10") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","10") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","200") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","200") ).intValue() ;
		flatFileName = String.valueOf( p.getProperty("dumpFile", "SpoPart.csv"));
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
	} catch (java.lang.Exception e) {
		System.err.println("Warning: " + e.getMessage()) ;
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
	    batchNum = SpoBatch.createBatch("X_IMP_PART") ;			
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
	    F1 = new TableSnapshot(bufSize, new SpoPartFactory("SELECT * FROM " + F1InstanceSQL + 
	    "V_PART where end_date is null order by name", spo, prefetchSize)) ;
            F2 = new TableSnapshot(ageBufSize, new SpoPartFactory("SELECT * FROM " + F2InstanceSQL + 
	    "x_part_v where end_date is null order by name" , amd, prefetchSize)) ;
	    long readTime = System.currentTimeMillis() - readStart;
            w.diff(F1, F2) ;
	    p.close();
	    long batchStart = System.currentTimeMillis();
	    transTime = transTime + (System.currentTimeMillis() - batchStart);

	    System.out.println("time to write flat file:" + flatFileTime/1000 + "seconds");
	    System.out.println("time to read SQL and put in buffer:" + readTime/1000 + "seconds");
        }
        catch (SQLException e) {
            System.out.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;	 
	    p.close();   
            System.exit(4) ;
        }
        catch (ClassNotFoundException e) {
            System.out.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;	    
	    p.close();
            System.exit(4) ;
        }

	finally {
            updateCounts() ;

        }
    }

    private static void updateCounts() {

	if (F1 != null) {
	   	System.out.println("v_part=" + F1.getRecsIn()) ;
        	logger.info("v_part=" + F1.getRecsIn()) ;
	}
	if (F2 != null) {
	        System.out.println("x_part_v=" + F2.getRecsIn()) ;
        	logger.info("x_part_v=" + F2.getRecsIn()) ;
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
            Key k = (SpoPart.Key) o ;
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
        String         part_type ;
        String         description ;
        boolean        description_is_null ;
        double         cost ;
        boolean        cost_is_null ;
        java.sql.Date  begin_date ;
        boolean	       begin_date_is_null ;
        java.sql.Date  end_date ;
        boolean	       end_date_is_null ;
        BigDecimal     holding_cost_rate ;
        boolean	       holding_cost_rate_is_null ;
        double         fixed_order_cost ;
        boolean	       fixed_order_cost_is_null ;
        String         is_order_policy_roq_based ;
        boolean        is_order_policy_roq_based_is_null ;
        String         is_reparable ;
        boolean        is_reparable_is_null ;
        BigDecimal     repair_cost ;
        boolean        repair_cost_is_null ;
        double         decay_rate ;
        boolean        decay_rate_is_null ;
        String         generate_new_buy ;
        boolean        generate_new_buy_is_null ;
        String         generate_repair ;
        boolean        generate_repair_is_null ;
        String         generate_allocation ;
        boolean        generate_allocation_is_null ;
        String         generate_transshipment ;
        boolean        generate_transshipment_is_null ;
        int	       max_qty_allowance ;
        boolean        max_qty_allowance_is_null ;
        int            max_total_tsl ;
        boolean        max_total_tsl_is_null ;
        java.sql.Date last_modified ;
        boolean        last_modified_is_null ;
        String         is_planned ;
        boolean        is_planned_is_null ;
        String         attribute_1 ;
        boolean        attribute_1_is_null ;
        String         attribute_2 ;
        boolean        attribute_2_is_null ;
        String         attribute_3 ;
        boolean        attribute_3_is_null ;
        String         attribute_4 ;
        boolean        attribute_4_is_null ;
        String         attribute_5 ;
        boolean        attribute_5_is_null ;
        String         attribute_6 ;
        boolean        attribute_6_is_null ;
        String         attribute_7 ;
        boolean        attribute_7_is_null ;
        String         attribute_8 ;
        boolean        attribute_8_is_null ;
        String         attribute_9 ;
        boolean        attribute_9_is_null ;
        String         attribute_10 ;
        boolean        attribute_10_is_null ;
        String         attribute_11 ;
        boolean        attribute_11_is_null ;
        String         attribute_12 ;
        boolean        attribute_12_is_null ;
        String         attribute_13 ;
        boolean        attribute_13_is_null ;
        String         attribute_14 ;
        boolean        attribute_14_is_null ;
        String         attribute_15 ;
        boolean        attribute_15_is_null ;
        String         attribute_16 ;
        boolean        attribute_16_is_null ;
        String         attribute_17 ;
        boolean        attribute_17_is_null ;
        String         attribute_18 ;
        boolean        attribute_18_is_null ;
        String         attribute_19 ;
        boolean        attribute_19_is_null ;
        String         attribute_20 ;
        boolean        attribute_20_is_null ;
        String         attribute_21 ;
        boolean        attribute_21_is_null ;
        String         attribute_22 ;
        boolean        attribute_22_is_null ;
        String         attribute_23 ;
        boolean        attribute_23_is_null ;
        String         attribute_24 ;
        boolean        attribute_24_is_null ;
        String         attribute_25 ;
        boolean        attribute_25_is_null ;
        String         attribute_26 ;
        boolean        attribute_26_is_null ;
        String         attribute_27 ;
        boolean        attribute_27_is_null ;
        String         attribute_28 ;
        boolean        attribute_28_is_null ;
        String         attribute_29 ;
        boolean        attribute_29_is_null ;
        String         attribute_30 ;
        boolean        attribute_30_is_null ;
        String         attribute_31 ;
        boolean        attribute_31_is_null ;
        String         attribute_32 ;
        boolean        attribute_32_is_null ;
        String         material_class ;
        boolean        material_class_is_null ;
        int            weight ;
        boolean        weight_is_null ;
        int            volume ;
        boolean        volume_is_null ;
        String         is_exempt ;
        boolean        is_exempt_is_null ;
        String         ignore_weight_and_volume ;
        boolean        ignore_weight_and_volume_is_null ;
        String         is_seasonal ;
        boolean        is_seasonal_is_null ;
        String         is_cannibalizable ;
        boolean        is_cannibalizable_is_null ;
        String         partial_one_way_supersession ;
        boolean        partial_one_way_supersession_is_null ;
        String         is_new ;
        boolean        is_new_is_null ;
        String         like_part ;
        boolean        like_part_is_null ;
        double         new_buy_lead_time ;
        boolean        new_buy_lead_time_is_null ;
        double         repair_lead_time ;
        boolean        repair_lead_time_is_null ;
        int            new_buy_lead_time_variance ;
        boolean        new_buy_lead_time_variance_is_null ;
        int            repair_lead_time_variance ;
        boolean        repair_lead_time_variance_is_null ;
        int            forecast_risk_multiplier ;
        boolean        forecast_risk_multiplier_is_null ;

        public String toString() {
	 return "part_type=" + part_type
	 + " description=" + description
	 + " COST=" + cost
	 + " begin_date=" + begin_date
	 + " end_date=" + end_date
	 + " holding_cost_rate=" + holding_cost_rate
	 + " fixed_order_cost=" + fixed_order_cost
	 + " is_order_policy_roq_based=" + is_order_policy_roq_based
	 + " is_reparable=" + is_reparable
	 + " repair_cost=" + repair_cost
	 + " decay_rate=" + decay_rate
	 + " generate_new_buy=" + generate_new_buy
	 + " generate_repair=" + generate_repair
	 + " generate_allocation=" + generate_allocation
	 + " generate_transshipment=" + generate_transshipment
	 + " max_qty_allowance=" + max_qty_allowance
	 + " max_total_tsl=" + max_total_tsl
	 + " last_modified=" + last_modified
	 + " is_planned=" + is_planned
	 + " attribute_1=" + attribute_1
	 + " attribute_2=" + attribute_2
	 + " attribute_3=" + attribute_3
	 + " attribute_4=" + attribute_4
	 + " attribute_5=" + attribute_5
	 + " attribute_6=" + attribute_6
	 + " attribute_7=" + attribute_7
	 + " attribute_8=" + attribute_8
	 + " attribute_9=" + attribute_9
	 + " attribute_10=" + attribute_10
	 + " attribute_11=" + attribute_11
	 + " attribute_12=" + attribute_12
	 + " attribute_13=" + attribute_13
	 + " attribute_14=" + attribute_14
	 + " attribute_15=" + attribute_15
	 + " attribute_16=" + attribute_16
	 + " attribute_17=" + attribute_17
	 + " attribute_18=" + attribute_18
	 + " attribute_19=" + attribute_19
	 + " attribute_20=" + attribute_20
	 + " attribute_21=" + attribute_21
	 + " attribute_22=" + attribute_22
	 + " attribute_23=" + attribute_23
	 + " attribute_24=" + attribute_24
	 + " attribute_25=" + attribute_25
	 + " attribute_26=" + attribute_26
	 + " attribute_27=" + attribute_27
	 + " attribute_28=" + attribute_28
	 + " attribute_29=" + attribute_29
	 + " attribute_30=" + attribute_30
	 + " attribute_31=" + attribute_31
	 + " attribute_32=" + attribute_32
	 + " material_class=" + material_class
	 + " weight=" + weight
	 + " volume=" + volume
	 + " is_exempt=" + is_exempt
	 + " ignore_weight_and_volume=" + ignore_weight_and_volume
	 + " is_seasonal=" + is_seasonal
	 + " is_cannibalizable=" + is_cannibalizable
	 + " partial_one_way_supersession=" + partial_one_way_supersession 
	 + " is_new=" + is_new 
	 + " like_part=" + like_part 
	 + " new_buy_lead_time=" + new_buy_lead_time 
	 + " repair_lead_time=" + repair_lead_time 
	 + " new_buy_lead_time_variance=" + new_buy_lead_time_variance 
	 + " repair_lead_time_variance=" + repair_lead_time_variance 
	 + " forecast_risk_multiplier=" + forecast_risk_multiplier ;

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
        boolean equal(int i1, boolean null1, int i2, boolean null2) {
	    // logger.debug("int: i1=" + i1 + " null1=" + null1) ;
	    // logger.debug("int: i2=" + i2 + " null2=" + null2) ;
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
	    // logger.debug("double: d1=" + d1 + " null1=" + null1) ;
	    // logger.debug("double: d2=" + d2 + " null2=" + null2) ;
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
	    // logger.debug("BigDecimal: d1=" + d1 + " null1=" + null1) ;
	    // logger.debug("BigDecimal: d2=" + d2 + " null2=" + null1) ;
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
        private boolean isDiff(boolean result, String fieldName, String field1, String field2) {
                if (!field1.equals(field2)) {
                    	logger.debug("key = " + key + " field: " + fieldName + " *" + field1 + "* *" + field2 + "*") ;
		}
		return field1.equals(field2) ;
	}

        private void showDiff(boolean result, String fieldName, String field1, String field2) {
                if (!(field1 + "").equals((field2 + ""))) {
		    showDiffCnt++ ;
		    // if (showDiffCnt % showDiffThreshold == 0) {
                    	logger.debug("key = " + key + " field: " + fieldName + " *" + field1 + "* *" + field2 + "*") ;
		    // }
                }
        }

        public boolean equals(Object o) {
            Body b = (Body) o ;
            boolean result = true ;
            result = equal(b.part_type, part_type) ;
            showDiff(result, "part_type", b.part_type, part_type) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.description,description) ;
            showDiff(result, "description", b.description, description) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.cost,b.cost_is_null,cost,cost_is_null) ;
            showDiff(result, "cost", (b.cost_is_null ? "null" : b.cost + ""), (cost_is_null ? "null" : cost + "") ) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.holding_cost_rate,b.holding_cost_rate_is_null,holding_cost_rate, holding_cost_rate_is_null) ;
            showDiff(result, "holding_cost_rate", (b.holding_cost_rate_is_null ? "null" : b.holding_cost_rate + ""), (holding_cost_rate_is_null ? "null" : holding_cost_rate + "")) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.fixed_order_cost,b.fixed_order_cost_is_null,fixed_order_cost,fixed_order_cost_is_null) ;
            showDiff(result, "fixed_order_cost", (b.fixed_order_cost_is_null ? "null" : b.fixed_order_cost + ""), (fixed_order_cost_is_null ? "null" : fixed_order_cost + "")) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.is_order_policy_roq_based,is_order_policy_roq_based) ;
            showDiff(result, "is_order_policy_roq_based", b.is_order_policy_roq_based, is_order_policy_roq_based) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.is_reparable,is_reparable) ;
            showDiff(result, "is_reparable", b.is_reparable, is_reparable) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.repair_cost,b.repair_cost_is_null,repair_cost,repair_cost_is_null) ;
            showDiff(result, "repair_cost", (b.repair_cost_is_null ? "null" : b.repair_cost + ""), (repair_cost_is_null ? "null" : repair_cost + "")) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.decay_rate,b.decay_rate_is_null,decay_rate,decay_rate_is_null) ;
            showDiff(result, "decay_rate", (b.decay_rate_is_null ? "null" : b.decay_rate + ""), (decay_rate_is_null ? "null" : decay_rate + "")) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.generate_new_buy,generate_new_buy) ;
            showDiff(result, "generate_new_buy", b.generate_new_buy, generate_new_buy) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.generate_repair,generate_repair) ;
            showDiff(result, "generate_repair", b.generate_repair, generate_repair) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.generate_allocation,generate_allocation) ;
            showDiff(result, "generate_allocation", b.generate_allocation, generate_allocation) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.generate_transshipment,generate_transshipment) ;
            showDiff(result, "generate_transshipment", b.generate_transshipment, generate_transshipment) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.max_qty_allowance,b.max_qty_allowance_is_null,max_qty_allowance, max_qty_allowance_is_null) ;
            showDiff(result, "max_qty_allowance", (b.max_qty_allowance_is_null ? "null" : b.max_qty_allowance + ""), (max_qty_allowance_is_null ? "null" : max_qty_allowance + "")) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.max_total_tsl,b.max_total_tsl_is_null,max_total_tsl,max_total_tsl_is_null) ;
            showDiff(result, "max_total_tsl", (b.max_total_tsl_is_null ? "null" : b.max_total_tsl + ""), (max_total_tsl_is_null ? "null" : max_total_tsl + "")) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.is_planned,is_planned) ;
            showDiff(result, "is_planned", b.is_planned, is_planned) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_1,attribute_1) ;
            showDiff(result, "attribute_1", b.attribute_1, attribute_1) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_2,attribute_2) ;
            showDiff(result, "attribute_2", b.attribute_2, attribute_2) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_3,attribute_3) ;
            showDiff(result, "attribute_3", b.attribute_3, attribute_3) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_4,attribute_4) ;
            showDiff(result, "attribute_4", b.attribute_4, attribute_4) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_5,attribute_5) ;
            showDiff(result, "attribute_5", b.attribute_5, attribute_5) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_6,attribute_6) ;
            showDiff(result, "attribute_6", b.attribute_6, attribute_6) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_7,attribute_7) ;
            showDiff(result, "attribute_7", b.attribute_7, attribute_7) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_8,attribute_8) ;
            showDiff(result, "attribute_8", b.attribute_8, attribute_8) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_9,attribute_9) ;
            showDiff(result, "attribute_9", b.attribute_9, attribute_9) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_10,attribute_10) ;
            showDiff(result, "attribute_10", b.attribute_10, attribute_10) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_11,attribute_11) ;
            showDiff(result, "attribute_11", b.attribute_11, attribute_11) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_12,attribute_12) ;
            showDiff(result, "attribute_12", b.attribute_12, attribute_12) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_13,attribute_13) ;
            showDiff(result, "attribute_13", b.attribute_13, attribute_13) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_14,attribute_14) ;
            showDiff(result, "attribute_14", b.attribute_14, attribute_14) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_15,attribute_15) ;
            showDiff(result, "attribute_15", b.attribute_15, attribute_15) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_16,attribute_16) ;
            showDiff(result, "attribute_16", b.attribute_16, attribute_16) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_17,attribute_17) ;
            showDiff(result, "attribute_17", b.attribute_17, attribute_17) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_18,attribute_18) ;
            showDiff(result, "attribute_18", b.attribute_18, attribute_18) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_19,attribute_19) ;
            showDiff(result, "attribute_19", b.attribute_19, attribute_19) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_20,attribute_20) ;
            showDiff(result, "attribute_20", b.attribute_20, attribute_20) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_21,attribute_21) ;
            showDiff(result, "attribute_21", b.attribute_21, attribute_21) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_22,attribute_22) ;
            showDiff(result, "attribute_22", b.attribute_22, attribute_22) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_23,attribute_23) ;
            showDiff(result, "attribute_23", b.attribute_23, attribute_23) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_24,attribute_24) ;
            showDiff(result, "attribute_24", b.attribute_24, attribute_24) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_25,attribute_25) ;
            showDiff(result, "attribute_25", b.attribute_25, attribute_25) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_26,attribute_26) ;
            showDiff(result, "attribute_26", b.attribute_26, attribute_26) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_27,attribute_27) ;
            showDiff(result, "attribute_27", b.attribute_27, attribute_27) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_28,attribute_28) ;
            showDiff(result, "attribute_28", b.attribute_28, attribute_28) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_29,attribute_29) ;
            showDiff(result, "attribute_29", b.attribute_29, attribute_29) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_30,attribute_30) ;
            showDiff(result, "attribute_30", b.attribute_30, attribute_30) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_31,attribute_31) ;
            showDiff(result, "attribute_31", b.attribute_31, attribute_31) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.attribute_32,attribute_32) ;
            showDiff(result, "attribute_32", b.attribute_32, attribute_32) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.material_class,material_class) ;
            showDiff(result, "material_class", b.material_class, material_class) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.weight,b.weight_is_null,weight,weight_is_null) ;
            showDiff(result, "weight", (b.weight_is_null ? "null" : b.weight + ""), (weight_is_null ? "null" : weight + "")) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.volume,b.volume_is_null,volume,volume_is_null) ;
            showDiff(result, "volume", (b.volume_is_null ? "null" : b.volume + ""), (volume_is_null ? "null" : volume + "")) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.is_exempt,is_exempt) ;
            showDiff(result, "is_exempt", b.is_exempt, is_exempt) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.ignore_weight_and_volume,ignore_weight_and_volume) ;
            showDiff(result, "isgnore_weight_and_volume", b.ignore_weight_and_volume, ignore_weight_and_volume) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            showDiff(result, "is_seasonal", b.is_seasonal, is_seasonal) ;
            result = result && equal(b.is_cannibalizable,is_cannibalizable) ;
            showDiff(result, "is_cannibalizable", b.is_cannibalizable, is_cannibalizable) ;
            result = result && equal(b.partial_one_way_supersession,partial_one_way_supersession) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            showDiff(result, "partial_one_way_supersession", b.partial_one_way_supersession, partial_one_way_supersession) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            showDiff(result, "is_new", b.is_new, is_new) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            showDiff(result, "like_part", b.like_part, like_part) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.new_buy_lead_time,b.new_buy_lead_time_is_null,new_buy_lead_time,new_buy_lead_time_is_null) ;
            showDiff(result, "new_buy_lead_time", (b.new_buy_lead_time_is_null ? "null" : b.new_buy_lead_time + ""), (new_buy_lead_time_is_null ? "null" : new_buy_lead_time + "")) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            result = result && equal(b.repair_lead_time,b.repair_lead_time_is_null,repair_lead_time,repair_lead_time_is_null) ;
            showDiff(result, "repair_lead_time", (b.repair_lead_time_is_null ? "null" : b.repair_lead_time + ""), (repair_lead_time_is_null ? "null" : repair_lead_time + "")) ;
	    if (!result) {logger.debug("result:" + result) ; return result ; }
            return result ;
        }
    }
    Body body ;
    Body bodyToCompare;

    SpoPart(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.name = r.getString("NAME") ;

        body = new Body() ;
        body.part_type = r.getString("PART_TYPE") ;
        body.description = r.getString("DESCRIPTION") ;
        body.description_is_null = r.wasNull() ;

        body.cost = r.getDouble("COST") ;
        body.cost_is_null = r.wasNull() ;
	if (body.cost_is_null) {
		body.cost = 0 ;
	}

        body.begin_date = r.getDate("begin_date") ;
        body.begin_date_is_null = r.wasNull() ;
        body.end_date = r.getDate("end_date") ;
        body.end_date_is_null = r.wasNull() ;
        body.holding_cost_rate = r.getBigDecimal("HOLDING_COST_RATE") ;
        body.holding_cost_rate_is_null = r.wasNull() ;
        body.fixed_order_cost = r.getDouble("FIXED_ORDER_COST") ;
        body.is_order_policy_roq_based = r.getString("IS_ORDER_POLICY_ROQ_BASED") ;
        body.is_order_policy_roq_based_is_null = r.wasNull() ;
        body.is_reparable = r.getString("IS_REPARABLE") ;
        body.is_reparable_is_null = r.wasNull() ;
        body.repair_cost = r.getBigDecimal("REPAIR_COST") ;
        body.repair_cost_is_null = r.wasNull() ;
        body.decay_rate = r.getDouble("DECAY_RATE") ;
        body.decay_rate_is_null = r.wasNull() ;
        body.generate_new_buy = r.getString("GENERATE_NEW_BUY") ;
        body.generate_new_buy_is_null = r.wasNull() ;
        body.generate_repair = r.getString("GENERATE_REPAIR") ;
        body.generate_repair_is_null = r.wasNull() ;
        body.generate_allocation = r.getString("GENERATE_ALLOCATION") ;
        body.generate_allocation_is_null = r.wasNull() ;
        body.generate_transshipment = r.getString("GENERATE_TRANSSHIPMENT") ;
        body.generate_transshipment_is_null = r.wasNull() ;
        body.max_qty_allowance = r.getInt("MAX_QTY_ALLOWANCE") ;
        body.max_qty_allowance_is_null = r.wasNull() ;
        body.max_total_tsl = r.getInt("MAX_TOTAL_TSL") ;
        body.max_total_tsl_is_null = r.wasNull() ;
        body.last_modified = r.getDate("LAST_MODIFIED") ;
        body.last_modified_is_null = r.wasNull() ;
        body.is_planned = r.getString("IS_PLANNED") ;
        body.is_planned_is_null = r.wasNull() ;
        body.attribute_1 = r.getString("ATTRIBUTE_1") ;
        body.attribute_1_is_null = r.wasNull() ;
        body.attribute_2 = r.getString("ATTRIBUTE_2") ;
        body.attribute_2_is_null = r.wasNull() ;
        body.attribute_3 = r.getString("ATTRIBUTE_3") ;
        body.attribute_3_is_null = r.wasNull() ;
        body.attribute_4 = r.getString("ATTRIBUTE_4") ;
        body.attribute_4_is_null = r.wasNull() ;
        body.attribute_5 = r.getString("ATTRIBUTE_5") ;
        body.attribute_5_is_null = r.wasNull() ;
        body.attribute_6 = r.getString("ATTRIBUTE_6") ;
        body.attribute_6_is_null = r.wasNull() ;
        body.attribute_7 = r.getString("ATTRIBUTE_7") ;
        body.attribute_7_is_null = r.wasNull() ;
        body.attribute_8 = r.getString("ATTRIBUTE_8") ;
        body.attribute_8_is_null = r.wasNull() ;
        body.attribute_9 = r.getString("ATTRIBUTE_9") ;
        body.attribute_9_is_null = r.wasNull() ;
        body.attribute_10 = r.getString("ATTRIBUTE_10") ;
        body.attribute_10_is_null = r.wasNull() ;
        body.attribute_11 = r.getString("ATTRIBUTE_11") ;
        body.attribute_11_is_null = r.wasNull() ;
        body.attribute_12 = r.getString("ATTRIBUTE_12") ;
        body.attribute_12_is_null = r.wasNull() ;
        body.attribute_13 = r.getString("ATTRIBUTE_13") ;
        body.attribute_13_is_null = r.wasNull() ;
        body.attribute_14 = r.getString("ATTRIBUTE_14") ;
        body.attribute_14_is_null = r.wasNull() ;
        body.attribute_15 = r.getString("ATTRIBUTE_15") ;
        body.attribute_15_is_null = r.wasNull() ;
        body.attribute_16 = r.getString("ATTRIBUTE_16") ;
        body.attribute_16_is_null = r.wasNull() ;
        body.attribute_17 = r.getString("ATTRIBUTE_17") ;
        body.attribute_17_is_null = r.wasNull() ;
        body.attribute_18 = r.getString("ATTRIBUTE_18") ;
        body.attribute_18_is_null = r.wasNull() ;
        body.attribute_19 = r.getString("ATTRIBUTE_19") ;
        body.attribute_19_is_null = r.wasNull() ;
        body.attribute_20 = r.getString("ATTRIBUTE_20") ;
        body.attribute_20_is_null = r.wasNull() ;
        body.attribute_21 = r.getString("ATTRIBUTE_21") ;
        body.attribute_21_is_null = r.wasNull() ;
        body.attribute_22 = r.getString("ATTRIBUTE_22") ;
        body.attribute_22_is_null = r.wasNull() ;
        body.attribute_23 = r.getString("ATTRIBUTE_23") ;
        body.attribute_23_is_null = r.wasNull() ;
        body.attribute_24 = r.getString("ATTRIBUTE_24") ;
        body.attribute_24_is_null = r.wasNull() ;
        body.attribute_25 = r.getString("ATTRIBUTE_25") ;
        body.attribute_25_is_null = r.wasNull() ;
        body.attribute_26 = r.getString("ATTRIBUTE_26") ;
        body.attribute_26_is_null = r.wasNull() ;
        body.attribute_27 = r.getString("ATTRIBUTE_27") ;
        body.attribute_27_is_null = r.wasNull() ;
        body.attribute_28 = r.getString("ATTRIBUTE_28") ;
        body.attribute_28_is_null = r.wasNull() ;
        body.attribute_29 = r.getString("ATTRIBUTE_29") ;
        body.attribute_29_is_null = r.wasNull() ;
        body.attribute_30 = r.getString("ATTRIBUTE_30") ;
        body.attribute_30_is_null = r.wasNull() ;
        body.attribute_31 = r.getString("ATTRIBUTE_31") ;
        body.attribute_31_is_null = r.wasNull() ;
        body.attribute_32 = r.getString("ATTRIBUTE_32") ;
        body.attribute_32_is_null = r.wasNull() ;
        body.material_class = r.getString("MATERIAL_CLASS") ;
        body.material_class_is_null = r.wasNull() ;
        body.weight = r.getInt("WEIGHT") ;
        body.weight_is_null = r.wasNull() ;
        body.volume = r.getInt("VOLUME") ;
        body.volume_is_null = r.wasNull() ;
        body.is_exempt = r.getString("IS_EXEMPT") ;
        body.is_exempt_is_null = r.wasNull() ;
        body.ignore_weight_and_volume = r.getString("IGNORE_WEIGHT_AND_VOLUME") ;
        body.ignore_weight_and_volume_is_null = r.wasNull() ;
        body.is_seasonal = r.getString("IS_SEASONAL") ;
        body.is_seasonal_is_null = r.wasNull() ;
        body.is_cannibalizable = r.getString("IS_CANNIBALIZABLE") ;
        body.is_cannibalizable_is_null = r.wasNull() ;
        body.partial_one_way_supersession = r.getString("PARTIAL_ONE_WAY_SUPERSESSION") ;
        body.partial_one_way_supersession_is_null = r.wasNull() ;
        body.is_new = r.getString("IS_NEW") ;
        body.is_new_is_null = r.wasNull() ;
        body.like_part = r.getString("LIKE_PART") ;
        body.like_part_is_null = r.wasNull() ;
        body.new_buy_lead_time = r.getDouble("NEW_BUY_LEAD_TIME") ;
        body.new_buy_lead_time_is_null = r.wasNull() ;
        body.repair_lead_time = r.getDouble("REPAIR_LEAD_TIME") ;
        body.repair_lead_time_is_null = r.wasNull() ;
        body.new_buy_lead_time_variance = r.getInt("NEW_BUY_LEAD_TIME_VARIANCE") ;
        body.new_buy_lead_time_variance_is_null = r.wasNull() ;
        body.repair_lead_time_variance = r.getInt("REPAIR_LEAD_TIME_VARIANCE") ;
        body.repair_lead_time_variance_is_null = r.wasNull() ;
        body.forecast_risk_multiplier = r.getInt("FORECAST_RISK_MULTIPLIER") ;
        body.forecast_risk_multiplier_is_null = r.wasNull() ;
        }
        catch (java.sql.SQLException e) {
            updateCounts() ;
            System.out.println(e.getMessage() ) ;
            logger.fatal(e.getMessage()) ;
	    e.printStackTrace() ;
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

	// logically delete the part by filling in end_date
	body.end_date = new java.sql.Date(new java.util.Date(System.currentTimeMillis()).getTime()) ;
	// logical deletion requires the import to do an update .. that's why UPD is passed
        writeFlatFile("UPD");
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
		p.println("\"" + key.name + "\", \"" 
					+ body.part_type + "\", \"" 
					+ body.description.replaceAll("\"","\"\"") + "\", " 
					+ (body.cost_is_null ? "0" : body.cost + "") + ", " 
					+ convertDate(body.begin_date) + ", " 
					+ convertDate(body.end_date) + ", " 
				       	+ (body.holding_cost_rate_is_null ? "\"null\"" : body.holding_cost_rate + "") + ", " 
					+ (body.fixed_order_cost_is_null ? "\"null\"" : body.fixed_order_cost + "") + ", \"" 
					+ body.is_order_policy_roq_based + "\", \"" 
					+ body.is_reparable + "\", " 
					+ (body.repair_cost_is_null ? "\"null\"" : body.repair_cost + "") + ", " 
					+ (body.decay_rate_is_null ? "\"null\"" : body.decay_rate + "") + ", \"" 
					+ body.generate_new_buy + "\", \"" 
					+ body.generate_repair + "\", \"" 
					+ body.generate_allocation + "\", \"" 
					+ body.generate_transshipment + "\", " 
					+ (body.max_qty_allowance_is_null ? "\"null\"" : body.max_qty_allowance + "") + ", \"" 
					+ body.is_planned + "\", \"" 
					+ body.attribute_1 + "\", \""
					+ body.attribute_2 + "\", \""
					+ body.attribute_3 + "\", \""
					+ body.attribute_4 + "\", \""
					+ body.attribute_5 + "\", \""
					+ body.attribute_6 + "\", \""
					+ body.attribute_7 + "\", \""
					+ body.attribute_8 + "\", \""
					+ body.attribute_9 + "\", \""
					+ body.attribute_10 + "\", \""
					+ body.attribute_11 + "\", \""
					+ body.attribute_12 + "\", \""
					+ body.attribute_13 + "\", \""
					+ body.attribute_14 + "\", \""
					+ body.attribute_15 + "\", \""
					+ body.attribute_16 + "\", \""
					+ body.attribute_17 + "\", \""
					+ body.attribute_18 + "\", \""
					+ body.attribute_19 + "\", \""
					+ body.attribute_20 + "\", \""
					+ body.attribute_21 + "\", \""
					+ body.attribute_22 + "\", \""
					+ body.attribute_23 + "\", \""
					+ body.attribute_24 + "\", \""
					+ body.attribute_25 + "\", \""
					+ body.attribute_26 + "\", \""
					+ body.attribute_27 + "\", \""
					+ body.attribute_28 + "\", \""
					+ body.attribute_29 + "\", \""
					+ body.attribute_30 + "\", \""
					+ body.attribute_31 + "\", \""
					+ body.attribute_32 + "\", \""
					+ body.material_class + "\", " 
					+ (body.weight_is_null ? "\"null\"" : body.weight + "") + ", " 
					+ (body.volume_is_null ? "\"null\"" : body.volume + "") + ", \"" 
					+ body.is_exempt + "\", \"" 
					+ body.ignore_weight_and_volume + "\", \"" 
					+ body.is_seasonal + "\", " 
					+ (body.max_total_tsl_is_null ? "\"null\"" : body.max_total_tsl + "") + ", \"" 
					+ body.is_cannibalizable + "\", \"" 
					+ body.partial_one_way_supersession + "\", \"" 
					+ body.is_new + "\", \"" 
					+ body.like_part + "\", " 
					+ (body.new_buy_lead_time_is_null ? "\"null\"" : body.new_buy_lead_time + "") + ", " 
					+ (body.repair_lead_time_is_null ? "\"null\"" : body.repair_lead_time + "") + ", " 
					+ body.new_buy_lead_time_variance + ", " 
					+ body.repair_lead_time_variance + ", " 
					+ body.forecast_risk_multiplier + ", \"" 
					+ action + "\", " + batchNum);
			flatFileTime = flatFileTime + (System.currentTimeMillis() - flatStart);
	}

}
