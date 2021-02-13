import java.sql.* ;
import java.io.* ;
import java.math.BigDecimal ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

/*   $Author:   zf297a  $
   $Revision:   1.37  $
       $Date:   24 Mar 2009 23:22:54  $
   $Workfile:   SparePart.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\SparePart.java-arc  $
   
      Rev 1.37   24 Mar 2009 23:22:54   zf297a
   Added RecsNotEqualException to allow for early exit from the equals method.
   
      Rev 1.36   24 Mar 2009 23:07:22   zf297a
   Made setDebug static
   
      Rev 1.35   24 Mar 2009 14:42:58   zf297a
   Added truning on debug for the amd_spare_parts_pkg.  
   
   Removed sending debug to System.out.println.  
   
   Added outputing an sql file that could be used by sqlplus to re-test the code with the same test data.
   
      Rev 1.34   31 Jan 2008 12:18:16   zf297a
   Added method loadParams
   Use properties file to get parameters
   Use DBConnection to get the connection information
   Changed showDiff to use a threshold value
   Add no_op to insert, update, & delete for testing purposes
   
      Rev 1.33   29 May 2007 15:55:24   zf297a
   Added wesm_indicator
   
      Rev 1.32   18 Apr 2007 23:35:02   zf297a
   Changed cost_to_repair_off_base to BigDecimal.  Once this was done, repeated executions resulted in NO updates.
   
      Rev 1.31   12 Feb 2007 14:10:24   zf297a
   Added fields amc_demand and amc_demand_cleaned
   
      Rev 1.30   Mar 08 2006 13:14:52   zf297a
   Made all mtbdr fields into data types of "double".
   
      Rev 1.29   Mar 08 2006 09:05:02   zf297a
   Added mtbdr_computed.  Corrected check for mtbdr_isnull!
   
      Rev 1.28   Jan 24 2006 13:28:18   zf297a
   Fixed error of not checking for a null value for the following fields:
   qpei_weighted
   condemn_avg
   criticality
   nrts_avg
   rts_avg
   
      Rev 1.27   Oct 28 2005 09:28:22   zf297a
   Fixed equal function: the result variable was NOT being and'ed with the equal function for the mic field.  Fixed it by adding "result &&" as part of the statement.
   
      Rev 1.26   Aug 09 2005 08:05:34   zf297a
   Test nsn for every part: prime and alternate (same change as branch ver 1.22.1)
   
      Rev 1.25   Jun 15 2005 14:22:26   c970183
   Added columns time_to_repair_off_base and cost_to_repair_off_base
   
      Rev 1.24   May 02 2005 13:10:36   c970183
   Finished adding cleaned data to the SparePart diff - corrected some data types from int to double.  Move mmac to be with other prime part data of equals method
   
      Rev 1.23   Apr 18 2005 11:03:54   c970183
   Added new cleaned data and other data to be maintained by the SparePart diff.  Switched to the new stored procedures.
   
      Rev 1.22   18 Jan 2005 10:39:00   c402417
   Added Mmac and Unit_of_Issue

      Rev 1.20   30 Aug 2004 08:13:22   c970183
   Fixed equal method for Double's and BigDecimal - used compareTo method.

      Rev 1.16   17 Sep 2002 07:46:40   c970183
   Accepts the ini file via a command line argument.

      Rev 1.15   27 Aug 2002 09:03:34   c970183
   Added seconds to the date/time format returned by the now method.

      Rev 1.14   26 Aug 2002 11:19:20   c970183
   Added start/end time messages

      Rev 1.9   05 Apr 2002 06:45:44   c970183
   Mic code should only be compared for equality for prime parts.  Aliased the mic code from tmp_amd_spare_parts to mic_code_lowest so it would be the same as the name in amd_spare_parts.
  */
public class SparePart implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug    = false;
    static boolean no_op    = false;
    static int bufSize      = 150000 ;
    static int ageBufSize   = 150000 ;
    static int prefetchSize = 5000 ;
    static int debugThreshold = 50000 ;
    static int showDiffThreshold = 1000 ;
    static String sqlFile = "../data/SparePart.sql" ;
    static FileOutputStream out ;
    static PrintStream sqlPrint ;

    static Logger logger = Logger.getLogger(SparePart.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_spare_parts
    private static TableSnapshot F2 = null ; // tmp_amd_spare_parts

    static private void loadParams() {
	try {
		java.util.Properties p        = new AppProperties(SparePart.class.getName()).getProperties() ;

       		sqlFile = p.getProperty("sqlFile","../data/SparePart.sql") ;
       		debug = p.getProperty("debug","false").equals("true") ;
       		no_op = p.getProperty("no_op","false").equals("true") ;
		bufSize = Integer.valueOf( p.getProperty("bufSize","73") ).intValue() ;
		debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","50000") ).intValue() ;
		showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","1000") ).intValue() ;
		ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","73") ).intValue() ;
		prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","10") ).intValue() ;
		logger.debug("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" + prefetchSize + " no_op=" + no_op
				+ " debug=" + debug + " debugThreshold=" + debugThreshold + " showDiffThreshold=" + showDiffThreshold) ;

		if (debug) {
			System.out.println("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" + prefetchSize + " no_op=" + no_op + " debugThreshold=" + debugThreshold + " showDiffThreshold=" + showDiffThreshold) ;
			out = new FileOutputStream(sqlFile) ;
			sqlPrint = new PrintStream(out) ;
			sqlPrint.println("variable rc number\n\n") ;
			setDebug() ;
		}

	} catch (java.io.IOException e) {
		System.err.println("SparePart: warning: " + e.getMessage()) ;
	} catch (java.lang.Exception e) {
		System.err.println("SparePart: warning: " + e.getMessage()) ;
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
            F1 = new TableSnapshot(bufSize, new SparePartsFactory("Select parts.part_no part_no, parts.mfgr mfgr, items.mic_code_lowest, parts.acquisition_advice_code, date_icp, disposal_cost, erc, icp_ind, nomenclature, order_lead_time, order_quantity, order_uom, scrap_value, serial_flag, shelf_life, unit_cost, unit_volume, parts.nsn, smr_code, item_type, planner_code, nsn_type, prime_ind, items.mmac, parts.unit_of_issue, mtbdr, qpei_weighted, condemn_avg_cleaned, criticality_cleaned, mtbdr_cleaned, mtbdr_computed, nrts_avg_cleaned, cost_to_repair_off_base_cleand, time_to_repair_off_base_cleand, order_lead_time_cleaned, planner_code_cleaned, rts_avg_cleaned, smr_code_cleaned, unit_cost_cleaned, condemn_avg, criticality, nrts_avg, rts_avg, cost_to_repair_off_base, time_to_repair_off_base, amc_demand, amc_demand_cleaned, wesm_indicator from amd_spare_parts parts, amd_national_stock_items items, amd_nsns nsns, amd_nsi_parts lnks where parts.action_code != 'D' and parts.nsn = nsns.nsn and nsns.nsi_sid = items.nsi_sid and nsns.nsi_sid = lnks.nsi_sid and parts.part_no = lnks.part_no  and lnks.unassignment_date is null order by parts.part_no")) ;
            F2 = new TableSnapshot(bufSize, new SparePartsFactory("Select part_no, mfgr, mic mic_code_lowest, acquisition_advice_code, date_icp, disposal_cost, erc, icp_ind, nomenclature, order_lead_time, order_quantity, order_uom, scrap_value, serial_flag, shelf_life, unit_cost, unit_volume, nsn, smr_code, item_type, planner_code, nsn_type, prime_ind, mmac, unit_of_issue, mtbdr, qpei_weighted, condemn_avg_cleaned, criticality_cleaned, mtbdr_cleaned, mtbdr_computed, nrts_avg_cleaned, cost_to_repair_off_base_cleand, time_to_repair_off_base_cleand, order_lead_time_cleaned, planner_code_cleaned, rts_avg_cleaned, smr_code_cleaned, unit_cost_cleaned, condemn_avg, criticality, nrts_avg, rts_avg, cost_to_repair_off_base, time_to_repair_off_base, amc_demand, amc_demand_cleaned, wesm_indicator from tmp_amd_spare_parts order by part_no")) ;
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
		if (sqlPrint != null) {
			sqlPrint.close() ;
		}
	}
    }

    private static void updateCounts() {
        System.out.println("amd_spare_parts_in=" + F1.getRecsIn()) ;
        logger.info("amd_spare_parts_in=" + F1.getRecsIn()) ;
        System.out.println("tmp_amd_spare_parts_in=" + F2.getRecsIn()) ;
        logger.info("tmp_amd_spare_parts_in=" + F2.getRecsIn()) ;
        System.out.println("rows inserted=" + rowsInserted) ;
        logger.info("rows inserted=" + rowsInserted) ;
        System.out.println("rows updated=" + rowsUpdated) ;
        logger.info("rows updated=" + rowsUpdated) ;
        System.out.println("rows deleted=" + rowsDeleted) ;
        logger.info("rows deleted=" + rowsDeleted) ;
        System.out.println("end time: " + now()) ;
    }

    class Key implements Comparable {
        String      part_no ;
        public boolean equals(Object o) {
            Key k = (Key) o ;
            return (k.part_no.equals(part_no) ) ;
        }
        public int hashCode() {
            return part_no.hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return part_no.compareTo(theKey.part_no) ;
        }
        public String toString() {
            return "part_no=" + part_no  ;
        }
    }
    Key key ;
    class Body {
        java.sql.Date date_icp ;
        String      mic ;
        String      mfgr ;
        String      acquisition_advice_code ;
        boolean     date_icp_isnull ;
        double      disposal_cost ;
        boolean     disposal_cost_isnull ;
        String      erc ;
        String      icp_ind ;
        String      nomenclature ;
        String      nsn ;
        double      order_lead_time ;
        boolean     order_lead_time_isnull ;
        String      order_uom ;
        String      prime_ind ;
        int         scrap_value ;
        boolean     scrap_value_isnull ;
        String      serial_flag ;
        int         shelf_life ;
        boolean     shelf_life_isnull ;
        double      unit_cost ;
        boolean     unit_cost_isnull ;
        BigDecimal  unit_volume ;
        boolean     unit_volume_isnull ;
        String		unit_of_issue ;
        // the following fields correspond to fields in
        // amd_national_stock_items
        String      item_type ;
        double      order_quantity ;
        boolean     order_quantity_isnull ;
        String      planner_code ;
        String      smr_code ;
        String		mmac ;
        // the following field is contained in amd_nsns
        String      nsn_type ;
	int	qpei_weighted ;
	boolean qpei_weighted_isnull ;
	double	mtbdr ;
	boolean mtbdr_isnull ;
	double mtbdr_computed ;
	boolean mtbdr_computed_isnull ;
	double	condemn_avg ;
	boolean	condemn_avg_isnull ;
	double	criticality ;
	boolean criticality_isnull ;
	double	nrts_avg ;
	boolean nrts_avg_isnull ;
	double	rts_avg ;
	boolean	rts_avg_isnull ;
	int time_to_repair_off_base ;
	boolean time_to_repair_off_base_isnull ;
	BigDecimal cost_to_repair_off_base ;
	boolean cost_to_repair_off_base_isnull ;
	double amc_demand ;
	boolean amc_demand_isnull ;
        String      wesm_indicator ;

	class CleanedData {
	  double condemn_avg_cleaned ;
	  boolean condemn_avg_cleaned_isnull ;
	  double criticality_cleaned ;
	  boolean criticality_cleaned_isnull ;
	  double mtbdr_cleaned ;
	  boolean mtbdr_cleaned_isnull ;
	  double nrts_avg_cleaned ;
	  boolean nrts_avg_cleaned_isnull ;
	  double cost_to_repair_off_base_cleand ;
	  boolean cost_to_repair_off_base_cleand_isnull ;
	  int time_to_repair_off_base_cleand ;
	  boolean time_to_repair_off_base_cleand_isnull ;
	  int order_lead_time_cleaned ;
	  boolean order_lead_time_cleaned_isnull ;
	  String planner_code_cleaned ;
	  boolean planner_code_cleaned_isnull ;
	  double rts_avg_cleaned ;
	  boolean rts_avg_cleaned_isnull ;
	  String smr_code_cleaned ;
	  boolean smr_code_cleaned_isnull ;
	  double unit_cost_cleaned ;
	  boolean unit_cost_cleaned_isnull ;
	  double amc_demand_cleaned ;
	  boolean amc_demand_cleaned_isnull ;

	  	CleanedData(ResultSet r) throws SQLException {		
        		condemn_avg_cleaned = r.getDouble("CONDEMN_AVG_CLEANED") ;
        		condemn_avg_cleaned_isnull = r.wasNull() ;
			criticality_cleaned = r.getDouble("CRITICALITY_CLEANED") ;
        		criticality_cleaned_isnull = r.wasNull() ;
			mtbdr_cleaned = r.getDouble("MTBDR_CLEANED") ;
        		mtbdr_cleaned_isnull = r.wasNull() ;
			nrts_avg_cleaned = r.getDouble("NRTS_AVG_CLEANED") ;
        		nrts_avg_cleaned_isnull = r.wasNull() ;
			cost_to_repair_off_base_cleand = r.getDouble("COST_TO_REPAIR_OFF_BASE_CLEAND") ;
        		cost_to_repair_off_base_cleand_isnull = r.wasNull() ;
			time_to_repair_off_base_cleand = r.getInt("TIME_TO_REPAIR_OFF_BASE_CLEAND") ;
        		time_to_repair_off_base_cleand_isnull = r.wasNull() ;
        		order_lead_time_cleaned = r.getInt("ORDER_LEAD_TIME_CLEANED") ;
        		order_lead_time_cleaned_isnull = r.wasNull() ;
        		planner_code_cleaned = r.getString("PLANNER_CODE_CLEANED") ;
        		planner_code_cleaned_isnull = r.wasNull() ;
			rts_avg_cleaned = r.getDouble("RTS_AVG_CLEANED") ;
			rts_avg_cleaned_isnull = r.wasNull() ;
        		smr_code_cleaned = r.getString("SMR_CODE_CLEANED") ;
        		smr_code_cleaned_isnull = r.wasNull() ;
			unit_cost_cleaned = r.getDouble("UNIT_COST_CLEANED") ;
        		unit_cost_cleaned_isnull = r.wasNull() ;
			amc_demand_cleaned = r.getDouble("AMC_DEMAND_CLEANED") ;
        		amc_demand_cleaned_isnull = r.wasNull() ;
		}

		public String toString() {
		  return 
			"condemn_avg_cleaned=" + condemn_avg_cleaned +
			" criticality_cleaned=" + criticality_cleaned +
			" mtbdr_cleaned=" + mtbdr_cleaned +
			" nrts_avg_cleaned=" + nrts_avg_cleaned +
		  	" cost_to_repair_off_base_cleand=" + cost_to_repair_off_base_cleand +
		  	" time_to_repair_off_base_cleand=" + time_to_repair_off_base_cleand +
		  	" order_lead_time_cleaned=" + order_lead_time_cleaned +
		  	" planner_code_cleaned=" + planner_code_cleaned +
		  	" rts_avg_cleaned=" + rts_avg_cleaned +
		  	" smr_code_cleaned=" + smr_code_cleaned +
		  	" unit_cost_cleaned=" + unit_cost_cleaned +
		  	" amc_demand_cleaned=" + amc_demand_cleaned ;

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
        	boolean equal(String s1, boolean null1, String s2, boolean null2) {
            		if (!null1)
               			if (!null2)
               		     		return s1.equals(s2) ; 
               		 	else
               		     		return false ;
            		else if (null2)
               		 	return true ; // both null
            		else
               		 	return false ; // s1 == null && s2 != null
        	}
		public boolean equals(Object o)  {
			CleanedData cd = (CleanedData) o ;
			boolean result = false;
			try {
				result = equal(cd.condemn_avg_cleaned, cd.condemn_avg_cleaned_isnull, condemn_avg_cleaned, condemn_avg_cleaned_isnull) ;
				showDiff(result, "condemn_avg_cleaned", cd.condemn_avg_cleaned + "", condemn_avg_cleaned + "") ;
				result = result && equal(cd.criticality_cleaned, cd.criticality_cleaned_isnull, criticality_cleaned, criticality_cleaned_isnull) ;
				showDiff(result, "criticality_cleaned", cd.criticality_cleaned + "", criticality_cleaned + "") ;
				result = result && equal(cd.mtbdr_cleaned, cd.mtbdr_cleaned_isnull, mtbdr_cleaned, mtbdr_cleaned_isnull) ;
				showDiff(result, "mtbdr_cleaned", cd.mtbdr_cleaned + "", mtbdr_cleaned + "") ;
				result = result && equal(cd.nrts_avg_cleaned, cd.nrts_avg_cleaned_isnull, nrts_avg_cleaned, nrts_avg_cleaned_isnull) ; 
				showDiff(result, "nrts_avg_cleaned", cd.nrts_avg_cleaned + "", nrts_avg_cleaned + "") ;
				result = result && equal(cd.cost_to_repair_off_base_cleand, cd.cost_to_repair_off_base_cleand_isnull,  cost_to_repair_off_base_cleand, cost_to_repair_off_base_cleand_isnull) ; 
				showDiff(result, "cost_to_repair_off_base_cleand", cd.cost_to_repair_off_base_cleand + "", cost_to_repair_off_base_cleand + "") ;
				result = result && equal(cd.time_to_repair_off_base_cleand, cd.time_to_repair_off_base_cleand_isnull, time_to_repair_off_base_cleand, time_to_repair_off_base_cleand_isnull) ;
				showDiff(result, "time_to_repair_off_base_cleand", cd.time_to_repair_off_base_cleand + "", time_to_repair_off_base_cleand + "") ;
				result = result && equal(cd.order_lead_time_cleaned, cd.order_lead_time_cleaned_isnull, order_lead_time_cleaned, order_lead_time_cleaned_isnull); 
				showDiff(result, "order_lead_time_cleaned", cd.order_lead_time_cleaned + "", order_lead_time_cleaned + "") ;
				result = result && equal(cd.planner_code_cleaned, cd.planner_code_cleaned_isnull,planner_code_cleaned, planner_code_cleaned_isnull) ; 
				showDiff(result, "planner_code_cleaned", cd.planner_code_cleaned + "", planner_code_cleaned + "") ;
				result = result && equal(cd.rts_avg_cleaned, cd.rts_avg_cleaned_isnull, rts_avg_cleaned, rts_avg_cleaned_isnull) ;
				result = result && equal(cd.smr_code_cleaned, cd.smr_code_cleaned_isnull, smr_code_cleaned, smr_code_cleaned_isnull) ;
				showDiff(result, "smr_code_cleaned", cd.smr_code_cleaned + "", smr_code_cleaned + "") ;
				result = result && equal(cd.unit_cost_cleaned, cd.unit_cost_cleaned_isnull, unit_cost_cleaned, unit_cost_cleaned_isnull) ; 
				showDiff(result, "unit_cost_cleaned", cd.unit_cost_cleaned + "", unit_cost_cleaned + "") ;
				result = result && equal(cd.amc_demand_cleaned, cd.amc_demand_cleaned_isnull, amc_demand_cleaned, amc_demand_cleaned_isnull) ; 
				showDiff(result, "amc_demand_cleaned", cd.amc_demand_cleaned + "", amc_demand_cleaned + "") ;
			}  catch (RecsNotEqualException e) {
		    	return false ;
	    }
			return result ;
		}
	}

	CleanedData cd ;

        public String toString() {
            return "mfgr=" + mfgr + " " +
                    "mic=" + mic + " " +
                    "acquisition_advice_code=" + acquisition_advice_code + " " +
                    "date_icp=" + date_icp + " " +
                    "erc=" +erc + " " +
                    "icp_ind=" + icp_ind + " " +
                    "nomenclature=" +nomenclature + " " +
                    "nsn=" + nsn + " " +
                    "order_lead_time=" + order_lead_time + " " +
                    "order_uom=" + order_uom + " " +
                    "prime_ind=" + prime_ind + " " +
                    "scrap_value=" + scrap_value + " " +
                    "serial_flag=" + serial_flag + " " +
                    "shelf_life=" + shelf_life + " " +
                    "disposal_code=" + disposal_cost + " " +
                    "unit_code=" + unit_cost + " " +
                    "unit_volume=" + unit_volume + " " +
                    "smr_code=" + smr_code + " " +
                    "item_type=" + item_type + " " +
                    "order_quantity=" + order_quantity + " " +
                    "planner_code=" + planner_code + " " +
                    "mmac=" + mmac + " " +
                    "unit_of_issue=" + unit_of_issue + " " +
                    "nsn_type=" + nsn_type + " " +
	    	    "qpei_weighted=" + qpei_weighted + " " +
		    "mtbdr=" + mtbdr + " " +
		    "mtbdr_computed=" + mtbdr_computed + " " +
		    "condemn_avg=" + condemn_avg + " " +
		    "criticality=" + criticality + " " +
		    "nrts_avg=" + nrts_avg + " " +
		    "rts_avg=" + rts_avg  + " " +
		    "cd=" + cd + " " +
		    "cost_to_repair_off_base=" + cost_to_repair_off_base + " " +
		    "time_to_repair_off_base=" + time_to_repair_off_base + " " +
		    "amc_demand=" + amc_demand + " " +
		    "wesm_indicator=" + wesm_indicator ;
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
        private void showDiff(boolean result, String fieldName, String field1, String field2)  throws RecsNotEqualException {
                if (!result) {
		    showDiffCnt++ ;
		    if (showDiffCnt % showDiffThreshold == 0) {
                    	logger.debug("key = " + key + " field: " + fieldName + " *" + field1 + "* *" + field2 + "*") ;
		    }
		    throw new RecsNotEqualException("Records are not equal") ;
                }
        }

        public boolean equals(Object o) {
            Body b = (Body) o ;
            boolean result ;
	    try {
		    result = equal(b.mfgr, mfgr) ;
		    showDiff(result, "mfgr", b.mfgr + "", mfgr + "") ;
		    result = result && equal(b.date_icp, b.date_icp_isnull, date_icp, date_icp_isnull) ;
		    showDiff(result, "date_icp", b.date_icp + "", date_icp + "") ;
		    result = result && equal(b.disposal_cost, b.disposal_cost_isnull, disposal_cost, disposal_cost_isnull) ;
		    showDiff(result, "disposal_cost", b.disposal_cost + "", disposal_cost + "") ;
		    result = result && equal(b.erc, erc) ;
		    showDiff(result, "erc", b.erc + "", erc + "") ;
		    result = result && equal(b.icp_ind, icp_ind) ;
		    showDiff(result, "icp_ind", b.icp_ind + "", icp_ind + "") ;
		    result = result && equal(b.nomenclature, nomenclature) ;
		    showDiff(result, "nomenclature", b.nomenclature + "", nomenclature + "") ;
		    result = result && equal(b.order_lead_time, b.order_lead_time_isnull, order_lead_time, order_lead_time_isnull);
		    showDiff(result, "order_lead_time", b.order_lead_time + "", order_lead_time + "") ;
		    result = result && equal(b.order_uom, order_uom) ;
		    showDiff(result, "order_uom", b.order_uom + "", order_uom + "") ;
		    result = result && equal(b.scrap_value, b.scrap_value_isnull, scrap_value, scrap_value_isnull) ;
		    showDiff(result, "scrap_value", b.scrap_value + "", scrap_value + "") ;
		    result = result && equal(b.serial_flag, serial_flag) ;
		    showDiff(result, "serial_flag", b.serial_flag + "", serial_flag + "") ;
		    result = result && equal(b.shelf_life, b.shelf_life_isnull, shelf_life, shelf_life_isnull) ;
		    showDiff(result, "shelf_life", b.shelf_life + "", shelf_life + "") ;
		    result = result && equal(b.unit_cost, b.unit_cost_isnull, unit_cost, unit_cost_isnull) ;
		    showDiff(result, "unit_cost", b.unit_cost + "", unit_cost + "") ;
		    result = result && equal(b.unit_volume, b.unit_volume_isnull, unit_volume, unit_volume_isnull) ;
		    showDiff(result, "unit_volume", b.unit_volume + "", unit_volume + "") ;
		    result = result && equal(b.prime_ind, prime_ind) ;
		    showDiff(result, "prime_ind", b.prime_ind + "", prime_ind + "") ;
		    result = result && equal(b.acquisition_advice_code, acquisition_advice_code) ;
		    showDiff(result, "acquisition_advice_code", b.acquisition_advice_code + "", acquisition_advice_code + "") ;
		    result = result && equal(b.unit_of_issue, unit_of_issue) ;
		    showDiff(result, "unit_of_issue", b.unit_of_issue + "", unit_of_issue + "") ;
		    result = result && equal(b.nsn, nsn) ;
		    showDiff(result, "nsn", b.nsn + "", nsn + "") ;
		    if (result && prime_ind.equals(PRIME_PART)) {
			/* Make sure all fields are equal (result=true) and
			    check the following fields  for prime parts,
			    since they only exist for prime parts
			    */
			result = result && equal(b.mmac, mmac) ;
			showDiff(result, "mmac", b.mmac + "", mmac + "") ;
			result = result && equal(b.qpei_weighted, b.qpei_weighted_isnull, qpei_weighted, qpei_weighted_isnull) ;
			showDiff(result, "qpei_weighted", b.qpei_weighted + "", qpei_weighted + "") ;
			result = result && equal(b.mtbdr, b.mtbdr_isnull, mtbdr, mtbdr_isnull) ;
			showDiff(result, "mtbdr", b.mtbdr + "", mtbdr + "") ;
			result = result && equal(b.mtbdr_computed, b.mtbdr_computed_isnull, mtbdr_computed, mtbdr_computed_isnull) ;
			showDiff(result, "mtbdr_computed", b.mtbdr_computed + "", mtbdr_computed + "") ;
			result = result && equal(b.condemn_avg, b.condemn_avg_isnull, condemn_avg, condemn_avg_isnull) ;
			showDiff(result, "condemn_avg", b.condemn_avg + "", condemn_avg + "") ;
			result = result && equal(b.criticality, b.criticality_isnull, criticality, criticality_isnull) ;
			showDiff(result, "criticality", b.criticality + "", criticality + "") ;
			result = result && equal(b.nrts_avg, b.nrts_avg_isnull, nrts_avg, nrts_avg_isnull) ;
			showDiff(result, "nrts_avg", b.nrts_avg + "", nrts_avg + "") ;
			result = result && equal(b.rts_avg, b.rts_avg_isnull, rts_avg, rts_avg_isnull) ;
			showDiff(result, "rts_avg", b.rts_avg + "", rts_avg + "") ;
			result = result && equal(b.mic, mic) ;
			showDiff(result, "mic", b.mic + "", mic + "") ;
			result = result && equal(b.item_type, item_type) ;
			showDiff(result, "item_type", b.item_type + "", item_type + "") ;
			result = result && equal(b.order_quantity, b.order_quantity_isnull, order_quantity, order_quantity_isnull) ;
			showDiff(result, "order_quantity", b.order_quantity + "", order_quantity + "") ;
			result = result && equal(b.planner_code, planner_code) ;
			showDiff(result, "planner_code", b.planner_code + "", planner_code + "") ;
			result = result && equal(b.smr_code, smr_code) ;
			showDiff(result, "smr_code", b.smr_code + "", smr_code + "") ;
			result = result && equal(b.nsn_type, nsn_type) ;
			showDiff(result, "nsn_type", b.nsn_type + "", nsn_type + "") ;
			result = result && equal(b.cost_to_repair_off_base, b.cost_to_repair_off_base_isnull, cost_to_repair_off_base, cost_to_repair_off_base_isnull) ;
			showDiff(result, "cost_to_repair_off_base", b.cost_to_repair_off_base + "", cost_to_repair_off_base + "") ;
			result = result && equal(b.time_to_repair_off_base, b.time_to_repair_off_base_isnull, time_to_repair_off_base, time_to_repair_off_base_isnull) ;
			showDiff(result, "time_to_repair_off_base", b.time_to_repair_off_base + "", time_to_repair_off_base + "") ;
			result = result && equal(b.amc_demand, b.amc_demand_isnull, amc_demand, amc_demand_isnull) ;
			showDiff(result, "amc_demand", b.amc_demand + "", amc_demand + "") ;
			result = result && equal(b.wesm_indicator, wesm_indicator) ;
			showDiff(result, "wesm_indicator", b.wesm_indicator + "", wesm_indicator + "") ;
		    }
		    result = result && cd.equals(b.cd) ;
	    }  catch (RecsNotEqualException e) {
		    	return false ;
	    }
            return result ;
        }
    }
    Body body ;

    SparePart(ResultSet r) throws SQLException {
        try {
        key = new Key() ;
        key.part_no = r.getString("PART_NO") ;

        body = new Body() ;
	body.cd = body.new CleanedData(r) ;
        body.mfgr = r.getString("MFGR") ;
        body.mic = r.getString("MIC_CODE_LOWEST") ;
        body.acquisition_advice_code = r.getString("ACQUISITION_ADVICE_CODE") ;
        body.mmac = r.getString("MMAC") ;
        body.unit_of_issue = r.getString("UNIT_OF_ISSUE") ;
        body.date_icp = r.getDate("DATE_ICP") ;
        body.date_icp_isnull = r.wasNull() ;
        body.disposal_cost = r.getDouble("DISPOSAL_COST") ;
        body.disposal_cost_isnull =   r.wasNull() ;
        body.erc = r.getString("ERC") ;
        body.icp_ind = r.getString("ICP_IND") ;
        body.nomenclature = r.getString("NOMENCLATURE") ;
        body.order_lead_time = r.getDouble("ORDER_LEAD_TIME") ;
        body.order_lead_time_isnull = r.wasNull() ;
        body.order_uom = r.getString("ORDER_UOM") ;
        body.prime_ind = r.getString("PRIME_IND") ;
        body.scrap_value = r.getInt("SCRAP_VALUE") ;
        body.scrap_value_isnull = r.wasNull() ;
        body.serial_flag = r.getString("SERIAL_FLAG") ;
        body.shelf_life = r.getInt("SHELF_LIFE") ;
        body.shelf_life_isnull = r.wasNull() ;
        body.unit_cost = r.getDouble("UNIT_COST") ;
        body.unit_cost_isnull = r.wasNull() ;
        body.unit_volume = r.getBigDecimal("UNIT_VOLUME");
        body.unit_volume_isnull = r.wasNull() ;
        body.nsn = r.getString("NSN") ;
        // NOTE: in the DB SMR_CODE, ITEM_TYPE, and
        // PLANNER_CODE belong to
        // a different table than the preceding columns, but
        // for the Snapshot Differential Algorithm, they need
        // to be considered as part of the SparePart record
        body.item_type = r.getString("ITEM_TYPE") ;
        body.order_quantity = r.getDouble("ORDER_QUANTITY") ;
        body.order_quantity_isnull = r.wasNull() ;
        body.planner_code = r.getString("PLANNER_CODE") ;
        body.smr_code = r.getString("SMR_CODE") ;
        body.nsn_type = r.getString("NSN_TYPE") ;
	body.qpei_weighted = r.getInt("QPEI_WEIGHTED") ;
	body.qpei_weighted_isnull = r.wasNull() ;
	body.mtbdr = r.getDouble("MTBDR") ;
	body.mtbdr_isnull = r.wasNull() ;
	body.mtbdr_computed = r.getDouble("MTBDR_COMPUTED") ;
	body.mtbdr_computed_isnull = r.wasNull() ;
	body.condemn_avg = r.getDouble("CONDEMN_AVG") ;
	body.condemn_avg_isnull = r.wasNull() ;
	body.criticality = r.getDouble("CRITICALITY") ;
	body.criticality_isnull = r.wasNull() ;
	body.nrts_avg = r.getDouble("NRTS_AVG") ;
	body.nrts_avg_isnull = r.wasNull() ;
	body.rts_avg = r.getDouble("RTS_AVG") ;
	body.rts_avg_isnull = r.wasNull() ;
	body.cost_to_repair_off_base = r.getBigDecimal("COST_TO_REPAIR_OFF_BASE") ;
        body.cost_to_repair_off_base_isnull = r.wasNull() ;
	body.time_to_repair_off_base = r.getInt("TIME_TO_REPAIR_OFF_BASE") ;
        body.time_to_repair_off_base_isnull = r.wasNull() ;
	body.amc_demand = r.getInt("AMC_DEMAND") ;
        body.amc_demand_isnull = r.wasNull() ;
	body.wesm_indicator = r.getString("WESM_INDICATOR") ;
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

    public void     insert() {
	if (! no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_spare_parts_pkg.InsertRow("
			+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
			+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
			+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}") ;
			if (debug) {
				sqlPrint.println("exec :rc := amd_spare_parts_pkg.insertRow('" 
					+ key.part_no + "','"
					+ body.mfgr + "',"
					+ ((body.date_icp == null) ? "null" 
						: "to_date('" + convertDate(body.date_icp) + "','MM/DD/YYYY'),") + ",\n"
					+ (body.disposal_cost_isnull ? "null" : body.disposal_cost + "") + "," 
					+ ((body.erc == null) ? "null" :  "'" + body.erc + "'") + ",\n"
					+ ((body.icp_ind == null) ?  "null" : "'" + body.icp_ind + "'") + ",'"
					+ body.nomenclature + "',"
					+ (body.order_lead_time_isnull ? "null" : body.order_lead_time + "") + ",\n" 
					+ (body.order_quantity_isnull ? "null" : body.order_quantity + "") + ",'" 
					+ body.order_uom + "','"
					+ body.prime_ind + "',\n"
					+ (body.scrap_value_isnull ? "null" : body.scrap_value + "") + ",'" 
					+ body.serial_flag + "',"
					+ (body.shelf_life_isnull ? "null" : body.shelf_life + "") + ",\n" 
					+ (body.unit_cost_isnull ? "null" : body.unit_cost + "") + "," 
					+ (body.unit_volume_isnull ? "null" : body.unit_volume + "") + ","
					+ ((body.nsn == null) ? "null" : "'" + body.nsn + "'")	+ ",\n"
					+ ((body.nsn_type == null) ? "null" : "'" + body.nsn_type + "'") + "',"
					+ ((body.item_type == null) ? "null" : "'" + body.item_type + ",") + ","
					+ ((body.smr_code == null) ? "null" : "'" + body.smr_code + "'") + ",\n"
					+ ((body.planner_code == null) ? "null" : "'" + body.planner_code + "'") + ","
					+ ((body.mic == null) ? "null" : "'" + body.mic + "'") + ","
					+ ((body.acquisition_advice_code == null) ? "null" 
							: "'" + body.acquisition_advice_code + "'") + ",\n"
					+ ((body.mmac == null) ? "null" : "'" + body.mmac + "'") + ","
					+ ((body.unit_of_issue == null) ? "null" : "'" + body.unit_of_issue + "'") + ","
					+ (body.mtbdr_isnull ? "null" : body.mtbdr + "") + ",\n"
					+ (body.mtbdr_computed_isnull ? "null" : body.mtbdr_computed + "") + ","
					+ (body.qpei_weighted_isnull ? "null" : body.qpei_weighted + "") + ","
					+ (body.condemn_avg_isnull ? "null" : body.condemn_avg + "") + ",\n"
					+ (body.cd.condemn_avg_cleaned_isnull ? "null" : body.cd.condemn_avg_cleaned + "") + ","
					+ (body.cd.criticality_cleaned_isnull ? "null" : body.cd.criticality_cleaned + "") + ","
					+ (body.cd.mtbdr_cleaned_isnull ? "null" : body.cd.mtbdr_cleaned + "") + ",\n"
					+ (body.cd.nrts_avg_cleaned_isnull ? "null" : body.cd.nrts_avg_cleaned + "") + ","
					+ (body.cd.cost_to_repair_off_base_cleand_isnull ? "null" : body.cd.cost_to_repair_off_base_cleand + "") + ","
					+ (body.cd.time_to_repair_off_base_cleand_isnull ? "null" : body.cd.time_to_repair_off_base_cleand + "") + ",\n"
					+ (body.cd.order_lead_time_cleaned_isnull ? "null" : body.cd.order_lead_time_cleaned + "") + ","
					+ ((body.cd.planner_code_cleaned == null) ? "null" 
							: "'" + body.cd.planner_code_cleaned + "'") + ","
					+ (body.cd.rts_avg_cleaned_isnull ? "null" : body.cd.rts_avg_cleaned + "") + ",\n"
					+ ((body.cd.smr_code_cleaned == null) ? "null" 
							: "'" + body.cd.smr_code_cleaned + "'") + ","
					+ (body.cd.unit_cost_cleaned_isnull ? "null" : body.cd.unit_cost_cleaned + "") + ","
					+ (body.cd.condemn_avg_cleaned_isnull ? "null" : body.cd.condemn_avg_cleaned + "") + ",\n"
					+ (body.criticality_isnull ? "null" : body.criticality + "") + ","
					+ (body.nrts_avg_isnull ? "null" : body.nrts_avg + "") + ","
					+ (body.rts_avg_isnull ? "null" : body.rts_avg + "") + ",\n"
					+ (body.cost_to_repair_off_base_isnull ? "null" : body.cost_to_repair_off_base + "") + ","
					+ (body.time_to_repair_off_base_isnull ? "null" : body.time_to_repair_off_base + "") + ","
					+ (body.amc_demand_isnull ? "null" : body.amc_demand + "") + ",\n"
					+ (body.cd.amc_demand_cleaned_isnull ? "null" : body.cd.amc_demand_cleaned + "") + ","
					+ ((body.wesm_indicator == null) ? "null" : "'" + body.wesm_indicator + "'") + ");" ) ;
			}
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.part_no) ;
		    cstmt.setString(3, body.mfgr) ;
		    cstmt.setDate(4, body.date_icp) ;
		    setDouble(cstmt, 5, body.disposal_cost, body.disposal_cost_isnull) ;
		    cstmt.setString(6, body.erc) ;
		    cstmt.setString(7, body.icp_ind) ;
		    cstmt.setString(8, body.nomenclature) ;
		    setDouble(cstmt, 9, body.order_lead_time, body.order_lead_time_isnull) ;
		    setDouble(cstmt, 10, body.order_quantity, body.order_quantity_isnull) ;
		    cstmt.setString(11, body.order_uom) ;
		    cstmt.setString(12, body.prime_ind) ;
		    setInt(cstmt, 13, body.scrap_value, body.scrap_value_isnull) ;
		    cstmt.setString(14, body.serial_flag) ;
		    setInt(cstmt, 15, body.shelf_life, body.shelf_life_isnull) ;
		    setDouble(cstmt, 16, body.unit_cost, body.unit_cost_isnull) ;
		    setBigDecimal(cstmt, 17, body.unit_volume, body.unit_volume_isnull) ;
		    cstmt.setString(18, body.nsn) ;
		    cstmt.setString(19, body.nsn_type) ;
		    cstmt.setString(20, body.item_type) ;
		    cstmt.setString(21, body.smr_code) ;
		    cstmt.setString(22, body.planner_code) ;
		    cstmt.setString(23, body.mic) ;
		    cstmt.setString(24, body.acquisition_advice_code) ;
		    cstmt.setString(25, body.mmac) ;
		    cstmt.setString(26, body.unit_of_issue) ;
		    setDouble(cstmt, 27, body.mtbdr, body.mtbdr_isnull) ;
		    setDouble(cstmt, 28, body.mtbdr_computed, body.mtbdr_computed_isnull) ;
		    setInt(cstmt, 29, body.qpei_weighted, body.qpei_weighted_isnull) ;
		    setDouble(cstmt, 30, body.cd.condemn_avg_cleaned, body.cd.condemn_avg_cleaned_isnull) ;
		    setDouble(cstmt, 31, body.cd.criticality_cleaned, body.cd.criticality_cleaned_isnull) ;
		    setDouble(cstmt, 32, body.cd.mtbdr_cleaned, body.cd.mtbdr_cleaned_isnull) ;
		    setDouble(cstmt, 33, body.cd.nrts_avg_cleaned, body.cd.nrts_avg_cleaned_isnull) ;
		    setDouble(cstmt, 34, body.cd.cost_to_repair_off_base_cleand, body.cd.cost_to_repair_off_base_cleand_isnull) ;
		    setInt(cstmt, 35, body.cd.time_to_repair_off_base_cleand, body.cd.time_to_repair_off_base_cleand_isnull) ;
		    setInt(cstmt, 36, body.cd.order_lead_time_cleaned, body.cd.order_lead_time_cleaned_isnull) ;
		    cstmt.setString(37, body.cd.planner_code_cleaned) ;
		    setDouble(cstmt, 38, body.cd.rts_avg_cleaned, body.cd.rts_avg_cleaned_isnull) ;
		    cstmt.setString(39, body.cd.smr_code_cleaned) ;
		    setDouble(cstmt, 40, body.cd.unit_cost_cleaned, body.cd.unit_cost_cleaned_isnull) ;
		    setDouble(cstmt, 41, body.condemn_avg, body.condemn_avg_isnull) ;
		    setDouble(cstmt, 42, body.criticality, body.criticality_isnull) ;
		    setDouble(cstmt, 43, body.nrts_avg, body.nrts_avg_isnull) ;
		    setDouble(cstmt, 44, body.rts_avg, body.rts_avg_isnull) ;
		    setBigDecimal(cstmt, 45, body.cost_to_repair_off_base, body.cost_to_repair_off_base_isnull) ;
		    setInt(cstmt, 46, body.time_to_repair_off_base, body.time_to_repair_off_base_isnull) ;
		    setDouble(cstmt, 47, body.amc_demand, body.amc_demand_isnull) ;
		    setDouble(cstmt, 48, body.cd.amc_demand_cleaned, body.cd.amc_demand_cleaned_isnull) ;
		    cstmt.setString( 49, body.wesm_indicator) ;
		    logger.info("Insert: key=" + key + " nsn=" + body.nsn) ;
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_spare_parts_pkg.InsertRow failed with result = " + result) ;
			logger.fatal("amd_spare_parts_pkg.InsertRow failed with result = " + result) ;
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
			"{? = call amd_spare_parts_pkg.UpdateRow("
			+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
			+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
			+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
			+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}") ;
			if (debug) {
				sqlPrint.println("exec :rc := amd_spare_parts_pkg.UpdateRow('" 
					+ key.part_no + "','"
					+ body.mfgr + "',"
					+ ((body.date_icp == null) ? "null" 
						: "to_date('" + convertDate(body.date_icp) + "','MM/DD/YYYY'),") + ",\n"
					+ (body.disposal_cost_isnull ? "null" : body.disposal_cost + "") + "," 
					+ ((body.erc == null) ? "null" :  "'" + body.erc + "'") + ",\n"
					+ ((body.icp_ind == null) ?  "null" : "'" + body.icp_ind + "'") + ",'"
					+ body.nomenclature + "',"
					+ (body.order_lead_time_isnull ? "null" : body.order_lead_time + "") + ",\n" 
					+ (body.order_quantity_isnull ? "null" : body.order_quantity + "") + ",'" 
					+ body.order_uom + "','"
					+ body.prime_ind + "',\n"
					+ (body.scrap_value_isnull ? "null" : body.scrap_value + "") + ",'" 
					+ body.serial_flag + "',"
					+ (body.shelf_life_isnull ? "null" : body.shelf_life + "") + ",\n" 
					+ (body.unit_cost_isnull ? "null" : body.unit_cost + "") + "," 
					+ (body.unit_volume_isnull ? "null" : body.unit_volume + "") + ","
					+ ((body.nsn == null) ? "null" : "'" + body.nsn + "'")	+ ",\n"
					+ ((body.nsn_type == null) ? "null" : "'" + body.nsn_type + "'") + "',"
					+ ((body.item_type == null) ? "null" : "'" + body.item_type + ",") + ","
					+ ((body.smr_code == null) ? "null" : "'" + body.smr_code + "'") + ",\n"
					+ ((body.planner_code == null) ? "null" : "'" + body.planner_code + "'") + ","
					+ ((body.mic == null) ? "null" : "'" + body.mic + "'") + ","
					+ ((body.acquisition_advice_code == null) ? "null" 
							: "'" + body.acquisition_advice_code + "'") + ",\n"
					+ ((body.mmac == null) ? "null" : "'" + body.mmac + "'") + ","
					+ ((body.unit_of_issue == null) ? "null" : "'" + body.unit_of_issue + "'") + ","
					+ (body.mtbdr_isnull ? "null" : body.mtbdr + "") + ",\n"
					+ (body.mtbdr_computed_isnull ? "null" : body.mtbdr_computed + "") + ","
					+ (body.qpei_weighted_isnull ? "null" : body.qpei_weighted + "") + ","
					+ (body.condemn_avg_isnull ? "null" : body.condemn_avg + "") + ",\n"
					+ (body.cd.condemn_avg_cleaned_isnull ? "null" : body.cd.condemn_avg_cleaned + "") + ","
					+ (body.cd.criticality_cleaned_isnull ? "null" : body.cd.criticality_cleaned + "") + ","
					+ (body.cd.mtbdr_cleaned_isnull ? "null" : body.cd.mtbdr_cleaned + "") + ",\n"
					+ (body.cd.nrts_avg_cleaned_isnull ? "null" : body.cd.nrts_avg_cleaned + "") + ","
					+ (body.cd.cost_to_repair_off_base_cleand_isnull ? "null" : body.cd.cost_to_repair_off_base_cleand + "") + ","
					+ (body.cd.time_to_repair_off_base_cleand_isnull ? "null" : body.cd.time_to_repair_off_base_cleand + "") + ",\n"
					+ (body.cd.order_lead_time_cleaned_isnull ? "null" : body.cd.order_lead_time_cleaned + "") + ","
					+ ((body.cd.planner_code_cleaned == null) ? "null" 
							: "'" + body.cd.planner_code_cleaned + "'") + ","
					+ (body.cd.rts_avg_cleaned_isnull ? "null" : body.cd.rts_avg_cleaned + "") + ",\n"
					+ ((body.cd.smr_code_cleaned == null) ? "null" 
							: "'" + body.cd.smr_code_cleaned + "'") + ","
					+ (body.cd.unit_cost_cleaned_isnull ? "null" : body.cd.unit_cost_cleaned + "") + ","
					+ (body.cd.condemn_avg_cleaned_isnull ? "null" : body.cd.condemn_avg_cleaned + "") + ",\n"
					+ (body.criticality_isnull ? "null" : body.criticality + "") + ","
					+ (body.nrts_avg_isnull ? "null" : body.nrts_avg + "") + ","
					+ (body.rts_avg_isnull ? "null" : body.rts_avg + "") + ",\n"
					+ (body.cost_to_repair_off_base_isnull ? "null" : body.cost_to_repair_off_base + "") + ","
					+ (body.time_to_repair_off_base_isnull ? "null" : body.time_to_repair_off_base + "") + ","
					+ (body.amc_demand_isnull ? "null" : body.amc_demand + "") + ",\n"
					+ (body.cd.amc_demand_cleaned_isnull ? "null" : body.cd.amc_demand_cleaned + "") + ","
					+ ((body.wesm_indicator == null) ? "null" : "'" + body.wesm_indicator + "'") + ");" ) ;
			}
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.part_no) ;
		    cstmt.setString(3, body.mfgr) ;
		    cstmt.setDate(4, body.date_icp) ;
		    setDouble(cstmt, 5, body.disposal_cost, body.disposal_cost_isnull) ;
		    cstmt.setString(6, body.erc) ;
		    cstmt.setString(7, body.icp_ind) ;
		    cstmt.setString(8, body.nomenclature) ;
		    setDouble(cstmt, 9, body.order_lead_time, body.order_lead_time_isnull) ;
		    setDouble(cstmt, 10, body.order_quantity, body.order_quantity_isnull) ;
		    cstmt.setString(11, body.order_uom) ;
		    cstmt.setString(12, body.prime_ind) ;
		    setInt(cstmt, 13, body.scrap_value, body.scrap_value_isnull) ;
		    cstmt.setString(14, body.serial_flag) ;
		    setInt(cstmt, 15, body.shelf_life, body.shelf_life_isnull) ;
		    setDouble(cstmt, 16, body.unit_cost, body.unit_cost_isnull) ;
		    setBigDecimal(cstmt, 17, body.unit_volume, body.unit_volume_isnull) ;
		    cstmt.setString(18, body.nsn) ;
		    cstmt.setString(19, body.nsn_type) ;
		    cstmt.setString(20, body.item_type) ;
		    cstmt.setString(21, body.smr_code) ;
		    cstmt.setString(22, body.planner_code) ;
		    cstmt.setString(23, body.mic) ;
		    cstmt.setString(24, body.acquisition_advice_code) ;
		    cstmt.setString(25, body.mmac) ;
		    cstmt.setString(26, body.unit_of_issue) ;
		    setDouble(cstmt, 27, body.mtbdr, body.mtbdr_isnull) ;
		    setDouble(cstmt, 28, body.mtbdr_computed, body.mtbdr_computed_isnull) ;
		    setInt(cstmt, 29, body.qpei_weighted, body.qpei_weighted_isnull) ;
		    setDouble(cstmt, 30, body.cd.condemn_avg_cleaned, body.cd.condemn_avg_cleaned_isnull) ;
		    setDouble(cstmt, 31, body.cd.criticality_cleaned, body.cd.criticality_cleaned_isnull) ;
		    setDouble(cstmt, 32, body.cd.mtbdr_cleaned, body.cd.mtbdr_cleaned_isnull) ;
		    setDouble(cstmt, 33, body.cd.nrts_avg_cleaned, body.cd.nrts_avg_cleaned_isnull) ;
		    setDouble(cstmt, 34, body.cd.cost_to_repair_off_base_cleand, body.cd.cost_to_repair_off_base_cleand_isnull) ;
		    setInt(cstmt, 35, body.cd.time_to_repair_off_base_cleand, body.cd.time_to_repair_off_base_cleand_isnull) ;
		    setInt(cstmt, 36, body.cd.order_lead_time_cleaned, body.cd.order_lead_time_cleaned_isnull) ;
		    cstmt.setString(37, body.cd.planner_code_cleaned) ;
		    setDouble(cstmt, 38, body.cd.rts_avg_cleaned, body.cd.rts_avg_cleaned_isnull) ;
		    cstmt.setString(39, body.cd.smr_code_cleaned) ;
		    setDouble(cstmt, 40, body.cd.unit_cost_cleaned, body.cd.unit_cost_cleaned_isnull) ;
		    setDouble(cstmt, 41, body.condemn_avg, body.condemn_avg_isnull) ;
		    setDouble(cstmt, 42, body.criticality, body.criticality_isnull) ;
		    setDouble(cstmt, 43, body.nrts_avg, body.nrts_avg_isnull) ;
		    setDouble(cstmt, 44, body.rts_avg, body.rts_avg_isnull) ;
		    setBigDecimal(cstmt, 45, body.cost_to_repair_off_base, body.cost_to_repair_off_base_isnull) ;
		    setInt(cstmt, 46, body.time_to_repair_off_base, body.time_to_repair_off_base_isnull) ;
		    setDouble(cstmt, 47, body.amc_demand, body.amc_demand_isnull) ;
		    setDouble(cstmt, 48, body.cd.amc_demand_cleaned, body.cd.amc_demand_cleaned_isnull) ;
		    cstmt.setString(49, body.wesm_indicator) ;
		    logger.info("cd=" + body.cd) ;
		    logger.info("Update: key=" + key + " nsn=" + body.nsn) ;
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_spare_parts_pkg.UpdateRow failed with result = " + result) ;
			logger.fatal("amd_spare_parts_pkg.UpdateRow failed with result = " + result) ;
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
			"{? = call amd_spare_parts_pkg.DeleteRow(?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.part_no) ;
		    cstmt.setString(3, body.nomenclature) ;
		    cstmt.setString(4, body.mfgr) ;
		    logger.info("Delete: key=" + key + " nsn=" + body.nsn) ;
		    cstmt.execute() ;
		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.out.println("amd_spare_parts_pkg.DeleteRow failed with result = " + result) ;
			logger.fatal("amd_spare_parts_pkg.DeleteRow failed with result = " + result) ;
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

    static public void     setDebug() {
	if (! no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{call amd_spare_parts_pkg.setDebug(?)}") ;
		    cstmt.setString(1, "Y") ;
		    cstmt.execute() ;
		    cstmt.close() ;
		}
		catch (java.sql.SQLException e) {
		    updateCounts() ;
		    System.out.println(e.getMessage()) ;
		    logger.fatal(e.getMessage()) ;
		    System.exit(4) ;
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
}
