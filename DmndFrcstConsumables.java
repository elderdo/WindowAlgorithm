import java.sql.* ;
import java.math.BigDecimal ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

/*   $Author:   zf297a  $
   $Revision:   1.5  $
       $Date:   31 Jan 2008 12:19:06  $
   $Workfile:   DmndFrcstConsumables.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\DmndFrcstConsumables.java.-arc  $
/*
/*   Rev 1.5   31 Jan 2008 12:19:06   zf297a
/*Added method loadParams
/*Use properties file to get parameters
/*Use DBConnection to get the connection information
/*Changed showDiff to use a threshold value
/*Add no_op to insert, update, & delete for testing purposes
/*
/*   Rev 1.4   01 Aug 2007 13:19:00   zf297a
/*Added duplicate column to diff.
/*
/*   Rev 1.3   18 Jul 2007 11:56:42   zf297a
/*added check for null demand forecast
/*
/*   Rev 1.2   11 Jul 2007 15:09:42   zf297a
/*Added column alias demand_forecast for the round function of the F1 and F2 queries.
/*
/*   Rev 1.1   11 Jul 2007 14:59:50   zf297a
/*Updated queries for F1 and F2.  rounded demand_forecast to 4 decimals.
/*
/*   Rev 1.0   23 May 2007 00:15:10   zf297a
/*Initial revision.
        */

public class DmndFrcstConsumables implements Rec {
    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug ;
    static boolean no_op    = false;
    static int bufSize      = 150000 ;
    static int ageBufSize   = 150000 ;
    static int prefetchSize = 5000 ;
    static int debugThreshold = 50000 ;
    static int showDiffThreshold = 5000 ;


    static Logger logger = Logger.getLogger(DmndFrcstConsumables.class.getName());

    final String PRIME_PART = "Y" ;
    private static TableSnapshot F1 = null ; // amd_dmnd_frcst_consumables
    private static TableSnapshot F2 = null ; // tmp_amd_dmnd_frcst_consumables

        static private void loadParams() {
		try {
			java.util.Properties p        = new AppProperties(DmndFrcstConsumables.class.getName()).getProperties() ;

			debug = p.getProperty("debug","false").equals("true") ;
			no_op = p.getProperty("no_op","false").equals("true") ;
			bufSize = Integer.valueOf( p.getProperty("bufSize","150000") ).intValue() ;
			debugThreshold = Integer.valueOf( p.getProperty("debugThreshold","50000") ).intValue() ;
			showDiffThreshold = Integer.valueOf( p.getProperty("showDiffThreshold","5000") ).intValue() ;
			ageBufSize = Integer.valueOf( p.getProperty("ageBufSize","150000") ).intValue() ;
			prefetchSize = Integer.valueOf( p.getProperty("prefetchSize","5000") ).intValue() ;
			logger.debug("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" + prefetchSize + " no_op=" + no_op
				+ " debug=" + debug + " debugThreshold=" + debugThreshold + " showDiffThreshold=" + showDiffThreshold) ;

			if (debug) {
				System.out.println("bufSize=" + bufSize + " ageBufSize = " + ageBufSize + " prefetchSize=" + prefetchSize + " no_op=" + no_op + " debugThreshold=" + debugThreshold + " showDiffThreshold=" + showDiffThreshold) ;
			}

		} catch (java.io.IOException e) {
			System.err.println("DmndFrcstConsumables: warning: " + e.getMessage()) ;
		} catch (java.lang.Exception e) {
			System.err.println("DmndFrcstConsumables: warning: " + e.getMessage()) ;
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
            F1 = new TableSnapshot(bufSize, new DmndFrcstConsumablesFactory("select nsn, sran, period, round(demand_forecast,4) demand_forecast, duplicate from amd_dmnd_frcst_consumables where action_code != 'D' order by nsn,sran,period")) ;
            F2 = new TableSnapshot(bufSize, new DmndFrcstConsumablesFactory("select nsn, sran,period,round(demand_forecast,4) demand_forecast, duplicate from tmp_amd_dmnd_frcst_consumables order by nsn, sran, period")) ;
            logger.debug("start diff") ;

	    w.diff(F1, F2) ;
        }
        catch (SQLException e) {
            System.err.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;
			System.exit(2) ;
        }
        catch (ClassNotFoundException e) {
            System.err.println(e.getMessage()) ;
            logger.error(e.getMessage()) ;
			System.exit(4) ;
        }
        catch (Exception e) {
			System.err.println(e.getMessage()) ;
			logger.error(e.getMessage()) ;
            if (F1 == null) {
				System.err.println("F1 not initialized.") ;
				logger.error("F1 not initialized.") ;
			}
            if (F2 == null) {
				System.err.println("F2 not initialize.") ;
				logger.error("F2 not initialized.") ;
			}
			System.exit(6) ;
		}
        finally {
            updateCounts() ;
	    System.exit(0) ;
        }
    }

    private static void updateCounts() {
	if (F1 != null) {
		System.out.println("amd_dmnd_frcst_consumables=" + F1.getRecsIn()) ;
		logger.info("amd_dmnd_frcst_consumables=" + F1.getRecsIn()) ;
	}
	if (F2 != null) {
		System.out.println("tmp_amd_dmnd_frcst_consumables=" + F2.getRecsIn()) ;
		logger.info("tmp_amd_dmnd_frcst_consumables=" + F2.getRecsIn()) ;
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
		String      nsn ;
		String	    sran ;
		int	    period ;


        public boolean equals(Object o) {
            Key k = (Key) o ;
            return ( (k.nsn.equals(nsn) )
			&& (k.sran.equals(sran) )
			&& (k.period == period ) ) ;

        }
        public int hashCode() {
            return nsn.hashCode()
            + sran.hashCode()
		+ new Integer(period).hashCode() ;
        }
        public int compareTo(Object o) {
            Key theKey ;
            theKey = (Key) o ;
            return (theKey.nsn + theKey.sran + theKey.period).compareTo(
				nsn + sran + period) ;
        }
        public String toString() {
            return "DmndFrcstConsumables =" + nsn + sran + period  ;
        }
    }
    Key key ;
    class Body {
		BigDecimal	demand_forecast ;
		boolean 	demand_forecast_isnull ;
		int		duplicate ;
		boolean 	duplicate_isnull ;


        public String toString() {
            return "demand_forcast=" + demand_forecast + " " + duplicate + " " ;
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
            boolean resultField ;
	    result = equal(b.demand_forecast, b.demand_forecast_isnull,  demand_forecast, demand_forecast_isnull) ; 
	    result = result && equal(b.duplicate, b.duplicate_isnull,  duplicate, duplicate_isnull) ; 
            return result ;
        }
    }
    Body body ;

    DmndFrcstConsumables(ResultSet r) throws SQLException {
        try {
        	key = new Key() ;
        	key.nsn = r.getString("NSN") ;
		key.sran = r.getString("SRAN") ;
		key.period = r.getInt("PERIOD") ;

        	body = new Body() ;
        	body.demand_forecast = r.getBigDecimal("DEMAND_FORECAST");
        	body.demand_forecast_isnull = r.wasNull();
        	body.duplicate = r.getInt("DUPLICATE");
        	body.duplicate_isnull = r.wasNull();
        } catch (java.sql.SQLException e) {
            updateCounts() ;
            System.err.println(e.getMessage() ) ;
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
            logger.fatal(e.getMessage()) ;
            System.exit(4) ;
        }
    }

    private void setInt(CallableStatement cstmt,
            int paramaterIndex, int value, boolean isNull) {
        try {
            if (isNull) {
                cstmt.setNull(paramaterIndex, java.sql.Types.NUMERIC) ;
            }
            else {
                cstmt.setInt(paramaterIndex, value) ;
            }
        } catch (java.sql.SQLException e) {
            updateCounts() ;
            System.err.println(e.getMessage()) ;
            logger.fatal(e.getMessage()) ;
            System.exit(4) ;
        }
    }
    

    private void  doDmndFrcstConsumablesDiff(String action_code) {
	if (! no_op) {
		try {
		    CallableStatement cstmt = amd.c.prepareCall(
			"{? = call amd_demand.doDmndFrcstConsumablesDiff("
			+ "?, ?, ?, ?, ?, ?)}") ;
		    cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
		    cstmt.setString(2, key.nsn) ;
		    cstmt.setString(3, key.sran) ;
		    cstmt.setInt(4, key.period) ;
		    setBigDecimal(cstmt, 5, body.demand_forecast, body.demand_forecast_isnull) ;
		    setInt(cstmt, 6, body.duplicate, body.duplicate_isnull) ;
		    cstmt.setString(7, action_code) ;
		    cstmt.execute() ;

		    int result = cstmt.getInt(1) ;
		    if (result > 0) {
			updateCounts() ;
			System.err.println("amd_demand.doDmndFrcstConsumables failed with result = " + result) ;
			logger.fatal("amd_demand.doDmndFrcstConsumables failed with result = " + result) ;
			System.exit(result) ;
		    }
		    cstmt.close() ;
		}
		catch (java.sql.SQLException e) {
		    updateCounts() ;
		    System.err.println(e.getMessage()) ;
		    logger.fatal(e.getMessage()) ;
		    System.exit(4) ;
		}
	}
	if ((rowsInserted + rowsDeleted + rowsUpdated) % debugThreshold == 0) {
		logger.info(action_code + ": key=" + key + " body=" + body) ;
	}
    }
    public void  insert() {
	    doDmndFrcstConsumablesDiff("A") ;
            rowsInserted++ ;
    }
    public void     update() {
	    doDmndFrcstConsumablesDiff("C") ;
            rowsUpdated++ ;
    }
    public void     delete() {
		doDmndFrcstConsumablesDiff("D") ;
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
