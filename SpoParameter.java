/*   
     $Author:   zf297a  $
   $Revision:   1.0  $
       $Date:   21 Nov 2008 13:43:02  $
   $Workfile:   SpoParameter.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\SpoParameter.java.-arc  $
/*
/*   Rev 1.0   21 Nov 2008 13:43:02   zf297a
/*Initial revision.
 *
 * This class provides a way to get SPO parameters.
 * The oracleInstacne can be overriden using the setOracleInstance method.
 * To use this class:
 * 	long batchNum = SpoParameter.getValue("MAX_OVERRIDE_VALUE") ;
 * 	This will retrieve the value in the SPO parameter table.
*/

import org.apache.log4j.Logger ;
public class SpoParameter {

	static Logger logger = Logger.getLogger(SpoParameter.class.getName());

	static SpoConnection spo = SpoConnection.instance() ;

	static String oracleInstance = "spoC17v2" ;
		
	public static void setOracleInstance(String value) {
		oracleInstance = value ;
	}

	public static int getValue(String parameterName) {
		logger.debug("getting parameter " + parameterName) ;
		try {
			java.sql.Statement s = spo.c.createStatement() ;

			logger.debug("select value from " + oracleInstance + ".parameter where name = '" + parameterName + "'") ;
			java.sql.ResultSet rsParameter = s.executeQuery("select value from " + oracleInstance + ".parameter where name = '" + parameterName + "'") ;
			rsParameter.next() ;
			int value = rsParameter.getInt("VALUE") ;	
			logger.debug("rsParameter(" + parameterName + ")=" + value) ;
				

			return value ;

		} catch (java.sql.SQLException e) {
			System.err.println(e.getMessage()) ;
			logger.error(e.getMessage()) ;
			return 0 ;
		}

	}
	static public void main(String[] Args) {
		try {
			if (!spo.c.isClosed()) {
				System.out.println("getValue(MAX_OVERRIDE_VALUE)=" + getValue("MAX_OVERRIDE_VALUE")) ;
			}
		}  catch (java.sql.SQLException e) {
			System.err.println(e.getMessage()) ;
			logger.error(e.getMessage()) ;
		}
	}

}
