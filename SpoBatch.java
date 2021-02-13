/*   
     $Author:   zf297a  $
   $Revision:   1.1  $
       $Date:   01 Jul 2009 15:18:32  $
   $Workfile:   SpoBatch.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\SpoBatch.java.-arc  $
/*
/*   Rev 1.1   01 Jul 2009 15:18:32   zf297a
/*Added export table arg to the main for the createBatch method
/*
/*   Rev 1.0   31 Jan 2008 10:39:28   zf297a
/*Initial revision.
 *
 * This class creates an x_imnp_interface_batch entry for the pr_imp process.  
 * The tablename used by the createBatch method must be
 * upper case and must match the table to be loaded by the pr_imp procedure.
 * The oracleInstacne can be overriden using the setOracleInstance method.
 * To use this class:
 * 	long batchNum = SpoBatch.createBatch("X_IMP_LP_OVERRIDE") ;
 * 	This will create a batcn interface record for table x_imp_lp_override.
 * 	The batchNum returned must be used as x_imp_lp_override's batch column's value.
*/

import org.apache.log4j.Logger ;
public class SpoBatch {

	static Logger logger = Logger.getLogger(SpoBatch.class.getName());

	static SpoConnection spo = SpoConnection.instance() ;

	static String oracleInstance = "spoC17v2" ;
		
	public static void setOracleInstance(String value) {
		oracleInstance = value ;
	}

	public static long createBatch(String tablename) {
		logger.debug("createBatch for " + tablename.replace('\n','_').replace('\r','_')) ;
		try {
			java.sql.PreparedStatement s = spo.c.prepareStatement("select " 
					+ oracleInstance + ".batch_sequence.nextval BATCH_COLUMN from dual") ;

			logger.debug("select " + oracleInstance + ".batch_sequence.nextval batch_column from dual".replace('\n','_').replace('\r','_')) ;
			java.sql.ResultSet rsBatch = s.executeQuery() ;
			rsBatch.next() ;
			long batch = rsBatch.getLong("BATCH_COLUMN") ;	
			logger.debug("rsBatch(batch_column)=" + batch) ;
				
			java.sql.PreparedStatement s2 = spo.c.prepareStatement("select " 
					+ oracleInstance + ".interface_sequence.nextval INTERFACE_COLUMN from dual") ; 
			java.sql.ResultSet rsInterface = s2.executeQuery(); 
			rsInterface.next() ;
			long interfaceNum = rsInterface.getLong("INTERFACE_COLUMN") ;
			logger.debug(("rsInterface(interface_column)=" + interfaceNum).replace('\n','_').replace('\r','_')) ;

			String insertCmd = "insert into " + oracleInstance 
				+ ".x_imp_interface_batch (interface,batch,batch_mode,action,interface_sequence)"
				+ "values ('" + tablename + "'," + batch + ",'Overlay','INS'," 
				+  interfaceNum + ")" ;

			logger.debug(insertCmd.replace('\n','_').replace('\r','_')) ;

			java.sql.PreparedStatement insertStmt = spo.c.prepareStatement(insertCmd) ;
			s.executeUpdate() ;

			return batch ;

		} catch (java.sql.SQLException e) {
			System.err.println(e.getMessage()) ;
			logger.error(e.getMessage()) ;
			return 0 ;
		}

	}
	static public void main(String[] Args) {
		String exportTable = "X_IMP_LP_OVERRIDE" ;
		if (Args.length == 1) {
			exportTable = Args[0] ;
		}
		try {
			if (!spo.c.isClosed()) {
				System.out.println("createBatch(" 
					+ exportTable + ")=" + createBatch(exportTable)) ;
			}
		}  catch (java.sql.SQLException e) {
			System.err.println(e.getMessage()) ;
			logger.error(e.getMessage()) ;
		}
	}

}

