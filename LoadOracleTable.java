import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import java.sql.* ;
import org.apache.log4j.Logger ;
import java.util.Calendar ;
import java.util.TimeZone ;
import java.text.SimpleDateFormat ;

/*   $Author:   zf297a  $
   $Revision:   1.1  $
       $Date:   03 Oct 2008 10:41:14  $
   $Workfile:   LoadOracleTable.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\LoadOracleTable.java.-arc  $
/*
/*   Rev 1.1   03 Oct 2008 10:41:14   zf297a
/*Changed the default input file to Uims.csv and the default oracle function to user_planner_pkg.insertUimsRow
/*
/*Added the separator character and quote character to the properties files and use the default values from the CSVReader class.
/*
/*Display the values of the input parameters.  Use the CSVReader constructor with the three arguments: filename, separator character,  and quote character.
/*
/*
/*   Rev 1.0   30 Sep 2008 11:11:02   zf297a
/*Initial revision.
  */

public class LoadOracleTable {

    static AmdConnection amd = AmdConnection.instance() ;
    static int rowsInserted ;
    static int rowsUpdated ;
    static int rowsDeleted ;

    static boolean debug ;
    static String inputFile = "../data/Uims.csv" ;
    static String oracleFunction = "{? = call user_planner_pkg.insertUimsRow("
					+ "?, ?, ?, ?, ?)}" ;
    static int functionFields = 6 ;
    static boolean no_op    = false;
    static boolean firstRecIsFieldNames    = true ;
    static char separator = CSVReader.DEFAULT_SEPARATOR ;
    static char quoteCharacter = CSVReader.DEFAULT_QUOTE_CHARACTER ;
	
    static Logger logger = Logger.getLogger(LoadOracleTable.class.getName());

    static private String [] fields;
    static private String [] fieldNames;

	static int countFields(String str) {
        
		logger.debug(str) ;

		if (oracleFunction == null)
			return 0;
        
		int counter = 0;
        
		for (int i = 0; i < str.length(); i++) {

			if (str.charAt(i) == '?')
				counter++;
	        }
		logger.debug("" + (counter - 1)) ;
		return counter - 1 ; // don't include the return code
        
	}

	static private void loadParams() {
		try {
			java.util.Properties p        = new AppProperties(LoadOracleTable.class.getName()).getProperties() ;

			quoteCharacter = p.getProperty("quoteCharacter",String.valueOf(quoteCharacter)).charAt(0) ;
			separator = p.getProperty("separator",String.valueOf(separator)).charAt(0) ;
			inputFile = p.getProperty("inputFile",inputFile) ;
			oracleFunction = p.getProperty("oracleFunction",oracleFunction) ;
			debug = p.getProperty("debug","false").equals("true") ;
			firstRecIsFieldNames = p.getProperty("firstRecIsFieldNames","true").equals("true") ;
			no_op = p.getProperty("no_op","false").equals("true") ;

		} catch (java.io.IOException e) {
			System.err.println("Warning: " + e.getMessage()) ;
		} catch (java.lang.Exception e) {
			System.err.println("Warning: " + e.getMessage()) ;
		}
	}
   
	public static void main(String[] args) {

		loadParams() ;
		functionFields = countFields(oracleFunction) ;
		if (amd.c == null)  {
			System.out.println("Unable to connect to AMD") ;
			System.exit(2) ;
		}

	        System.out.println("start time: " + now()) ;
		if (args.length > 0) {
			for (int i = 0; i < args.length; i++) {
				if (args[i].equals("-d")) {
					debug = true ;
				} else if (args[i].equals("-i") ) {
					inputFile = args[i+1] ;
				} else if (args[i].equals("-f") ) {
					oracleFunction = args[i+1] ;
					functionFields = countFields(oracleFunction) ;
				} else if (args[i].equals("-n") ) {
					firstRecIsFieldNames = true ;
				} else if (args[i].equals("-q") ) {
					quoteCharacter = args[i+1].charAt(0) ;
				} else if (args[i].equals("-s") ) {
					separator = args[i+1].charAt(0) ;
				}
			}
		}

		System.out.println("oracleFunction=" + oracleFunction) ;
		if (no_op) {
			System.out.print("no_op=" + no_op + ": ") ;
			System.out.println("Running simulation, no records will be inserted or updated");
		}
		if (separator != CSVReader.DEFAULT_SEPARATOR) {
			System.out.println("separator=" + String.valueOf(separator)) ;
		}
		if (quoteCharacter != CSVReader.DEFAULT_QUOTE_CHARACTER) {
			System.out.println("quoteCharacter=" + String.valueOf(quoteCharacter)) ;
		}
		System.out.println("inputFile=" + inputFile) ;

		try {
			CSVReader reader  = new CSVReader(new FileReader(inputFile), separator, quoteCharacter);
			int lineCnt = 0 ;
			while ((fields = reader.readNext()) != null) {
				lineCnt++ ;
				if (fields != null && fields[0] != "") {
					if (fields.length != functionFields) {
						System.out.println("Bad input file(" 
								+ inputFile 
								+ "): number of fields(" 
								+ fields.length 
								+ ") does not match function(" 
								+ functionFields 
								+ ")") ;
						for (int i =0; i < fields.length;i++)
							System.out.println("fields[" + i + "]=" + fields[i]) ;
						System.exit(24) ;
					}
					if (firstRecIsFieldNames && lineCnt == 1) {
						fieldNames = fields ;
					} else {
						insert() ;
					}
				}
			} 
			System.out.println(lineCnt + " records read from " + inputFile) ;
			try {
				amd.c.rollback() ;
			} catch (java.sql.SQLException e) {
				System.out.println(e.getMessage()) ;
				System.exit(4) ;
			}
			updateCounts() ;
		} catch (java.io.IOException e) {
			System.out.println(e.getMessage() ) ;
			System.exit(8) ;
		}
	}

	private static void updateCounts() {
		System.out.println("rows inserted/updated=" + rowsInserted) ;
		logger.info("rows inserted/updated=" + rowsInserted) ;
		System.out.println("end time: " + now()) ;
	}


	public static void     insert() {
		try {
			if (firstRecIsFieldNames) {
				for (int i = 0;i < functionFields; i++) {
					logger.debug(fieldNames[i] + "=" + fields[i] + " length=" + fields[i].length()) ;
				}
			}

			CallableStatement cstmt = amd.c.prepareCall(
					oracleFunction) ;
            		cstmt.registerOutParameter(1, java.sql.Types.NUMERIC) ;
			for (int i = 0; i < functionFields ; i++ ) {
				cstmt.setString(i+2, fields[i]) ;
			}
			if (!no_op) {
				cstmt.execute() ;
				int result = cstmt.getInt(1) ;
				if (result > 0) {
					updateCounts() ;
					System.out.println(oracleFunction + ": failed with result = " + result) ;
					logger.fatal(oracleFunction + ": failed with result = " + result) ;
					System.exit(result) ;
				}
			}
			rowsInserted++ ;
			cstmt.close() ;
		} catch (java.sql.SQLException e) {
			updateCounts() ;
			System.out.println(e.getMessage()) ;
			logger.fatal(e.getMessage()) ;
			System.exit(16) ;
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
