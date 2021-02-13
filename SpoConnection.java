import java.sql.* ;
import java.io.* ;
import java.util.Properties ;
/*   $Author:   zf297a  $
   $Revision:   1.2  $
       $Date:   14 Dec 2007 20:34:18  $
   $Workfile:   SpoConnection.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\SpoConnection.java.-arc  $
/*
/*   Rev 1.2   14 Dec 2007 20:34:18   zf297a
/*Made the class a subclass of DBConnection.  The default constructor overrides the property names for the connection string, the userid and the password.
/*
/*   Rev 1.1   Oct 19 2007 11:32:36   c970984
/*new
/*
/*   Rev 1.0   Sep 24 2007 13:53:04   c970984
/*Initial revision.

  */

public class SpoConnection extends DBConnection 
{
       	static private SpoConnection instance_ = new SpoConnection() ;


        private SpoConnection()  {
		setConnectionStringProperty("ConnectionString_SPO") ;
		setUidProperty("UID_SPO") ;
		setPwdProperty("PWD_SPO") ;
		try {
			loadParams() ;
			createConnection() ;
		} catch(java.lang.Exception e) {
			System.err.println("warning: " + e.getMessage()) ;
		}
        }

        static public SpoConnection instance() {
                return instance_ ;
        }

}
