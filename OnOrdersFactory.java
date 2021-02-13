/*   $Author:   zf297a  $
   $Revision:   1.3  $
       $Date:   08 Jan 2008 22:55:14  $
   $Workfile:   OnOrdersFactory.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\OnOrdersFactory.java-arc  $
/*
/*   Rev 1.3   08 Jan 2008 22:55:14   zf297a
/*Fixed Logger
/*
/*   Rev 1.2   20 Dec 2007 19:08:20   zf297a
/*Added logger debug statements.
/*
/*   Rev 1.1   Jul 30 2004 12:42:42   c970183
/*add pvcs keyword comments
*/
import java.sql.* ;
import org.apache.log4j.Logger ;

public class OnOrdersFactory extends DBFactory
{
    static Logger logger = Logger.getLogger(OnOrdersFactory.class.getName());

    OnOrdersFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ;
	logger.debug("executed " + query) ;
    }
    public Rec createRec() {
        try {
            return new OnOrder(rs) ;
        }
        catch (SQLException e) {
	    System.err.println("SQLException " + e.getMessage()) ;
            return null ;
        }
    }
}
