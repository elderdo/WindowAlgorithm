/*   $Author:   zf297a  $
   $Revision:   1.1  $
       $Date:   08 Jan 2008 22:56:18  $
   $Workfile:   PartLocForecastsFactory.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\PartLocForecastsFactory.java.-arc  $
/*
/*   Rev 1.1   08 Jan 2008 22:56:18   zf297a
/*Fixed Logger
/*
/*   Rev 1.0   Mar 08 2006 00:04:26   zf297a
/*Initial revision.
*/
import java.sql.* ;
import org.apache.log4j.Logger ;

public class  PartLocForecastsFactory extends DBFactory
{
    static Logger logger = Logger.getLogger(PartLocForecastsFactory.class.getName());

     PartLocForecastsFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ;
    }
    public Rec createRec() {
        try {
            return new PartLocForecasts(rs) ;
        }
        catch (SQLException e) {
	    logger.debug("SQLException " + e.getMessage()) ;
            return null ;
        }
    }
}
