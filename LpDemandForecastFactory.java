import java.sql.* ;
import org.apache.log4j.Logger ;

public class LpDemandForecastFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(LpDemandForecastFactory.class.getName());

    LpDemandForecastFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    LpDemandForecastFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new LpDemandForecast(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
