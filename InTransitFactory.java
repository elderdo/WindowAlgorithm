import java.sql.* ;
import org.apache.log4j.Logger ;

public class InTransitFactory extends DBFactory
{
    static Logger logger = Logger.getLogger(WindowAlgo.class.getName());

    InTransitFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ;
    }
    public Rec createRec() {
        try {
            return new InTransit(rs) ;
        }
        catch (SQLException e) {
			logger.debug("SQLException " + e.getMessage()) ;
            return null ;
        }
    }
}