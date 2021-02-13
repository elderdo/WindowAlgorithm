import java.sql.* ;
import org.apache.log4j.Logger ;

public class InTransitSumFactory extends DBFactory
{
    static Logger logger = Logger.getLogger(WindowAlgo.class.getName());

    InTransitSumFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ;
    }
    public Rec createRec() {
        try {
            return new InTransitSum(rs) ;
        }
        catch (SQLException e) {
			logger.debug("SQLException " + e.getMessage()) ;
            return null ;
        }
    }
}