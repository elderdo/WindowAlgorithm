import java.sql.* ;
public class SparePartsFactory extends DBFactory
{
    SparePartsFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ; 
    }
    public Rec createRec() {
        try {
            return new SparePart(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}