import java.sql.* ;
public class MovieFactory extends DBFactory
{
    MovieFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ; 
    }
    public Rec createRec() {
        try {
            return new Movie(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}