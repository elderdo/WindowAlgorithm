import java.sql.* ;

public class TmpSpareParts implements SortedSnapshot {
    final int MAXBUF = 73 ;
    protected InputBuffer buf = new InputBuffer(MAXBUF) ;
    protected ResultSet rs ;
    
    TmpSpareParts() throws SQLException {
        AmdConnection amd = AmdConnection.instance() ;
        Statement s = amd.c.createStatement() ;
        s.setFetchSize(100) ;
        rs = s.executeQuery("Select * from tmp_amd_spare_parts order by part_no, mfgr") ;
    }
    public void save() {
    }
    public Rec read() {
        try {
            if (rs.next())
                return new SparePart(rs) ;
            else
                return null ;
        }
        catch (SQLException e) {
            return null ;
        }
    }
    public InputBuffer getBlocks(int n) {
        try {
            for (int i = 0; (i < n && rs.next()); i++) {
		Rec theRec = new SparePart(rs) ;
                buf.put(theRec.getKey(),theRec) ;
            }
            if (buf.size() > 0) 
                return buf ;
            else
                return null ;
        }
        catch (SQLException e) {
            if (buf.size() > 0) 
                return buf ;
            else
                return null ;
        }
    }
}
