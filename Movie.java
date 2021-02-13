import java.sql.* ;
/*
 *   tables are movie and movie2 on dev3 for amd_owner.
 *   testing with inputbuffers small - so they drop to respective
 *   aging buffers instead of just the inputbuffer to inputbuffer match. 

 *   expected outcomes:
 *   movie lacks ID 125 (deletes)
 *   movie2 lacks ID 131, 136 (inserts)
 *   135 and 127 have had their bodies updated.

 *   incorrect => getting insert and delete of 133.

 **/
public class Movie implements Rec {
    public static void main(String[] args) {
        WindowAlgo w = new WindowAlgo(/* input buf */ 3, 
            /* aging buffer */ 30) ;
        try {
            TableSnapshot F1 = new TableSnapshot(73, new MovieFactory("Select * from movie2")) ;
            TableSnapshot F2 = new TableSnapshot(73, new MovieFactory("Select * from movie order by id desc")) ;
            w.diff(F1, F2) ;
        }
        catch (SQLException e) {
            System.out.println(e.getMessage()) ;
        }
        catch (ClassNotFoundException e) {
            System.out.println(e.getMessage()) ;
        }
    }
     class Key implements Comparable {
        int 	ID ; 	
        public boolean equals(Object o) {
		   Key k = (Key) o; 
	           return (k.ID == ID);
       }
        public int compareTo(Object o) {
            Key theKey ;   
            theKey = (Key) o ;
	    if (ID > theKey.ID){
		return 1;
	    }else if (ID == theKey.ID){
		return 0;
	    }else{
		return -1;
	    }
        }
        public String toString() {
            return ID + "";
        }
	// here!added
	public int hashCode() {	
		return ID;
	}
    }
    Key key ;
    class Body {
        String      title;
        String      director;
        String      actor;
        String      actress;
        String      type;
        String      rating;
        float 	    daily_rate;

        public String toString() {
           return 
        	title +
	        director +
        	actor +
	        actress +
	        type +
        	rating +
        	daily_rate;
        }
        boolean equal(String s1, String s2) {
            if (s1 != null)
                if (s2 != null)
                    return s1.equals(s2) ;
                else
                    return false ;
            else if (s2 == null)        
                return true ; // both null
            else
                return false ; // s1 == null && s2 != null
                        
        }
        boolean equal(int i1, boolean null1, int i2, boolean null2) {
            if (!null1)
                if (!null2)
                    return i1 == i2 ;
                else
                    return false ;
            else if (null2)
                return true ; // both null
            else
                return false ; // i1 == null && i2 != null
        }
        boolean equal(double d1, boolean null1, double d2, boolean null2) {
            if (!null1)
                if (!null2)
                    return d1 == d2 ;
                else
                    return false ;
            else if (null2)
                return true ; // both null
            else
                return false ; // i1 == null && i2 != null
        }
        public boolean equals(Object o) {
             Body b = (Body) o;
	    boolean result;
	    result = equal(b.title, title);
	    result = result && equal(b.director, director);
	    result = result && equal(b.actor, actor);
	    result = result && equal(b.actress, actress);
	    result = result && equal(b.type, type);
	    result = result && equal(b.rating, rating);
	    result = result && (b.daily_rate == daily_rate);
	    return result;
        }
    }
    Body body ;

    Movie(ResultSet r) throws SQLException {
        this.key = new Key() ;
        this.key.ID = r.getInt("ID") ;
        body = new Body() ;
        body.title= r.getString("TITLE") ;
        body.director= r.getString("DIRECTOR") ;
        body.actor= r.getString("ACTOR");
        body.actress= r.getString("ACTRESS") ;
        body.type= r.getString("TYPE") ;
        body.rating= r.getString("RATING") ;
        body.daily_rate = r.getFloat("DAILY_RATE") ;
    }
   
    public Comparable  getKey() {
        return  key ;
    }
    public Object getBody() {
        return body ;
    }

    
    public boolean  keysEqual(Rec r) {
        boolean result = key.equals(r.getKey()) ;
        return (key.equals( r.getKey() )) ;
    }
    public boolean  bodiesEqual(Rec r) {
        return (body.equals(r.getBody())) ;
    }
    public void     insert() {
      System.out.println("Insert: key=" + key + " body=" + body) ;
    }

    public void     update() {
      System.out.println("Update: key=" + key + " body=" + body) ;
    }
    public void     delete() {
      System.out.println("Delete: key=" + key) ;
    }
//here

     public boolean equals(Object o) {
	return (this.keysEqual( (Rec)o ) && this.bodiesEqual( (Rec)o ) );
     }


}
