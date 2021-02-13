public interface Rec {
       public Comparable   getKey() ; 
       public Object   getBody() ;
       public boolean  keysEqual(Rec r) ;
       public boolean  bodiesEqual(Rec r) ;
       public void     insert() ;
       public void     update() ;
       public void     delete() ;
}