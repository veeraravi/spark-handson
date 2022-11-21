
class A{
    A(){
        System.out.println("A");
    }
}
class B extends A{
    B(){
        System.out.println("B");
    }

}


public class Test {
    public static void main(String[] args) {
       B b = new B();
    }
}
