import java.util.HashMap;
import java.util.Map;

class MyClass {
    public void method(Map m) {
        m = null;
    }
}


public class TestReference {
    public static void main(String[] args) {
        MyClass m = new MyClass();
        Map map = new HashMap();
        map.put("A", "A");
        map.put("b", "b");
        map.put("c", "c");

        System.out.println("1 "+map);
        m.method(map);

        System.out.println("2 "+map);

        method(map);

        System.out.println("3 "+map);
    }


    public static void method(Map m) {

        m.put("d", "d");
        m = null;
    }

}
