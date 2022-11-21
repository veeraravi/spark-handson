import java.util.stream.Collectors;

public class Uniqchar {
    public static void main(String[] args) {
    String s = "thisisatest";
        uniqueCharacters2(s);
        uniqueCharacters(s);
    }


    public static void uniqueCharactersSplit(String test){
        String[] tokens = test.split("");

        String temp = "";
        for (int i = 0; i < test.length(); i++){
            char current = test.charAt(i);
            if (temp.indexOf(current) < 0){
                temp = temp + current;
            } else {
                temp = temp.replace(String.valueOf(current), "");
            }
        }
        System.out.println("result "+temp + " ");
    }



    public static void uniqueCharacters(String test){
        String temp = "";
        for (int i = 0; i < test.length(); i++){
            char current = test.charAt(i);
            if (temp.indexOf(current) < 0){
                temp = temp + current;
            } else {
                temp = temp.replace(String.valueOf(current), "");
            }
        }
        System.out.println("result "+temp + " ");
    }

    public static void uniqueCharacters2(String test) {
        System.out.println(test.chars().distinct().mapToObj(c -> String.valueOf((char)c)).collect(Collectors.joining()));
    }
}
