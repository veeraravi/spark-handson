
import java.util.*;
public class SpecialNumber {

    //function to identify good numbers
    public static boolean isGood(int n) {
        String str = String.valueOf(n);
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) != '4' && str.charAt(i) != '5')
                return false;
        }
        return true;
    }

    public static void main(String[] args) {
        Scanner fs = new Scanner(System.in);
        int n = fs.nextInt();
        List<Integer> arr = new ArrayList();
        for (int i = 1; i <= n; i++) {
            if (isGood(i) == true) {
                arr.add(i);
            }
        }
        int[][] dp = new int[arr.size() + 1][n + 1];//dp


        for (int col = 0; col < dp[0].length; col++)
            dp[0][col] = Integer.MAX_VALUE;//base case
        for (int row = 1; row < dp.length; row++)
            dp[row][0] = 0;//base case


        //Try optimizing the values at each step
        for (int row = 1; row < dp.length; row++) {
            for (int col = 1; col < dp[0].length; col++) {
                if (arr.get(row - 1) > col) {
                    dp[row][col] = dp[row - 1][col];
                } else if (dp[row][col - arr.get(row - 1)] == Integer.MAX_VALUE) {
                    dp[row][col] = dp[row - 1][col];
                } else {
                    dp[row][col] = Math.min(1 + dp[row][col - arr.get(row - 1)],
                            dp[row - 1][col]);
                }
            }
        }

        //result
        System.out.println(dp[arr.size()][n] == Integer.MAX_VALUE ? -1 : dp[arr.size()][n]);
    }
}


