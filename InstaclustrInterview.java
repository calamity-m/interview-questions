
public class InstaclustrInterview {
	public static void InstaclustrPrint() {
		
		// Loop from 1 to 100 as our range is [1, 100]
		for (int i = 1; i <= 100; i++) {
			
			// If we have a multiple of both 3 and 5 (3 * 5 = 15)
			if (i % 15 == 0)
				System.out.print("Instaclustr");
			// If we have a multiple of 3
			else if (i % 3 == 0)
				System.out.print("Insta");
			// If we have a multiple of 5
			else if (i % 5 == 0)
				System.out.print("clustr");
			// Not a multiple of either 3 or 5
			else
				System.out.print(i);
			
			// Add space formatting to output
			System.out.print(" ");	
		}
	}
	
	public static void main(String[] args) {
		InstaclustrPrint();
	}
}
