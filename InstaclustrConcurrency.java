import java.util.Arrays;
import java.util.InputMismatchException;
import java.lang.Thread;
import java.util.Scanner;
import java.util.Stack;
import java.util.concurrent.ThreadLocalRandom;

class RandomNumberRunnable implements Runnable {

	// Upper bound for our random generator
	private int upperBound;
	
	RandomNumberRunnable(int upperBound) {
		this.upperBound = upperBound;
	}
	
	public void run() {
		// Ensure that we keep pushing to stack until we have at least a stack of 30
		while (InstaclustrConcurrency.randomIntStack.size() < 30) {
			// Calculate random number using thread safe random generator
			int currentRand = ThreadLocalRandom.current().nextInt(1, upperBound);
			// Push random number to the static stack
			InstaclustrConcurrency.randomIntStack.push(currentRand);
		}
	}
}

public class InstaclustrConcurrency {	
	public static Stack<Integer> randomIntStack = new Stack<Integer>();
	
	public static void printStackInformation(int frequencyRange) {		
		// Initialize the variables we will need to keep track
		int min, max, total, numFreq = 0;
		int[] freq = new int[frequencyRange];
		
		if (randomIntStack.size() < 30)
			return; // Exit if for some reason our stack is too small
		
		// Pop the top of stack and initialize variables
		int top = randomIntStack.pop();
		min = top; max = top; total = top;
		
		// Only check the last 30 numbers
		for (int i = 1; i < 30; i++) {
			// Pop our next int for checking
			top = randomIntStack.pop();	
			if (top < min)
				min = top;
			if (top > max)
				max = top;
			total += top;
			// Increment the frequency of this value
			freq[top]++;
		}
		
		// Calculate most frequent number using frequency array
		for (int i = 0; i < freq.length; i++) {
			// If the frequency of the number at this index is larger, new max
			if (freq[i] > freq[numFreq])
				numFreq=i;
		}
		
		// Display our values
		System.out.println("min: " + min + ", max: " + max + ", avg: " 
				+ (float)total/30.0f + ", most frequent: " + numFreq);
		
	}

	public static int getSanitizedInput(Scanner console) {
		System.out.println("Please enter N threads to be created:");	
		int input = 0;
		// Sanitize the input
		try {
			input = console.nextInt();
			if (input < 0)
				throw new InputMismatchException("Negative integer");
		} catch (InputMismatchException e) {
			System.out.println("Can only accept positive integers, try again:");
			console.nextLine();
			return getSanitizedInput(console);
		}	
		console.close();
		return input;
	}
	
	public static void main(String[] args) {
		int n = getSanitizedInput(new Scanner(System.in)); // Get input from the user
		int range = 11; // set to 11 due to exclusions in random range

		Thread[] threads = new Thread[n]; // Initialize our thread array
		for (int i = 0; i < n; i++) { // Create n threads
			threads[i] = new Thread(new RandomNumberRunnable(range));
			threads[i].start();
		}
		
		for (int i = 0; i < n; i++) { // Attempt to join threads
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		// Print out the information of our current stack after threads finished adding randoms
		printStackInformation(range);		
	}
}
