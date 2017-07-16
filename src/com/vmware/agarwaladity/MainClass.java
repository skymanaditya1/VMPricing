package com.vmware.agarwaladity;

import java.util.*;

public class MainClass {
	public static void main(String[] args) {
		Scanner in = new Scanner(System.in);
		System.out.println("Calculate VM Pricing from 1. CSV 2. Database?");
		int choice = in.nextInt();
		if(choice == 1) {
			// calculate VM Pricing from CSV
			CalculatingPricesDB calculateVMPrices = new CalculatingPricesDB();
			calculateVMPrices.findVMPrices();
		}
		else if (choice == 2) {
			// calculate VM Pricing from Database
			// DatabaseUtility db = new DatabaseUtility();
			// db.createDB();
		}
		else {
			// Invalid choice
			System.out.println("Invalid choice Selected");
		}
		in.close();
	}
}
