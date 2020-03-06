package org.emoflon.flight.cep.util;

import java.util.Scanner;

public class TestPause {
	public static void waitForEnter() {
		System.out.print("Restart Correlator. Press [ENTER] inside the Console "
				+ "after the restart to continue...");
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
	}
}
