package org.emoflon.flight.cep.test;

import org.emoflon.flight.cep.util.FlightApamaMonitor;
import org.emoflon.flight.scenario.EvaluationScenarioRunner;

import flight.monitor.FlightMonitor;

public class FlightCEPMonitorDemo {
	
	
	public static void main(String[] args) {
		EvaluationScenarioRunner runner = new EvaluationScenarioRunner(10);
		runner.initModel("../Flights/src/org/emoflon/flight/model/definitions");
		runner.initModelEventGenerator(15, 12, 51, 0.01, 0.5);
		
		FlightMonitor monitor = new FlightApamaMonitor();
		monitor.initModelAndEngine(runner.getModel());
		monitor.initMatchSubscribers();
		monitor.update(true);
//		runner.addFlightsAndBookings(2);
//		monitor.update(true);
		
		int i = 0;
		while(runner.advanceTime() &&  i < 20) {
			monitor.update(true);
			i++;
		}
		monitor.update(true);
		((FlightApamaMonitor)monitor).synch();
		
		System.err.println("Broken connecting flights: " + monitor.getDelayedConnectingFlightTravels());
		System.err.println("Working connecting flights: " + monitor.getWorkingConnectingFlightTravels());
		System.err.println("Issues: " + monitor.getIssues().size());
		monitor.shutdown();
	}
}
