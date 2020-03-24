package org.emoflon.flight.cep.test;

import org.emoflon.flight.cep.util.FlightApamaMonitor;
import org.emoflon.flight.scenario.EvaluationScenarioRunner;
import org.emoflon.flight.scenario.ScenarioRunner;

import flight.monitor.FlightMonitor;
import flight.puregt.FlightGTMonitor;
import flight.util.Runtimer;

public class FlightCEPMonitorDemo {
	
	
	public static void main(String[] args) {
		EvaluationScenarioRunner runner = new EvaluationScenarioRunner(15);
		runner.initModel("../Flights/src/org/emoflon/flight/model/definitions");
		runner.initModelEventGenerator(15, 12, 51, 0.01, 0.5);
		
		FlightMonitor monitor = new FlightApamaMonitor();
		monitor.initModelAndEngine(runner.getModel());
		monitor.initMatchSubscribers();
//		monitor.update(true);
//		runner.addFlightsAndBookings(2);
//		monitor.update(true);
//		
//		int i = 0;
//		while(runner.advanceTime() &&  i < 20) {
//			monitor.update(true);
//			i++;
//		}
//		monitor.update(true);
//		((FlightApamaMonitor)monitor).synch();
//		
//		System.err.println("Broken connecting flights: " + monitor.getDelayedConnectingFlightTravels());
//		System.err.println("Working connecting flights: " + monitor.getWorkingConnectingFlightTravels());
//		System.err.println("Issues: " + monitor.getIssues().size());
		
		Runtimer timer = Runtimer.getInstance();
		timer.measure(FlightCEPMonitorDemo.class, "FlightCEPRun", ()->run(monitor, runner));
		monitor.shutdown();
		
		System.out.println(timer.toString());
	}
	
	public static void run(FlightMonitor monitor, ScenarioRunner runner) {
		monitor.update(true);

		int i = 0;
		while(runner.advanceTime() &&  i < 20) {
			monitor.update(true);
			i++;
		}
		
		monitor.update(true);
		((FlightApamaMonitor)monitor).synch();
//		monitor.getDelayedConnectingFlightTravels();
//		monitor.getWorkingConnectingFlightTravels();
//		monitor.getIssues();
		
		System.err.println("Broken connecting flights: " + monitor.getDelayedConnectingFlightTravels());
		System.err.println("Working connecting flights: " + monitor.getWorkingConnectingFlightTravels());
		System.err.println("Issues: " + monitor.getIssues().size());
	}
}
