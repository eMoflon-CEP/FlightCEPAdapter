package org.emoflon.flight.cep.test;

import org.emoflon.flight.cep.util.FlightApamaMonitor;
import org.emoflon.flight.scenario.EvaluationScenarioRunner;

import flight.monitor.FlightMonitor;
import flight.util.Runtimer;

public class FlightCEPMonitorDemo {
	
	public static void main(String[] args) {
		EvaluationScenarioRunner runner = new EvaluationScenarioRunner(3,1);
		runner.initModel("../Flights/src/org/emoflon/flight/model/definitions");
		runner.initModelEventGenerator(15, 12, 51, 0.01, 0.5);
		
		FlightMonitor monitor = new FlightApamaMonitor();
		monitor.initModelAndEngine(runner.getModel());
		monitor.initMatchSubscribers();
		
		Runtimer timer = Runtimer.getInstance();
		timer.measure(FlightCEPMonitorDemo.class, "FlightCEPRun", ()->run(monitor, runner));
		monitor.shutdown();
		
		System.out.println(timer.toString());
	}
	
	public static void run(FlightMonitor monitor, EvaluationScenarioRunner runner) {
		Runtimer timer = Runtimer.getInstance();
		monitor.update(true);
		
		double days = 6.0;
		double delta = 0.5;
		boolean runnable = true;
		while(runnable && days>0) {
			timer.pause();
			runnable = runner.runForDays(delta);
			timer.resume();
			
			monitor.update(true);
			days-=delta;
		}
		
		monitor.update(true);
		((FlightApamaMonitor)monitor).synch();
		
		System.err.println("Broken connecting flights: " + monitor.getDelayedConnectingFlightTravels());
		System.err.println("Working connecting flights: " + monitor.getWorkingConnectingFlightTravels());
		System.err.println("Issues: " + monitor.getIssues().size());
	}
}
