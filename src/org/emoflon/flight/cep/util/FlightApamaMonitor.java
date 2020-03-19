package org.emoflon.flight.cep.util;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.emoflon.flight.cep.util.events.EventServiceHandler;
import org.emoflon.flight.cep.util.events.EventServiceHandler.ServiceLevel;

import Flights.FlightModel;
import flight.monitor.FlightMonitor;

public class FlightApamaMonitor extends FlightMonitor {
	protected EventServiceHandler eventService;

	@Override
	public void initModelAndEngine(String modelPath) {
		super.initModelAndEngine(modelPath);
		init();
	}

	@Override
	public void initModelAndEngine(FlightModel model) {
		super.initModelAndEngine(model);
		init();
	}

	private void init() {
		issues = Collections.synchronizedMap(new LinkedHashMap<>());
		solutions = Collections.synchronizedMap(new LinkedHashMap<>());
		eventService = new EventServiceHandler("localhost", 15903, ServiceLevel.Test, issues, solutions);

		issueMessages = new LinkedBlockingQueue<>();
		infoMessages = new LinkedBlockingQueue<>();
		solutionMessages = new LinkedBlockingQueue<>();
	}

	@Override
	public void initMatchSubscribers() {
		api.flight().subscribeAppearing(eventService::sendMatchEvent);
		api.flight().subscribeDisappearing(eventService::sendMatchRemovedEvent);
		api.flightWithArrival().subscribeAppearing(eventService::sendMatchEvent);
//		api.travelWithFlight().subscribeAppearing(eventService::sendMatchEvent);
//		api.travelWithFlight().subscribeDisappearing(eventService::sendMatchRemovedEvent);
		api.travelHasConnectingFlight().subscribeAppearing(eventService::sendMatchEvent);
		api.travelHasConnectingFlight().subscribeDisappearing(eventService::sendMatchRemovedEvent);
		api.connectingFlightAlternative().subscribeAppearing(eventService::sendMatchEvent);
		api.connectingFlightAlternative().subscribeDisappearing(eventService::sendMatchRemovedEvent);
	}

	@Override
	public void update(boolean debug) {
		api.updateMatches();
		// TODO: Debug messages?
	}

	@Override
	public long getWorkingConnectingFlightTravels() {
		return eventService.requestWorkingConnectingFlights();
	}

	@Override
	public long getDelayedConnectingFlightTravels() {
		return eventService.requestDelayedConnectingFlights();
	}

	@Override
	public void shutdown() {
		api.terminate();
		eventService.closeSocket();
		TestPause.waitForEnter();
	}

}