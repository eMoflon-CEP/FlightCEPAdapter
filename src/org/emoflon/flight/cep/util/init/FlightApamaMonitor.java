package org.emoflon.flight.cep.util.init;

import org.eclipse.emf.common.util.URI;
import org.emoflon.flight.cep.util.events.EventServiceHandler;

import FlightGTCEP.api.FlightGTCEPAPI;
import FlightGTCEP.api.FlightGTCEPApp;
import FlightGTCEP.api.FlightGTCEPHiPEApp;
import Flights.FlightModel;

public class FlightApamaMonitor {
	protected FlightGTCEPApp app;
	protected FlightGTCEPAPI api;
	protected EventServiceHandler eventService;
	
	public void init(String modelPath) {
		app = new FlightGTCEPHiPEApp();
		app.registerMetaModels();
		URI uri = URI.createFileURI(modelPath);
		app.loadModel(uri);
		api = app.initAPI();
		
		eventService = new EventServiceHandler("localhost", 15903);
	}
	
	public void init(FlightModel model) {
		app = new FlightGTCEPHiPEApp();
		app.registerMetaModels();
		app.createModel(URI.createFileURI(model.toString()));
		app.getModel().getResources().get(0).getContents().add(model);
		api = app.initAPI();
		
		eventService = new EventServiceHandler("localhost", 15903);
	}
	public void initMatchSubscribers() {
		api.flight().subscribeAppearing(eventService::sendMatchEvent);
		api.flight().subscribeDisappearing(eventService::sendMatchRemovedEvent);
		api.travelWithFlight().subscribeAppearing(eventService::sendMatchEvent);
		api.travelWithFlight().subscribeDisappearing(eventService::sendMatchRemovedEvent);
		api.travelHasConnectingFlight().subscribeAppearing(eventService::sendMatchEvent);
		api.travelHasConnectingFlight().subscribeDisappearing(eventService::sendMatchRemovedEvent);
		api.connectingFlightAlternative().subscribeAppearing(eventService::sendMatchEvent);
		api.connectingFlightAlternative().subscribeDisappearing(eventService::sendMatchRemovedEvent);
	}
	
	public void shutdown() {
		api.terminate();
	}
	
	
}