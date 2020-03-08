package org.emoflon.flight.cep.util.events;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.emoflon.ibex.gt.api.GraphTransformationMatch;

import FlightGTCEP.api.matches.ConnectingFlightAlternativeMatch;
import FlightGTCEP.api.matches.FlightMatch;
import FlightGTCEP.api.matches.FlightWithArrivalMatch;
import FlightGTCEP.api.matches.TravelHasConnectingFlightMatch;
import FlightGTCEP.api.matches.TravelWithFlightMatch;
import flight.monitor.FlightIssueEvent;
import flight.monitor.FlightSolutionEvent;

public class EventServiceHandler {
	public enum ServiceLevel {
		Error,
		Info,
		Test,
		Debug
	}

	private static final String eventProcessTyp = "eMoflonPatternMatch";
	private static final String requestProcessTyp = "RequestResponseChannel";
	private static final String receiveProcessTyp = "ReceiveChannel";
	private String[] requestChannels = {"eMoflonPatternMatch","eMoflonTalkback"};
	private String[] receiveChannels = {"eMoflonTalkback"};
	private long eventsSend = 0;
	private ThreadSender sender;
	private Requester requester;
	private Receiver receiver;
	private ServiceLevel serviceLevel;
	@SuppressWarnings("rawtypes")
	protected Map<Long, GraphTransformationMatch> matches;
	@SuppressWarnings("rawtypes")
	private Map<GraphTransformationMatch, FlightIssueEvent<GraphTransformationMatch>> issues;
	@SuppressWarnings("rawtypes")
	private Map<FlightIssueEvent<GraphTransformationMatch>, FlightSolutionEvent<GraphTransformationMatch, GraphTransformationMatch>> solutions;

	

	public EventServiceHandler(String host, int port, ServiceLevel serviceLevel,
			@SuppressWarnings("rawtypes") Map<GraphTransformationMatch, FlightIssueEvent<GraphTransformationMatch>> issues,
			@SuppressWarnings("rawtypes") Map<FlightIssueEvent<GraphTransformationMatch>, FlightSolutionEvent<GraphTransformationMatch, GraphTransformationMatch>> solutions) {
		sender = new ThreadSender(host, port, eventProcessTyp);
		requester = new Requester(host, port, requestChannels, requestProcessTyp);
		this.serviceLevel = serviceLevel;
		matches = Collections.synchronizedMap(new LinkedHashMap<>());
		this.issues = issues;
		this.solutions = solutions;
		receiver = new Receiver(host, port, receiveChannels, receiveProcessTyp, matches, this.issues, this.solutions);
		sender.start();
		
		if(this.serviceLevel==ServiceLevel.Test) sendServiceLevel();
	}

	public long getEventsSend() {
		return eventsSend;
	}

	public void sendEvent(String eventString) {
		sender.sendEventString(eventString);
		eventsSend++;
	}

	
	public long requestWorkingConnectingFlights() {
		return requester.requestWorkingConnectingFlights();
	}
	
	public long requestDelayedConnectingFlights() {
		return requester.requestDelayedConnectingFlights();
	}

	public void closeSocket() {
		receiver.terminate();
		sender.close();
		try {
			sender.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private void sendServiceLevel() {
		sendEvent("ServiceLevel(\"" + serviceLevel + "\")");
	}

	public void sendMatchEvent(FlightMatch match) {
		sendEvent("FlightMatchEvent" + "(\"" + match.getFlight().getID() + "\"," + match.getFlight().getArrival().getTime() + ","
				+ match.getFlight().getDeparture().getTime() + "," + match.getRoute().getDuration() + ")");
	}

	public void sendMatchRemovedEvent(FlightMatch match) {
		sendEvent("FlightRemovedEvent" + "(\"" + match.getFlight().getID() + "\")");
	}
	
	public void sendMatchEvent(FlightWithArrivalMatch match) {
		sendEvent("FlightMatchEvent" + "(\"" + match.getFlight().getID() + "\"," + match.getFlight().getArrival().getTime() + ","
				+ match.getFlight().getDeparture().getTime() + "," + match.getRoute().getDuration() + ")");
	}

	public void sendMatchEvent(TravelWithFlightMatch match) {
		sendEvent("TravelWithFlightMatchEvent" + "(\"" + match.getFlight().getID() + "\","
				+ match.getPlane().getCapacity() + ")");
	}

	public void sendMatchRemovedEvent(TravelWithFlightMatch match) {
		sendEvent("TravelWithFlightMatchRemovedEvent" + "(\"" + match.getFlight().getID() + "\")");
	}

	public void sendMatchEvent(TravelHasConnectingFlightMatch match) {
		long matchID = eventsSend;
		matches.put(matchID, match);
		sendEvent("TravelHasConnectingFlightMatchEvent" + "(" + matchID + ",\"" 
				+ match.getTravel().getID() + "\",\""
				+ match.getConnectingFlight().getID() + "\",\"" + match.getFlight().getID() + "\","
				+ match.getConnectingFlight().getDeparture().getTime() + "," + match.getFlight().getArrival().getTime()
				+ "," + match.getArrivalGate().getPosition() + "," + match.getDepartingGate().getPosition() + ","
				+ match.getTransitAirport().getSize() + ")");
	}

	public void sendMatchRemovedEvent(TravelHasConnectingFlightMatch match) {
		sendEvent("TravelHasConnectingFlightMatchRemovedEvent" + "(\"" + match.getTravel().getID() + "\",\""
				+ match.getConnectingFlight().getID() + "\")");
	}

	public void sendMatchEvent(ConnectingFlightAlternativeMatch match) {
		long matchID = eventsSend;
		matches.put(matchID, match);
		sendEvent("ConnectingFlightAlternativeMatchEvent" + "(" + matchID + ",\""
				+ match.getConnectingFlight().getID() + "\",\""
				+ match.getReplacementFlight().getID() + "\",\"" + match.getFlight().getID() + "\","
				+ match.getReplacementFlight().getDeparture().getTime() + ","
				+ match.getReplacementFlight().getArrival().getTime() + ","
				+ match.getReplacementFlight().getSrc().getPosition() + ")");
	}

	public void sendMatchRemovedEvent(ConnectingFlightAlternativeMatch match) {
		sendEvent("ConnectingFlightAlternativeMatchRemovedEvent" + "(\"" + match.getTravel().getID() + "\",\""
				+ match.getConnectingFlight().getID() + "\",\"" + match.getConnectingFlight().getID() + "\")");
	}

}
