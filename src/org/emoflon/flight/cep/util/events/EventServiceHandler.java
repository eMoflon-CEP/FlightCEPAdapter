package org.emoflon.flight.cep.util.events;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import com.apama.EngineException;
import com.apama.event.Event;
import com.apama.event.parser.EventType;
import com.apama.event.parser.FieldTypes;
import com.apama.services.event.EventServiceException;
import com.apama.services.event.EventServiceFactory;
import com.apama.services.event.IEventService;
import com.apama.services.event.IEventServiceChannel;
import com.apama.services.event.IResponseWrapper;
import com.apama.services.event.ResponseTimeoutException;

import FlightGTCEP.api.matches.ConnectingFlightAlternativeMatch;
import FlightGTCEP.api.matches.FlightMatch;
import FlightGTCEP.api.matches.FlightWithArrivalMatch;
import FlightGTCEP.api.matches.TravelHasConnectingFlightMatch;
import FlightGTCEP.api.matches.TravelWithFlightMatch;

class ThreadSender extends Thread {
	private String host;
	private int port;
	private String eventProcessTyp;
	private Queue<String> eventStrings;
	private boolean open = true;
	private boolean idle = true;

	public ThreadSender(String host, int port, String eventProcessTyp) {
		this.host = host;
		this.port = port;
		this.eventProcessTyp = eventProcessTyp;
		eventStrings = new LinkedBlockingQueue<String>();
	}

	public synchronized void sendEventString(String eventString) {
		eventStrings.add(eventString);
		if (idle) {
			idle = false;
			this.interrupt();
		}
	}

	public synchronized void close() {
		open = false;
		if (idle) {
			idle = false;
			this.interrupt();
		}
	}

	@Override
	public void run() {
		try (
				// get a IEventService instance from the EventServiceFactory
				// IEventService.close() will automatically get called upon exiting the
				// try-with-resources block
				final IEventService eventService = EventServiceFactory.createEventService(host, port,
						eventProcessTyp);) {
			while (open || !eventStrings.isEmpty()) {
				try {
					idle = true;
					Thread.sleep(100000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					// e.printStackTrace();
				}
				idle = false;
				while (!eventStrings.isEmpty()) {
					Event matchEvent = new Event(eventStrings.poll());
					try {
						eventService.sendEvent(matchEvent);
						System.out.println(matchEvent);
						// System.out.println("Thread queue size: "+ eventStrings.size());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}

public class EventServiceHandler {
	public enum ServiceLevel {
		Error,
		Info,
		Test,
		Debug
	}

	private static final String eventProcessTyp = "eMoflonPatternMatch";
	private long eventsSend = 0;
	private ThreadSender sender;
	private ServiceLevel serviceLevel;
	private String host;
	private int port;
	
	static final EventType DELAYEDCF_REQUEST = new EventType("RequestDelayedConnectingFlights",
			FieldTypes.INTEGER.newField(IEventServiceChannel.DEFAULT_MESSAGEID_FIELD_NAME)
			);
	
	static final EventType DELAYEDCF_RESPONSE = new EventType("SendDelayedConnectingFlights",
			FieldTypes.INTEGER.newField(IEventServiceChannel.DEFAULT_MESSAGEID_FIELD_NAME),  // field required for using requestResponse() call
			FieldTypes.INTEGER.newField("delayedConnectingFlights")
			);
	
	static final EventType WORKINGCF_REQUEST = new EventType("RequestWorkingConnectingFlights",
			FieldTypes.INTEGER.newField(IEventServiceChannel.DEFAULT_MESSAGEID_FIELD_NAME)
			);
	
	static final EventType WORKINGCF_RESPONSE = new EventType("SendWorkingConnectingFlights",
			FieldTypes.INTEGER.newField(IEventServiceChannel.DEFAULT_MESSAGEID_FIELD_NAME),  // field required for using requestResponse() call
			FieldTypes.INTEGER.newField("workingConnectingFlights")
			);
	

	public EventServiceHandler(String host, int port, ServiceLevel serviceLevel) {
		sender = new ThreadSender(host, port, eventProcessTyp);
		this.serviceLevel = serviceLevel;
		sender.start();
		this.host = host;
		this.port = port;
		
		if(this.serviceLevel==ServiceLevel.Test) sendServiceLevel();
	}

	public long getEventsSend() {
		return eventsSend;
	}

	public void sendEvent(String eventString) {
		sender.sendEventString(eventString);
		eventsSend++;
	}
	
	public long requestDelayedConnectingFlights() {
		try
		(
			// get a IEventService instance from the EventServiceFactory
			// IEventService.close() will automatically get called upon exiting the try-with-resources block
			IEventService eventService = EventServiceFactory.createEventService(host, port, "RequestResponseChannel");
		) {
			// Create the channel on "eventService.sample.channel"
			String[] channels = {"eMoflonPatternMatch","eMoflonTalkback"};
			IEventServiceChannel ourChannel = eventService.addChannel(channels, null);
			
			// Register the RequestEventType to the channel
			ourChannel.registerEventType(DELAYEDCF_REQUEST);
			
			// Create the requestEvent
			Event requestEvent = new Event("RequestDelayedConnectingFlights(0)");
			try
			(
				// IResponseWrapper.close() will automatically get called upon exiting the try-with-resources block
				// which in turns calls IResponseWrapper.releaseLock (which must be released after a response is received)
				IResponseWrapper responseWrapper = ourChannel.requestResponse(requestEvent, DELAYEDCF_RESPONSE);
			) {
				// print the response event
				System.out.println(responseWrapper.getEvent());
				return (long) responseWrapper.getEvent().getField("delayedConnectingFlights");
				
			} catch (ResponseTimeoutException e) {
				e.printStackTrace();
			} catch (EngineException e) {
				e.printStackTrace();
			} catch (EventServiceException e) {
				e.printStackTrace();
			}
		}
		return 0;
	}
	
	public long requestWorkingConnectingFlights() {
		try
		(
			// get a IEventService instance from the EventServiceFactory
			// IEventService.close() will automatically get called upon exiting the try-with-resources block
			IEventService eventService = EventServiceFactory.createEventService(host, port, "RequestResponseChannel");
		) {
			// Create the channel on "eventService.sample.channel"
			String[] channels = {"eMoflonPatternMatch","eMoflonTalkback"};
			IEventServiceChannel ourChannel = eventService.addChannel(channels, null);
			
			// Register the RequestEventType to the channel
			ourChannel.registerEventType(WORKINGCF_REQUEST);
			
			// Create the requestEvent
			Event requestEvent = new Event("RequestWorkingConnectingFlights(0)");
			try
			(
				// IResponseWrapper.close() will automatically get called upon exiting the try-with-resources block
				// which in turns calls IResponseWrapper.releaseLock (which must be released after a response is received)
				IResponseWrapper responseWrapper = ourChannel.requestResponse(requestEvent, WORKINGCF_RESPONSE);
			) {
				// print the response event
				System.out.println(responseWrapper.getEvent());
				return (long) responseWrapper.getEvent().getField("workingConnectingFlights");
				
			} catch (ResponseTimeoutException e) {
				e.printStackTrace();
			} catch (EngineException e) {
				e.printStackTrace();
			} catch (EventServiceException e) {
				e.printStackTrace();
			}
		}
		return 0;
	}

	public void closeSocket() {
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
		sendEvent("TravelHasConnectingFlightMatchEvent" + "(\"" + match.getTravel().getID() + "\",\""
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
		sendEvent("ConnectingFlightAlternativeMatchEvent" + "(\"" + match.getConnectingFlight().getID() + "\",\""
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
