package org.emoflon.flight.cep.util.events;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import FlightGTCEP.api.matches.FlightMatch;
import FlightGTCEP.api.matches.PlaneMatch;
import FlightGTCEP.api.matches.TravelHasConnectingFlightMatch;

import com.apama.event.Event;
import com.apama.services.event.EventServiceFactory;
import com.apama.services.event.IEventService;

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
			while (open) {
				try {
					idle = true;
					Thread.sleep(100000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					// e.printStackTrace();
				}
				while (!eventStrings.isEmpty()) {
					Event matchEvent = new Event(eventStrings.poll());
					try {
						eventService.sendEvent(matchEvent);
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
	private static final String eventProcessTyp = "eMoflonPatternMatch";
	private long eventsSend = 0;
	private ThreadSender sender;

	public EventServiceHandler(String host, int port) {
		sender = new ThreadSender(host, port, eventProcessTyp);
		sender.start();
	}
	
	public long getEventsSend() {
		return eventsSend;
	}
	public void sendEvent(String eventString) {
		sender.sendEventString(eventString);
		eventsSend++;
	}
	public void closeSocket() {
		sender.close();
		try {
			sender.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void sendMatchEvent(FlightMatch match) {
		sendEvent(
				"FlightMatchEvent" + "(\"" + match.getFlight().getID() + "\"," + match.getFlight().getArrival()
						+ "," + match.getFlight().getDeparture() + "," + match.getRoute().getDuration() + "," +match.getPlane().getID() + "\")");
	}
	public void sendMatchRemovedEvent(FlightMatch match) {
		sendEvent("FlightRemovedEvent" + "(\"" + match.getFlight().getID() + "\")");
	}
	public void sendMatchEvent(PlaneMatch match) {
		sendEvent("PlaneMatchEvent" + "(\"" + match.getPlane().getID() + "\","
				+ match.getPlane().getCapacity() + ")");
	}
	public void sendMatchRemovedEvent(PlaneMatch match) {
		sendEvent("PlaneRemovedEvent" + "(\"" + match.getPlane().getID() + "\")");
	}
	public void sendMatchEvent(TravelHasConnectingFlightMatch match) {
		sendEvent("TravelHasConnectingFlightMatchEvent" + "(\"" + match.getTravel().getID() + "\",\""
				+ match.getConnectingFlight().getID() + "\",\"" + match.getFlight().getID() + "\",\"" + match.getConnectingFlight().getDeparture() + ","
				+ match.getFlight().getArrival() + "," + match.getArrivalGate().getPosition() + ","
				+ match.getDepartingGate().getPosition() + "," + match.getTransitAirport().getSize() + ")");
	}
	public void sendMatchRemovedEvent(TravelHasConnectingFlightMatch match) {
		sendEvent("TravelHasConnectingFlightMatchRemovedEvent" + "(\"" + match.getTravel().getID() + "\",\""
				+ match.getConnectingFlight().getID() + "\")");
	}

}
