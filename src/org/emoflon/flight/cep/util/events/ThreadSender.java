package org.emoflon.flight.cep.util.events;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import com.apama.event.Event;
import com.apama.services.event.EventServiceFactory;
import com.apama.services.event.IEventService;

public class ThreadSender extends Thread {
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
