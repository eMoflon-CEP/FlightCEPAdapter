package org.emoflon.flight.cep.util.events;

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

public class Requester {
	private String[] channels = {"eMoflonPatternMatch","eMoflonTalkback"};
	private String host;
	private int port;
	private String processName;
	
	public Requester(String host, int port, String[] channels, String processName) {
		this.host = host;
		this.port = port;
		this.channels = channels;
		this.processName = processName;
	}
	
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
	
	
	private Event request(Event requestEvent, EventType requestType, EventType responseType) {
		try
		(
			// get a IEventService instance from the EventServiceFactory
			// IEventService.close() will automatically get called upon exiting the try-with-resources block
			IEventService eventService = EventServiceFactory.createEventService(host, port, processName);
		) {
			// Create the channel on "eventService.sample.channel"
			IEventServiceChannel requestChannel = eventService.addChannel(channels, null);
			
			// Register the RequestEventType to the channel
			requestChannel.registerEventType(requestType);
	
			try
			(
				// IResponseWrapper.close() will automatically get called upon exiting the try-with-resources block
				// which in turns calls IResponseWrapper.releaseLock (which must be released after a response is received)
				IResponseWrapper responseWrapper = requestChannel.requestResponse(requestEvent, responseType);
			) {
				// print the response event
				System.out.println("[Requester]: " +responseWrapper.getEvent());
				return responseWrapper.getEvent();	
			} catch (ResponseTimeoutException e) {
				e.printStackTrace();
			} catch (EngineException e) {
				e.printStackTrace();
			} catch (EventServiceException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	public long requestDelayedConnectingFlights() {
		Event requestEvent = new Event("RequestDelayedConnectingFlights(0)");
		return (long)request(requestEvent, DELAYEDCF_REQUEST, DELAYEDCF_RESPONSE).getField("delayedConnectingFlights");
	}
	public long requestWorkingConnectingFlights() {
		Event requestEvent = new Event("RequestWorkingConnectingFlights(0)");
		return (long)request(requestEvent,WORKINGCF_REQUEST,WORKINGCF_RESPONSE).getField("workingConnectingFlights");
	}
}
