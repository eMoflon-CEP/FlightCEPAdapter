package org.emoflon.flight.cep.util.events;

import java.util.Map;

import org.emoflon.ibex.gt.api.GraphTransformationMatch;

import com.apama.event.Event;
import com.apama.event.IEventListener;
import com.apama.event.parser.EventType;
import com.apama.event.parser.FieldTypes;
import com.apama.services.event.EventServiceFactory;
import com.apama.services.event.IEventService;
import com.apama.services.event.IEventServiceChannel;

public class Receiver {
	
	@SuppressWarnings("rawtypes")
	private Map<Long, GraphTransformationMatch> matches;
	static final EventType ISSUE = new EventType("Issue",
			FieldTypes.INTEGER.newField("issueMatchID"),
			FieldTypes.STRING.newField("description"));	
	static final EventType REMOVE_ISSUE = new EventType("RemoveIssue",
			FieldTypes.INTEGER.newField("issueMatchID"));
	static final EventType SOLUTION = new EventType("Solution",
			FieldTypes.INTEGER.newField("solutionMatchID"),
			FieldTypes.INTEGER.newField("issueMatchID"));
	static final EventType REMOVE_SOLUTION = new EventType("RemoveSolution",
			FieldTypes.INTEGER.newField("solutionMatchID"), 
			FieldTypes.INTEGER.newField("issueMatchID"));
	
	public Receiver(String host, int port, String[] channels, String eventProcessTyp, 
			@SuppressWarnings("rawtypes") Map<Long, GraphTransformationMatch> matches) {
		this.matches = matches;
		IEventService eventService = EventServiceFactory.createEventService(host, port, eventProcessTyp);
		IEventServiceChannel listenChannel = eventService.addChannel(channels, null);
		listenChannel.addEventListener(new IssueListener(matches), ISSUE);
		listenChannel.addEventListener(new RemoveIssueListener(matches), REMOVE_ISSUE);
	}
}

class IssueListener implements IEventListener {
	@SuppressWarnings("rawtypes")
	private Map<Long, GraphTransformationMatch> matches;
	public IssueListener(@SuppressWarnings("rawtypes") Map<Long, GraphTransformationMatch> matches) {
		this.matches = matches;
	}
	@Override
	public void handleEvent(Event arg0) {
		Long issueMatchID = (long) arg0.getField("issueMatchID");
		//@SuppressWarnings("rawtypes")
		//GraphTransformationMatch issueMatch = matches.get(issueMatchID);
		String description = (String) arg0.getField("description");
		System.out.println("Found Issue[" + issueMatchID +"]"  );
		// TODO ISSUE CREATE
	}
	@Override
	public void handleEvents(Event[] arg0) {
		for(Event e: arg0) {
			handleEvent(e);
		}
	}
}

class RemoveIssueListener implements IEventListener {
	@SuppressWarnings("rawtypes")
	private Map<Long, GraphTransformationMatch> matches;
	public RemoveIssueListener(@SuppressWarnings("rawtypes") Map<Long, GraphTransformationMatch> matches) {
		this.matches = matches;
	}
	@Override
	public void handleEvent(Event arg0) {
		Long issueMatchID = (long) arg0.getField("issueMatchID");
		System.out.println("Found Issue[" + issueMatchID +"]"  );
		//@SuppressWarnings("rawtypes")
		// GraphTransformationMatch issueMatch = matches.get(issueMatchID);
		// TODO ISSUE REMOVE
	}
	@Override
	public void handleEvents(Event[] arg0) {
		for(Event e: arg0) {
			handleEvent(e);
		}
	}
}

class SolutionListener implements IEventListener {
	@SuppressWarnings("rawtypes")
	private Map<Long, GraphTransformationMatch> matches;
	public SolutionListener(@SuppressWarnings("rawtypes") Map<Long, GraphTransformationMatch> matches) {
		this.matches = matches;
	}
	@Override
	public void handleEvent(Event arg0) {
		Long solutionMatchID = (long) arg0.getField("solutionMatchID");
		Long issueMatchID = (long) arg0.getField("issueMatchID");
		@SuppressWarnings("rawtypes")
		GraphTransformationMatch solutionMatch = matches.get(solutionMatchID);
		@SuppressWarnings("rawtypes")
		GraphTransformationMatch issueMatch = matches.get(issueMatchID);
		// TODO ISSUE CREATE
	}
	@Override
	public void handleEvents(Event[] arg0) {
		for(Event e: arg0) {
			handleEvent(e);
		}
	}
}

class RemoveSolutionListener implements IEventListener {
	@SuppressWarnings("rawtypes")
	private Map<Long, GraphTransformationMatch> matches;
	public RemoveSolutionListener(@SuppressWarnings("rawtypes") Map<Long, GraphTransformationMatch> matches) {
		this.matches = matches;
	}
	@Override
	public void handleEvent(Event arg0) {
		Long solutionMatchID = (long) arg0.getField("solutionMatchID");
		Long matchID = (long) arg0.getField("issueMatchID");
		@SuppressWarnings("rawtypes")
		GraphTransformationMatch solutionMatch = matches.get(solutionMatchID);
		@SuppressWarnings("rawtypes")
		GraphTransformationMatch issueMatch = matches.get(matchID);
		// TODO ISSUE REMOVE
	}
	@Override
	public void handleEvents(Event[] arg0) {
		for(Event e: arg0) {
			handleEvent(e);
		}
	}
}