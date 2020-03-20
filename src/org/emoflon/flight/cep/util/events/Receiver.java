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

import flight.monitor.FlightIssueEvent;
import flight.monitor.FlightSolutionEvent;

public class Receiver {
	
	@SuppressWarnings("rawtypes")
	private Map<Long, GraphTransformationMatch> matches;
	@SuppressWarnings("rawtypes")
	private Map<GraphTransformationMatch, FlightIssueEvent<GraphTransformationMatch>> issues;
	@SuppressWarnings("rawtypes")
	private Map<FlightIssueEvent<GraphTransformationMatch>, FlightSolutionEvent<GraphTransformationMatch, GraphTransformationMatch>> solutions;
	private IEventService eventService;
	static final EventType ISSUE = new EventType("Issue",
			FieldTypes.INTEGER.newField("issueMatchID"),
			FieldTypes.STRING.newField("description"));	
	static final EventType REMOVE_ISSUE = new EventType("RemoveIssue",
			FieldTypes.INTEGER.newField("issueMatchID"));
	static final EventType SOLUTION = new EventType("Solution",
			FieldTypes.INTEGER.newField("solutionMatchID"),
			FieldTypes.INTEGER.newField("issueMatchID"),
			FieldTypes.STRING.newField("description"));
	static final EventType REMOVE_SOLUTION = new EventType("RemoveSolution",
			FieldTypes.INTEGER.newField("solutionMatchID"), 
			FieldTypes.INTEGER.newField("issueMatchID"));
	
	public Receiver(String host, int port, String[] channels, String eventProcessTyp, 
			@SuppressWarnings("rawtypes") Map<Long, GraphTransformationMatch> matches,
			@SuppressWarnings("rawtypes") Map<GraphTransformationMatch, FlightIssueEvent<GraphTransformationMatch>> issues,
			@SuppressWarnings("rawtypes") Map<FlightIssueEvent<GraphTransformationMatch>, FlightSolutionEvent<GraphTransformationMatch, GraphTransformationMatch>> solutions) {
		this.matches = matches;
		this.issues = issues;
		this.solutions = solutions;
		eventService = EventServiceFactory.createEventService(host, port, eventProcessTyp);
		IEventServiceChannel listenChannel = eventService.addChannel(channels, null);
		listenChannel.addEventListener(new IssueListener(this.matches, this.issues), ISSUE);
		listenChannel.addEventListener(new RemoveIssueListener(this.matches, this.issues, this.solutions), REMOVE_ISSUE);
		listenChannel.addEventListener(new SolutionListener(this.matches, this.issues, this.solutions), SOLUTION);
		listenChannel.addEventListener(new RemoveSolutionListener(this.matches, this.issues, this.solutions), REMOVE_SOLUTION);
	}
	
	public void terminate() {
		eventService.close();
	}
}

class IssueListener implements IEventListener {
	@SuppressWarnings("rawtypes")
	private Map<Long, GraphTransformationMatch> matches;
	@SuppressWarnings("rawtypes")
	private Map<GraphTransformationMatch, FlightIssueEvent<GraphTransformationMatch>> issues;
	public IssueListener(@SuppressWarnings("rawtypes") Map<Long, GraphTransformationMatch> matches,
			@SuppressWarnings("rawtypes") Map<GraphTransformationMatch, FlightIssueEvent<GraphTransformationMatch>> issues) {
		this.matches = matches;
		this.issues = issues;
	}
	@SuppressWarnings("rawtypes")
	@Override
	public void handleEvent(Event arg0) {
		Long issueMatchID = (long) arg0.getField("issueMatchID");
		GraphTransformationMatch issueMatch = matches.get(issueMatchID);
		String description = (String) arg0.getField("description");
		System.out.println("[Receiver]: " + "Found Issue[" + issueMatchID +"]"  );
		if(issues.containsKey(issueMatch))
			return;
		issues.put(issueMatch, new FlightIssueEvent<GraphTransformationMatch>(issueMatch, description));
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
	@SuppressWarnings("rawtypes")
	private Map<GraphTransformationMatch, FlightIssueEvent<GraphTransformationMatch>> issues;
	@SuppressWarnings("rawtypes")
	private Map<FlightIssueEvent<GraphTransformationMatch>, FlightSolutionEvent<GraphTransformationMatch, GraphTransformationMatch>> solutions;
	public RemoveIssueListener(@SuppressWarnings("rawtypes") Map<Long, GraphTransformationMatch> matches,
			@SuppressWarnings("rawtypes") Map<GraphTransformationMatch, FlightIssueEvent<GraphTransformationMatch>> issues,
			@SuppressWarnings("rawtypes") Map<FlightIssueEvent<GraphTransformationMatch>, FlightSolutionEvent<GraphTransformationMatch, GraphTransformationMatch>> solutions) {
		this.matches = matches;
		this.issues = issues;
		this.solutions = solutions;
	}
	@Override
	@SuppressWarnings("rawtypes")
	public void handleEvent(Event arg0) {
		Long issueMatchID = (long) arg0.getField("issueMatchID");
		System.out.println("[Receiver]: " + "Found Issue[" + issueMatchID +"]"  );
		GraphTransformationMatch issueMatch = matches.get(issueMatchID);
		FlightIssueEvent<GraphTransformationMatch> issue = issues.remove(issueMatch);
		if(issue != null && solutions.containsKey(issue)) {
			solutions.remove(issue);
		}
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
	@SuppressWarnings("rawtypes")
	private Map<GraphTransformationMatch, FlightIssueEvent<GraphTransformationMatch>> issues;
	@SuppressWarnings("rawtypes")
	private Map<FlightIssueEvent<GraphTransformationMatch>, FlightSolutionEvent<GraphTransformationMatch, GraphTransformationMatch>> solutions;
	public SolutionListener(@SuppressWarnings("rawtypes") Map<Long, GraphTransformationMatch> matches,
			@SuppressWarnings("rawtypes") Map<GraphTransformationMatch, FlightIssueEvent<GraphTransformationMatch>> issues,
			@SuppressWarnings("rawtypes") Map<FlightIssueEvent<GraphTransformationMatch>, FlightSolutionEvent<GraphTransformationMatch, GraphTransformationMatch>> solutions) {
		this.issues = issues;
		this.solutions = solutions;
		this.matches = matches;
	}
	@SuppressWarnings("rawtypes")
	@Override
	public void handleEvent(Event arg0) {
		Long solutionMatchID = (long) arg0.getField("solutionMatchID");
		Long issueMatchID = (long) arg0.getField("issueMatchID");
		String description = (String) arg0.getField("description");
		
		System.out.println("[Receiver]: " + "Found Solution[" + solutionMatchID +"] for Issue[" + issueMatchID + "]"  );
		
		GraphTransformationMatch solutionMatch = matches.get(solutionMatchID);
		GraphTransformationMatch issueMatch = matches.get(issueMatchID);
		FlightIssueEvent<GraphTransformationMatch> issue = issues.get(issueMatch);
		
		if(issue == null)
			return;
		if(solutions.containsKey(issue))
			return;
		
		solutions.put(issue, new FlightSolutionEvent<GraphTransformationMatch, GraphTransformationMatch>(issueMatch, solutionMatch, description));
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
	@SuppressWarnings("rawtypes")
	private Map<GraphTransformationMatch, FlightIssueEvent<GraphTransformationMatch>> issues;
	@SuppressWarnings("rawtypes")
	private Map<FlightIssueEvent<GraphTransformationMatch>, FlightSolutionEvent<GraphTransformationMatch, GraphTransformationMatch>> solutions;
	public RemoveSolutionListener(@SuppressWarnings("rawtypes") Map<Long, GraphTransformationMatch> matches,
			@SuppressWarnings("rawtypes") Map<GraphTransformationMatch, FlightIssueEvent<GraphTransformationMatch>> issues,
			@SuppressWarnings("rawtypes") Map<FlightIssueEvent<GraphTransformationMatch>, FlightSolutionEvent<GraphTransformationMatch, GraphTransformationMatch>> solutions) {
		this.issues = issues;
		this.solutions = solutions;
		this.matches = matches;;
	}
	@SuppressWarnings("rawtypes")
	@Override
	public void handleEvent(Event arg0) {;
		Long matchID = (long) arg0.getField("issueMatchID");
		GraphTransformationMatch issueMatch = matches.get(matchID);
		System.out.println("[Receiver]: " +"Remove Solution[" + matchID + "]" );
		FlightIssueEvent<GraphTransformationMatch> issue = issues.get(issueMatch);
		if(issue != null && solutions.containsKey(issue)) {
			solutions.remove(issue);
		}
	}
	@Override
	public void handleEvents(Event[] arg0) {
		for(Event e: arg0) {
			handleEvent(e);
		}
	}
}