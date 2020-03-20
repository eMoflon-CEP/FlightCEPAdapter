package org.emoflon.flight.cep.test;

import org.emoflon.flight.cep.util.FlightApamaMonitor;

import flight.monitor.FlightMonitor;
import flight.test.FlightTest;

public class FlightTestApama extends FlightTest {

	@Override
	public FlightMonitor getMonitor() {
		return new FlightApamaMonitor();
	}

}
