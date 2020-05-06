package org.emoflon.flight.cep.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.apama.EngineException;
import com.apama.engine.EngineStatus;
import com.apama.engine.beans.EngineClientFactory;
import com.apama.engine.beans.interfaces.EngineClientInterface;


public class ApamaAPITest {

	public static void main(String[] args) {
		
		ProcessThread tr = new ProcessThread("C:\\SoftwareAG\\Apama\\bin\\correlator.exe");
		
		tr.start();
		
		EngineClientInterface engineClient = initClient("localhost", 15903, "ApamaAPITest");
		if(engineClient != null) {
			try {
				engineClient.connectNow();
				EngineStatus status = engineClient.getRemoteStatus();
				System.out.println(status);
			} catch (EngineException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}else {
			System.out.println("Could not initialize EngineClientInterface.");
		}
			
		
		
		tr.close();
		
		try {
			tr.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static EngineClientInterface initClient(String host, int port, String pid) {
		try {
			return EngineClientFactory.createEngineClient(host, port, pid);
		} catch (EngineException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
}


class ProcessThread extends Thread {
	private Process pr = null;
	private String execPath;
	private boolean running = true;
	
	public ProcessThread(String execPath) {
		this.execPath = execPath;
	}
	@Override
	public void run() {
		Runtime runtime = Runtime.getRuntime();     //getting Runtime object
		try
		{
			pr = runtime.exec(execPath);        //opens new notepad instance
			System.out.println("Running: " + pr.toString()+" status: "+pr.isAlive());
			
			BufferedReader stdInput = new BufferedReader(new 
				     InputStreamReader(pr.getInputStream()));

				BufferedReader stdError = new BufferedReader(new 
				     InputStreamReader(pr.getErrorStream()));
				while(running) {
					// Read the output from the command
					System.out.println("***APAMA-PROCRESS Output:\n");
					String s = null;
					while ((s = stdInput.readLine()) != null) {
					    System.out.println(s);
					}

					// Read any errors from the attempted command
					System.out.println("***APAMA-PROCRESS Errors:\n");
					while ((s = stdError.readLine()) != null) {
					    System.out.println(s);
					}
				}
				
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public void close() {
		System.out.println("Killing: "+pr.toString()+" status: "+pr.isAlive());
		pr.destroy();
		System.out.println("Exited with: "+pr.exitValue());
		running = false;
	}
}
