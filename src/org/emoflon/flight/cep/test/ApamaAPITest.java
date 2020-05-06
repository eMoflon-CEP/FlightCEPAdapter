package org.emoflon.flight.cep.test;

import java.io.IOException;

import com.apama.epl.plugin.Correlator;

public class ApamaAPITest {

	public static void main(String[] args) {
		
		ProcessThread tr = new ProcessThread("C:\\SoftwareAG\\Apama\\bin\\correlator.exe");
		
		tr.start();
		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		tr.close();
		
		try {
			tr.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}

class ProcessThread extends Thread {
	private Process pr = null;
	private String execPath;
	
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
	}
}
