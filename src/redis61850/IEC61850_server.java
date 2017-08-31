/*
 Example IEC 61850 server backed by Redis.

 Copyright 2017 Daniele Pala <daniele.pala@rse-web.it>

 This file is part of openIEC61850-redis.

 openIEC61850-redis is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 openIEC61850-redis is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with openIEC61850-redis. If not, see <http://www.gnu.org/licenses/>.
*/

package redis61850;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.openmuc.openiec61850.SclParseException;
import org.openmuc.openiec61850.ServerSap;

public class IEC61850_server {
	public static void main(String[] args) {
		// try to read config properties
		Properties p = new Properties();
		try {
			String cfg = "configuration/config.properties";
			p.load(new FileInputStream(cfg));
		} catch (FileNotFoundException e) {
			System.out.println("Configuration file not found! " +
					e.getMessage());
			System.exit(1);
		} catch (IOException e) {
			System.out.println("I/O error!");
			System.exit(1);
		}
		List<ServerSap> serverSaps = null;
		try {
			serverSaps = ServerSap.getSapsFromSclFile(p.getProperty("IEC61850_MODEL"));
		} catch (SclParseException e) {
			System.out.println("Error parsing SCL/ICD file: " + e.getMessage());
			return;
		}
		ServerSap serverSap = serverSaps.get(0);
		String portString = p.getProperty("IEC61850_PORT");
		serverSap.setPort(Integer.parseInt(portString));
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				if (serverSap != null) {
					serverSap.stop();
				}
				System.out.println("Server was stopped.");
			}
		});
		// start listening from Redis       
		RedisListener r = new RedisListener(serverSap, p);
		new Thread(r).start();
		try {
			serverSap.startListening(r);
		} catch (IOException e) {
			System.out.println("Error startng IEC61850 server: " + e.getMessage());
			System.exit(1);
		}
	}
}
