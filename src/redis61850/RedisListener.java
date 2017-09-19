/*
 Code for connecting Redis and OpenIEC61850.

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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.openmuc.openiec61850.BasicDataAttribute;
import org.openmuc.openiec61850.BdaDoubleBitPos;
import org.openmuc.openiec61850.BdaFloat32;
import org.openmuc.openiec61850.BdaFloat64;
import org.openmuc.openiec61850.BdaInt16;
import org.openmuc.openiec61850.BdaInt16U;
import org.openmuc.openiec61850.BdaInt32;
import org.openmuc.openiec61850.BdaInt32U;
import org.openmuc.openiec61850.BdaInt64;
import org.openmuc.openiec61850.BdaInt8;
import org.openmuc.openiec61850.BdaInt8U;
import org.openmuc.openiec61850.Fc;
import org.openmuc.openiec61850.ServerEventListener;
import org.openmuc.openiec61850.ServerModel;
import org.openmuc.openiec61850.ServerSap;
import org.openmuc.openiec61850.ServiceError;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

public class RedisListener implements Runnable, ServerEventListener {
	private JedisPool jpool;
	private ServerSap serverSap;
	private HashMap<String, BasicDataAttribute> redisToMms;
	private HashMap<String, String> mmsToRedis;
	private Properties p;

	public RedisListener(ServerSap serverSap, Properties p) {
		String hostString = p.getProperty("REDIS_HOST");
		String portString = p.getProperty("REDIS_PORT");
		this.p = p;
		jpool = new JedisPool(hostString, Integer.parseInt(portString));
		this.serverSap = serverSap;
		// read data mapping file
		this.redisToMms = new HashMap<String, BasicDataAttribute>();
		this.mmsToRedis = new HashMap<String, String>();
		readDatamap();
	}

	public void run() {
		// subscribe to events from local Redis instance
		try {
			psubscribe(new RedisSubscriber(this), "__keyspace@0__:*");
		} catch (Exception e) {
			System.out.println("Error connecting to Redis: " + e.getMessage());
			System.exit(1);
		}
	}

	public void stop() {
		jpool.destroy();
	}

	public void psubscribe(JedisPubSub resource, String pattern) {
		Jedis jedis = jpool.getResource();
		jedis.psubscribe(resource, pattern);
	}

	@Override
	public void serverStoppedListening(ServerSap serverSap) {
		System.out.println("The SAP stopped listening");
	}

	@Override
	public List<ServiceError> write(List<BasicDataAttribute> bdas) {
		Jedis jedis = jpool.getResource();
		for (BasicDataAttribute bda : bdas) {
			System.out.println("got a write request: " + bda);
			// write to Redis
			String redisKey = mmsToRedis.get(bda.getReference().toString());
			System.out.println("Redis key: " + redisKey);
			if (redisKey == null) {
				continue;
			}
			try {
				set(jedis, redisKey, getBdaValue(bda));
			} catch (IllegalArgumentException e) {
				System.out.println(e.getMessage());
				continue;
			}
		}
		return null;
	}

	private void readDatamap() {
		BufferedReader br = null;
		FileReader fr = null;
		try {
			fr = new FileReader(p.getProperty("IEC61850_DATAMAP"));
			br = new BufferedReader(fr);
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.trim();
				if (sCurrentLine.startsWith("#") == true) {
					continue;
				}
				String[] fields = sCurrentLine.split("\t");
				// see if we must read from Redis
				if (fields.length >= 3 && fields[2].startsWith("\"<<<")) {
					parseLine(fields);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null) {
					br.close();
				}
				if (fr != null) {
					fr.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	private void parseLine(String[] fields) {
		String ld = fields[0];
		String var61850 = fields[1];
		String varRedis = fields[2].substring(4, fields[2].length() - 1);
		String[] var61850fields = var61850.split("\\$");
		if (var61850fields.length >= 3) {
			String fcString = var61850fields[1];
			String var61850Path = "";
			for (int i = 0; i< var61850fields.length; i++) {
				String var61850field = var61850fields[i];
				if (i == 1) {
					continue;
				}
				var61850Path = var61850Path + var61850field;
				if (i < var61850fields.length - 1) {
					var61850Path = var61850Path + ".";
				}
			}
			var61850Path = ld + "/" + var61850Path;
			// Ok get the BDA
			ServerModel model = serverSap.getModelCopy();
			BasicDataAttribute bda = (BasicDataAttribute) model.findModelNode(var61850Path,
					Fc.fromString(fcString));
			// fill the datamap
			redisToMms.put(varRedis, bda);
			mmsToRedis.put(bda.getReference().toString(), varRedis);
		}
	}

	// TODO: handle more types (timestamps, booleans...)
	private String getBdaValue(BasicDataAttribute bda) {
		String valueString;
		if (bda instanceof BdaFloat32) {
			valueString = ((BdaFloat32) bda).getFloat() + "";
		}
		else if (bda instanceof BdaFloat64) {
			valueString = ((BdaFloat64) bda).getDouble() + "";
		}
		else if (bda instanceof BdaInt8) {
			valueString = ((BdaInt8) bda).getValue() + "";
		}
		else if (bda instanceof BdaInt8U) {
			valueString = ((BdaInt8U) bda).getValue() + "";
		}
		else if (bda instanceof BdaInt16) {
			valueString = ((BdaInt16) bda).getValue() + "";
		}
		else if (bda instanceof BdaInt16U) {
			valueString = ((BdaInt16U) bda).getValue() + "";
		}
		else if (bda instanceof BdaInt32) {
			valueString = ((BdaInt32) bda).getValue() + "";
		}
		else if (bda instanceof BdaInt32U) {
			valueString = ((BdaInt32U) bda).getValue() + "";
		}
		else if (bda instanceof BdaInt64) {
			valueString = ((BdaInt64) bda).getValue() + "";
		}
		else if (bda instanceof BdaDoubleBitPos) {
			byte[] value = ((BdaDoubleBitPos) bda).getValue(); 
			valueString = "0";
		        if ((value[0] & 0x80) == 0x80) {
		        	valueString = "1"; // ON
		        }
		        if ((value[0] & 0x40) == 0x40) {
		        	valueString = "0"; // OFF
		        }
		}
		else {
			throw new IllegalArgumentException();
		}
		return valueString;
	}

	protected JedisPool getPool() {
		return jpool;
	}

	protected ServerSap getServerSap() {
		return serverSap;
	}

	protected HashMap<String, BasicDataAttribute> getRedisToMms() {
		return redisToMms;
	}

	protected String get(Jedis j, String tag) {
		String value = null;
		if (isHash(tag) == false) {
			value = j.get(tag);
		} else {
			String[] hash = tag.split("/");
			String hashName = hash[0];
			String hashField = hash[1];
			value = j.hget(hashName, hashField);
		}
		return value;
	}

	protected void set(Jedis j, String tag, String value) {
		if (isHash(tag) == false) {
			j.set(tag, value);
		} else {
			String[] hash = tag.split("/");
			String hashName = hash[0];
			String hashField = hash[1];
			j.hset(hashName, hashField, value);
		}
		return;
	}

	protected boolean isHash(String tag) {
		return tag.contains("/");
	}
}
