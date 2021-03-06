/*
 Redis events subscriber.

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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.openmuc.openiec61850.BdaQuality;
import org.openmuc.openiec61850.BdaTimestamp;
import org.openmuc.openiec61850.ServerSap;
import org.openmuc.openiec61850.BdaQuality.Validity;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class RedisSubscriber extends JedisPubSub {
	private Jedis jedis;
	private ServerSap serverSap;
	private HashMap<String, BasicDataAttribute> redisToMms;
	private RedisListener listener;

	public RedisSubscriber(RedisListener listener) {
		this.listener = listener;
		this.jedis = listener.getPool().getResource();
		this.serverSap = listener.getServerSap();
		this.redisToMms = listener.getRedisToMms();
		// Enable key-event notifications for strings and hashes 
		jedis.configSet("notify-keyspace-events", "K$h");
		// Read initial values from Redis
		for (String varRedis: redisToMms.keySet()) {
			String value = listener.get(jedis, varRedis);
			if (value == null) {
				continue;
			}
			BasicDataAttribute bda = redisToMms.get(varRedis);
			try {
				setBdaValue(bda, value);
			} catch (Exception e) {
				System.out.println("Setting: " + bda + " with value: " + value);
				System.out.println("The application does not support writing this type of basic data attribute.");
				continue;
			}
		}
	}

	public void onMessage(String channel, String message) {
	}

	public void onPMessage(String pattern, String channel, String message) {
		if (message.equals("set")) {
			String varRedis = channel.split(":")[1];
			String value = listener.get(jedis, varRedis);
			BasicDataAttribute bda = redisToMms.get(varRedis);
			if (bda == null) {
				return;
			}
			try {
				setBdaValue(bda, value);
			} catch (Exception e) {
				System.out.println("Setting: " + bda + " with value: " + value);
				System.out.println("The application does not support writing this type of basic data attribute.");
				return;
			}
		}
		if (message.equals("hset")) {
			String varRedis = channel.split(":", 2)[1];
			Map<String, String> values = jedis.hgetAll(varRedis);
			for (String hashField: values.keySet()) {
				String value = values.get(hashField);
				BasicDataAttribute bda = redisToMms.get(varRedis + "/" + hashField);
				if (bda == null) {
					continue;
				}
				try {
					setBdaValue(bda, value);
				} catch (Exception e) {
					System.out.println("Setting: " + bda + " with value: " + value);
					System.out.println("The application does not support writing this type of basic data attribute.");
					continue;
				}
			}	
		}
	}	

	public void onSubscribe(String channel, int subscribedChannels) {	
	}

	public void onUnsubscribe(String channel, int subscribedChannels) {
	}

	public void onPUnsubscribe(String pattern, int subscribedChannels) {
	}

	public void onPSubscribe(String pattern, int subscribedChannels) {	
	}

	// TODO: handle more types (booleans...)
	private void setBdaValue(BasicDataAttribute bda, String valueString) {
		if (bda instanceof BdaFloat32) {
			float value = Float.parseFloat(valueString);
			((BdaFloat32) bda).setFloat(value);
		}
		else if (bda instanceof BdaFloat64) {
			double value = Float.parseFloat(valueString);
			((BdaFloat64) bda).setDouble(value);
		}
		else if (bda instanceof BdaInt8) {
			byte value = Byte.parseByte(valueString);
			((BdaInt8) bda).setValue(value);
		}
		else if (bda instanceof BdaInt8U) {
			short value = Short.parseShort(valueString);
			((BdaInt8U) bda).setValue(value);
		}
		else if (bda instanceof BdaInt16) {
			short value = Short.parseShort(valueString);
			((BdaInt16) bda).setValue(value);
		}
		else if (bda instanceof BdaInt16U) {
			int value = Integer.parseInt(valueString);
			((BdaInt16U) bda).setValue(value);
		}
		else if (bda instanceof BdaInt32) {
			int value = Integer.parseInt(valueString);
			((BdaInt32) bda).setValue(value);
		}
		else if (bda instanceof BdaInt32U) {
			long value = Long.parseLong(valueString);
			((BdaInt32U) bda).setValue(value);
		}
		else if (bda instanceof BdaInt64) {
			long value = Long.parseLong(valueString);
			((BdaInt64) bda).setValue(value);
		}
		else if (bda instanceof BdaDoubleBitPos) {
			byte[] value = new byte[] { (byte) 0xC0 }; // BAD_STATE
			if (valueString.equals("0")) {
				value[0] = 0x40; // OFF
			}
			if (valueString.equals("1")) {
				value[0] = (byte) 0x80; // ON
			}
			((BdaDoubleBitPos) bda).setValue(value);
		}
		else if (bda instanceof BdaTimestamp) {
			long time = Long.parseLong(valueString);
			Date d = new java.util.Date(time);
			((BdaTimestamp) bda).setDate(d);
		}
		else if (bda instanceof BdaQuality) {
			Validity v = Validity.INVALID;
			if (valueString.equals("0")) {
				v = Validity.INVALID;
			}
			if (valueString.equals("1")) {
				v = Validity.GOOD;
			}
			((BdaQuality) bda).setValidity(v);
		}
		else {
			throw new IllegalArgumentException();
		}
		List<BasicDataAttribute> bdas = new ArrayList<>();
		bdas.add(bda);
		serverSap.setValues(bdas);
	}
}