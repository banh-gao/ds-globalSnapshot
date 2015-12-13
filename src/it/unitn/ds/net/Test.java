package it.unitn.ds.net;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.Collections;

public class Test {

	public static void main(String[] args) throws Exception {
		NetOverlay ov = new UDPNetOverlay();
		ov.start(1, Collections.singletonMap(1, new InetSocketAddress(Inet4Address.getByName("0.0.0.0"), 8888)));
		ov.sendMessage(1, new NetOverlay.Transfer(123445));

		System.out.println(ov.receiveMessage());
	}
}
