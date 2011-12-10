package org.apache.hcatalog.templeton.test;

import static org.junit.Assert.*;

import org.apache.hcatalog.templeton.CallbackFailedException;
import org.apache.hcatalog.templeton.ServerCallback;
import org.junit.Test;

public class ServerCallbackTest {

	@Test
	public void test() {
		ServerCallback.registerCallback("fred", "http://apache.org");
		ServerCallback.registerCallback("joe", "http://localhost:2345");
		ServerCallback.registerCallback("wilma", "http://localhost:2345/foo/bar");
		ServerCallback.registerCallback("betty", "http://localhost:2345/foo/bar?a=something&b=somethingelse");
		try {
			ServerCallback.doCallback("fred");
			assertTrue(true); // We made it.
		} catch (CallbackFailedException e) {
			fail("Callback failed: " + e.getMessage());
		}
		try {
			ServerCallback.doCallback("joe");
			fail("Didn't call server.");
		} catch (CallbackFailedException e) {
			assertTrue(e.getMessage().indexOf("http://localhost:2345?jobid=joe") > -1);
		}
		try {
			ServerCallback.doCallback("wilma");
			fail("Didn't call server.");
		} catch (CallbackFailedException e) {
			assertTrue(e.getMessage().indexOf("http://localhost:2345/foo/bar?jobid=wilma") > -1);
		}
		try {
			ServerCallback.doCallback("betty");
			fail("Didn't call server.");
		} catch (CallbackFailedException e) {
			assertTrue(e.getMessage().indexOf
					("http://localhost:2345/foo/bar?a=something&b=somethingelse&jobid=betty") > -1);
		}
	}

}
