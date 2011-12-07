package org.apache.hcatalog.templeton.test.tool;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hcatalog.templeton.tool.TrivialExecService;
import org.junit.Test;

public class TrivialExecServiceTest {

	@Test
	public void test() {
		ArrayList<String> list = new ArrayList<String>();
		list.add("echo");
		list.add("success");
		BufferedReader out = null;
		BufferedReader err = null;
		try {
			Process process = TrivialExecService.getInstance().run(list);
			out = new BufferedReader(new InputStreamReader(
					process.getInputStream()));
			err = new BufferedReader(new InputStreamReader(
					process.getErrorStream()));
			assertEquals("success", out.readLine());
			out.close();
			String line;
			while ((line = err.readLine()) != null) {
				fail(line);
			}
			process.waitFor();
		} catch (Exception e) {
			e.printStackTrace();
			fail("Process caused exception.");
		} finally {
			try {
				out.close();
			} catch (Exception ex) {
				// Whatever.
			}
			try {
				err.close();
			} catch (Exception ex) {
				// Whatever
			}
		}
	}

}
