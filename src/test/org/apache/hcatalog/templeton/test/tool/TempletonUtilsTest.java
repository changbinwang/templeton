package org.apache.hcatalog.templeton.test.tool;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hcatalog.templeton.tool.TempletonUtils;
import org.junit.Test;

public class TempletonUtilsTest {

	@Test
	public void testIssetString() {
		assertFalse(TempletonUtils.isset((String)null));
		assertFalse(TempletonUtils.isset(""));
		assertTrue(TempletonUtils.isset("hello"));
	}

	@Test
	public void testIssetTArray() {
		assertFalse(TempletonUtils.isset((Long[]) null));
		assertFalse(TempletonUtils.isset(new String[0]));
		String[] parts = new String("hello.world").split("\\.");
		assertTrue(TempletonUtils.isset(parts));
	}

	@Test
	public void testPrintTaggedJobID() {
		//JobID job = new JobID();
		// TODO -- capture System.out?
	}

	@Test
	public void testExtractJobId() {
		assertEquals(null, TempletonUtils.extractJobId("fred"));
		assertEquals("fred", TempletonUtils.extractJobId
				(TempletonUtils.JOB_ID_TAG + "fred"));
	}

	@Test
	public void testEncodeCliArg() {
		assertEquals("*", TempletonUtils.encodeCliArg(null));
		assertEquals("*fred", TempletonUtils.encodeCliArg("fred"));
	}

	@Test
	public void testDecodeCliArg() {
		assertEquals(null, TempletonUtils.decodeCliArg(null));
		assertEquals("", TempletonUtils.decodeCliArg(""));
		assertEquals("fred", TempletonUtils.decodeCliArg(
				TempletonUtils.encodeCliArg("fred")));
	}

	@Test
	public void testEncodeArray() {
		assertEquals(null, TempletonUtils.encodeArray(null));
		String[] tmp = new String[0];
		assertTrue(TempletonUtils.encodeArray(new String[0]).length() == 0);
		tmp = new String[3];
		tmp[0] = "fred";
		tmp[1] = null;
		tmp[2] = "peter,lisa,, barney";
		assertEquals("fred,,peter" +
				StringUtils.ESCAPE_CHAR + ",lisa" + StringUtils.ESCAPE_CHAR + "," +
				StringUtils.ESCAPE_CHAR + ", barney", TempletonUtils.encodeArray(tmp));
	}

	@Test
	public void testDecodeArray() {
		assertTrue(TempletonUtils.encodeArray(null) == null);
		String[] tmp = new String[3];
		tmp[0] = "fred";
		tmp[1] = null;
		tmp[2] = "peter,lisa,, barney";
		String[] tmp2 = TempletonUtils.decodeArray(TempletonUtils.encodeArray(tmp));
		try {
			for (int i=0; i< tmp.length; i++) {
				assertEquals((String) tmp[i], (String)tmp2[i]);
			}
		} catch (Exception e) {
			fail("Arrays were not equal" + e.getMessage());
		}
	}

	@Test
	public void testEncodeCliArrayStringArray() {
		assertEquals("*", TempletonUtils.encodeCliArray((String[]) null));
		// These have both been tested earlier in the test suite, so just
		// test that this does what it says it does.
		String[] tmp = new String[3];
		tmp[0] = "fred";
		tmp[1] = null;
		tmp[2] = "peter,lisa,, barney";
		assertEquals(TempletonUtils.encodeCliArg(TempletonUtils.encodeArray(tmp)),
				TempletonUtils.encodeCliArray(tmp));
	}

	@Test
	public void testEncodeCliArrayListOfString() {
		assertEquals(null, TempletonUtils.encodeCliArray((List<String>) null));
		ArrayList<String> tmplist = new ArrayList<String>();
		tmplist.add("fred");
		tmplist.add(null);
		tmplist.add("peter,lisa,, barney");
		String[] tmp = new String[3];
		tmp[0] = "fred";
		tmp[1] = null;
		tmp[2] = "peter,lisa,, barney";
		assertEquals(TempletonUtils.encodeCliArray(tmp), 
				TempletonUtils.encodeCliArray(tmplist));
		
	}

	@Test
	public void testEncodeCliArrayString() {
		assertEquals(null, TempletonUtils.encodeCliArray((String) null));
		assertEquals("*fred" + StringUtils.ESCAPE_CHAR + ",joe", 
				TempletonUtils.encodeCliArray("fred,joe"));
	}

	@Test
	public void testDecodeCliArray() {
		String[] tmp = new String[3];
		tmp[0] = "fred";
		tmp[1] = null;
		tmp[2] = "peter,lisa,, barney";
		String[] tmp2 = TempletonUtils.decodeCliArray(TempletonUtils.encodeCliArray(tmp));
		try {
			for (int i=0; i<tmp.length; i++) {
				assertEquals(tmp[i], tmp2[i]);
			}
		} catch (Exception e) {
			fail("DecodeCliArray: " + e.getMessage());
		}
	}
	
	@Test
	public void testHadoopFsPath() {
		try {
			TempletonUtils.hadoopFsPath(null, null);
			TempletonUtils.hadoopFsPath("/tmp", null);
			TempletonUtils.hadoopFsPath("/tmp", new Configuration());
		} catch (FileNotFoundException e) {
			fail("Couldn't find /tmp");
		} catch (Exception e) {
			// This is our problem -- it means the configuration was wrong.
			e.printStackTrace();
		}
		try { 
			TempletonUtils.hadoopFsPath("/scoobydoo/teddybear", new Configuration());
			fail("Should not have found /scoobydoo/teddybear");
		} catch (FileNotFoundException e) {
			// Should go here.
		} catch (Exception e) {
			// This is our problem -- it means the configuration was wrong.
			e.printStackTrace();
		}
	}
	
	@Test
	public void testHadoopFsFilename() {
		try {
			assertEquals(null, TempletonUtils.hadoopFsFilename(null, null));
			assertEquals(null, TempletonUtils.hadoopFsFilename("/tmp", null));
			assertEquals("file:/tmp", 
					TempletonUtils.hadoopFsFilename("/tmp", new Configuration()));
		} catch (FileNotFoundException e) {
			fail("Couldn't find name for /tmp");
		} catch (Exception e) {
			// Something else is wrong
			e.printStackTrace();
		}
		try {
			TempletonUtils.hadoopFsFilename("/scoobydoo/teddybear", new Configuration());
			fail("Should not have found /scoobydoo/teddybear");
		} catch (FileNotFoundException e) {
			// Should go here.
		} catch (Exception e) {
			// Something else is wrong.
			e.printStackTrace();
		}
	}

	@Test
	public void testHadoopFsListAsArray() {
		try {
			assertTrue(TempletonUtils.hadoopFsListAsArray(null, null) == null);
			assertTrue(TempletonUtils.hadoopFsListAsArray("/tmp, /usr", null) == null);
			String[] tmp2 = TempletonUtils.hadoopFsListAsArray
						("/tmp,/usr", new Configuration());
			assertEquals("file:/tmp", tmp2[0]);
			assertEquals("file:/usr", tmp2[1]);
		} catch (FileNotFoundException e) {
			fail("Couldn't find name for /tmp");
		} catch (Exception e) {
			// Something else is wrong
			e.printStackTrace();
		}
		try {
			TempletonUtils.hadoopFsListAsArray("/scoobydoo/teddybear,joe", 
					new Configuration());
			fail("Should not have found /scoobydoo/teddybear");
		} catch (FileNotFoundException e) {
			// Should go here.
		} catch (Exception e) {
			// Something else is wrong.
			e.printStackTrace();
		}
	}

	@Test
	public void testHadoopFsListAsString() {
		try {
			assertTrue(TempletonUtils.hadoopFsListAsString(null, null) == null);
			assertTrue(TempletonUtils.hadoopFsListAsString("/tmp,/usr", null) == null);
			assertEquals("file:/tmp,file:/usr", TempletonUtils.hadoopFsListAsString
						("/tmp,/usr", new Configuration()));
		} catch (FileNotFoundException e) {
			fail("Couldn't find name for /tmp");
		} catch (Exception e) {
			// Something else is wrong
			e.printStackTrace();
		}
		try {
			TempletonUtils.hadoopFsListAsString("/scoobydoo/teddybear,joe", 
					new Configuration());
			fail("Should not have found /scoobydoo/teddybear");
		} catch (FileNotFoundException e) {
			// Should go here.
		} catch (Exception e) {
			// Something else is wrong.
			e.printStackTrace();
		}
	}

}
