package com.rw.Enricher.Test;

import static org.junit.Assert.*;

import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.rw.Enricher.EnrichmentFactory;
import com.rw.Enricher.WhitePagesImpl;

public class unitTests {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws Exception {
		
		WhitePagesImpl wp = new WhitePagesImpl();
		System.out.println(wp.mapAreaCodeToZips("(925) 819-6420"));
	}

}
