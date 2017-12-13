package com.linkedin.drelephant.tez.fetchers;

import java.util.regex.Matcher;
import org.junit.Assert;
import org.junit.Test;

public class TezFetcherTest {

    @Test
    public void testDiagnosticMatcher() {
        Matcher matcher = com.linkedin.drelephant.tez.fetchers.ThreadContextMR2.getDiagnosticMatcher("Task task_1443068695259_9143_m_000475 failed 1 time");
        Assert.assertEquals("Task[\\s\\u00A0]+(.*)[\\s\\u00A0]+failed[\\s\\u00A0]+([0-9])[\\s\\u00A0]+times[\\s\\u00A0]+", matcher.pattern().toString());
        Assert.assertEquals(false, matcher.matches());
        Assert.assertEquals(2, matcher.groupCount());
    }

}