package datawave.ingest.util.cache.watch;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import datawave.iterators.filter.AgeOffConfigParams;
import datawave.iterators.filter.AgeOffTtlUnits;
import datawave.iterators.filter.ageoff.AgeOffPeriod;
import datawave.iterators.filter.ageoff.AppliedRule;
import datawave.iterators.filter.ageoff.FilterOptions;
import datawave.iterators.filter.ageoff.FilterRule;

/**
 * Tests to verify capability of merging configs that use filters that inherit from {@code TokenizingFilterBase}
 */
public class FileRuleLoadContentsMergeFiltersTest {
    private static final String ROOT_FILTER_CONFIGURATION_FILE = "/test-root-rules.xml";
    private static final String CHILD_FILTER_CONFIGURATION_FILE = "/test-customized-rules.xml";
    private static final long MILLIS_IN_DAY = 24 * 60 * 60 * 1000;
    private static final int MILLIS_IN_ONE_SEC = 60 * 1000;

    private FileRuleWatcher watcher;
    private TestTrieFilter parentFilter;
    // this one inherits defaults from parentFilter
    private TestTrieFilter childFilter;
    // reference used by multiple tests, contents not used
    private Value value = new Value();
    // fixes the root time
    private long anchorTime;
    private FilterOptions filterOptions;

    @Before
    public void before() throws IOException {
        Path childPath = new Path(
                this.getClass().getResource(CHILD_FILTER_CONFIGURATION_FILE).toString());
        Path rootPath = new Path(
                this.getClass().getResource(ROOT_FILTER_CONFIGURATION_FILE).toString());
        FileSystem fs = childPath.getFileSystem(new Configuration());
        watcher = new FileRuleWatcher(fs, childPath, 1);
        parentFilter = (TestTrieFilter) loadRulesFromFile(watcher, fs, rootPath);
        childFilter = (TestTrieFilter) loadRulesFromFile(watcher, fs, childPath);
        anchorTime = System.currentTimeMillis();
        // use this to configure the age off evaluation, all units expected to be in days
        filterOptions = new FilterOptions();
        filterOptions.setTTL(365);
        filterOptions.setTTLUnits(AgeOffTtlUnits.DAYS);
    }

    @Test
    public void verifyOverridenValues() throws IOException {
        // there are 6 rules, three are overridden

        // original mappings should be
        // "baking powder" : 365d
        // "dried beans" : 548d
        // "baking soda" : 720d
        // "coffee grounds" : 90d
        // "coffee whole bean" : 183d
        // "coffee instant" : 730d

        // we are using the definitions from the customized rules file which should override
        // "coffee grounds" : 30d
        // "coffee whole bean" : 150d
        // "coffee instant" : 1080d

        // verify original mappings
        verifyParentRule("baking powder", 365L);
        verifyParentRule("dried beans", 548L);
        verifyParentRule("baking soda", 720L);
        verifyParentRule("coffee ground", 90L);
        verifyParentRule("coffee whole bean", 183L);
        verifyParentRule("coffee instant", 730L);

        // verify overridden mappings, which should include original and new values
        verifyChildRule("baking powder", 365L);
        verifyChildRule("dried beans", 548L);
        verifyChildRule("baking soda", 720L);
        verifyChildRule("coffee ground", 30L);
        verifyChildRule("coffee whole bean", 150L);
        verifyChildRule("coffee instant", 1080L);
    }

    private void verifyParentRule(String data, long offsetInDays) {
        // should accept if timestamp is exact
        verifyAppliedRule(parentFilter, data, offsetInDays, true);
        // should accept if timestamp is less than configured age off
        verifyAppliedRule(parentFilter, data, offsetInDays - 1, true);
        // should NOT accept if timestamp is more than configured age off
        verifyAppliedRule(parentFilter, data, offsetInDays + 1, false);
    }

    private void verifyChildRule(String data, long offsetInDays) {
        // should accept if timestamp is exact
        verifyAppliedRule(childFilter, data, offsetInDays, true);
        // should accept if timestamp is less than configured age off
        verifyAppliedRule(childFilter, data, offsetInDays - 1, true);
        // should NOT accept if timestamp is more than configured age off
        verifyAppliedRule(childFilter, data, offsetInDays + 1, false);
    }

    private void verifyAppliedRule(AppliedRule filter, String data, long offsetInDays, boolean expectation) {
        // need time stamp that is N days ago, use anchorTime to anchor time
        // that way only need one call to System.currentTimeInMillis()
        // the time check is ts > cutoffTimestamp, so + 1 will let exact offsetInDays to pass this
        long timestamp = anchorTime - (offsetInDays * MILLIS_IN_DAY) + 1;
        Key key = TestTrieFilter.create(data, timestamp);
        assertThat(failedExpectationMessage(data, offsetInDays, expectation),
                filter.accept(filterOptions.getAgeOffPeriod(anchorTime), key, value),
                is(expectation));
    }

    private String failedExpectationMessage(String data, long offsetInDays, boolean expectation) {
        return "Expected " + data + " with offset of " + offsetInDays + " days to be " + expectation
                + " but was " + !expectation;
    }

    @Test
    public void testTtl() {
        FilterOptions filterOpts = new FilterOptions();
        filterOpts.setOption(AgeOffConfigParams.MATCHPATTERN, "\"1234\":5s");
        long tenSecondsAgo = System.currentTimeMillis() - (10 * MILLIS_IN_ONE_SEC);

        TestTrieFilter filter = new TestTrieFilter();
        // set the default to 5 seconds
        filterOpts.setTTL(5);
        filterOpts.setTTLUnits(AgeOffTtlUnits.SECONDS);
        filter.init(filterOpts);

        Key key = new Key("1234".getBytes(), tenSecondsAgo);
        AgeOffPeriod ageOffPeriod = filterOpts.getAgeOffPeriod(System.currentTimeMillis());
        assertFalse(filter.accept(ageOffPeriod, key, new Value()));
        assertTrue(filter.isFilterRuleApplied());
    }

    private static FilterRule loadRulesFromFile(FileRuleWatcher watcher, FileSystem fs,
            Path filePath) throws IOException {
        Collection<FilterRule> rules = watcher.loadContents(fs.open(filePath));
        // should only have the single rule
        assertThat(rules.size(), is(1));
        for (FilterRule rule : rules) {
            assertEquals(TestTrieFilter.class, rule.getClass());
        }
        return rules.iterator().next();
    }
}
