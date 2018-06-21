package datawave.ingest.util;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import datawave.ingest.mapreduce.job.ShardedTableMapFile;
import datawave.ingest.mapreduce.partition.TestShardGenerator;
import datawave.ingest.util.ValidateSplits.ShardSplitValidator;
import datawave.util.time.DateHelper;

public class ValidateSplitsTest {
    
    private static final int SHARDS_PER_DAY = 12;
    private static final int NUM_DAYS = 3;
    private static final int TOTAL_TSERVERS = 48;
    
    private static Configuration conf;
    
    @BeforeClass
    public static void defineShardLocationsFile() throws IOException {
        conf = new Configuration();
        conf.setInt(ShardIdFactory.NUM_SHARDS, SHARDS_PER_DAY);
    }
    
    private ShardSplitValidator validator;
    
    @Before
    public void setUp() throws IOException {
        validator = new ValidateSplits.ShardSplitValidator(conf);
        // gotta load this every test, or using different values bleeds into other tests
        new TestShardGenerator(conf, NUM_DAYS, SHARDS_PER_DAY, TOTAL_TSERVERS, "shard");
        validator.setConf(conf);
        assertEquals("shard", conf.get(ShardedTableMapFile.CONFIGURED_SHARDED_TABLE_NAMES));
    }
    
    @Test
    public void testSingleDayMultipleTablesSplitsCreated_AndValid() throws Exception {
        String tableName1 = "validSplits1";
        String tableName2 = "validSplits2";
        
        SortedMap<KeyExtent,String> locations = createDistributedLocations(tableName1);
        // use same shard locations for both tables
        new TestShardGenerator(conf, locations, tableName1, tableName2);
        
        String[] tables = conf.getStrings(ShardedTableMapFile.CONFIGURED_SHARDED_TABLE_NAMES);
        assertThat(tables.length, is(2));
        
        // three days of splits, all should be good, for all tables
        assertThat(validator.allTablesValid(tables, 2), is(true));
    }
    
    @Test
    public void testSingleDayMultipleTablesSplitsCreated_AndNOTValid() throws Exception {
        String tableName1 = "validSplits1";
        String tableName2 = "validSplits2";
        
        SortedMap<KeyExtent,String> locations = simulateMissingSplitsForDay(1, tableName1);
        // use same shard locations for both tables
        new TestShardGenerator(conf, locations, tableName1, tableName2);
        
        String[] tables = conf.getStrings(ShardedTableMapFile.CONFIGURED_SHARDED_TABLE_NAMES);
        assertThat(tables.length, is(2));
        
        // three days of splits, all should be NOT good, for all tables
        assertThat(validator.allTablesValid(tables, 2), is(false));
    }
    
    @Test
    public void testSingleDaySplitsCreated_AndValid() throws Exception {
        String tableName = "validSplits";
        SortedMap<KeyExtent,String> locations = createDistributedLocations(tableName);
        new TestShardGenerator(conf, locations, tableName);
        // three days of splits, all should be good
        assertThat(validator.isValid(tableName, 0), is(true));
        assertThat(validator.isValid(tableName, 1), is(true));
        assertThat(validator.isValid(tableName, 2), is(true));
    }
    
    @Test
    public void testMissingAllOfTodaysSplits() throws Exception {
        String tableName = "missingTodaysSplits";
        SortedMap<KeyExtent,String> locations = simulateMissingSplitsForDay(0, tableName);
        new TestShardGenerator(conf, locations, tableName);
        // three days of splits, today should be invalid, which makes the rest bad too
        assertThat(validator.isValid(tableName, 0), is(false));
        assertThat(validator.isValid(tableName, 1), is(false));
        assertThat(validator.isValid(tableName, 2), is(false));
    }
    
    @Test
    public void testUnbalancedTodaysSplits() throws Exception {
        String tableName = "missingTodaysSplits";
        SortedMap<KeyExtent,String> locations = simulateUnbalancedSplitsForDay(0, tableName);
        new TestShardGenerator(conf, locations, tableName);
        // three days of splits, today should be invalid, which makes the rest bad too
        assertThat(validator.isValid(tableName, 0), is(false));
        assertThat(validator.isValid(tableName, 1), is(false));
        assertThat(validator.isValid(tableName, 2), is(false));
    }
    
    @Test
    public void testMissingAllOfYesterdaysSplits() throws Exception {
        String tableName = "missingTodaysSplits";
        SortedMap<KeyExtent,String> locations = simulateMissingSplitsForDay(1, tableName);
        new TestShardGenerator(conf, locations, tableName);
        // three days of splits, today should be valid
        // yesterday and all other days invalid
        assertThat(validator.isValid(tableName, 0), is(true));
        assertThat(validator.isValid(tableName, 1), is(false));
        assertThat(validator.isValid(tableName, 2), is(false));
    }
    
    @Test
    public void testUnbalancedYesterdaysSplits() throws Exception {
        String tableName = "missingTodaysSplits";
        SortedMap<KeyExtent,String> locations = simulateUnbalancedSplitsForDay(1, tableName);
        new TestShardGenerator(conf, locations, tableName);
        // three days of splits, today should be valid
        // yesterday and all other days invalid
        assertThat(validator.isValid(tableName, 0), is(true));
        assertThat(validator.isValid(tableName, 1), is(false));
        assertThat(validator.isValid(tableName, 2), is(false));
    }
    
    private SortedMap<KeyExtent,String> simulateUnbalancedSplitsForDay(int daysAgo, String tableName) throws IOException {
        // start with a well distributed set of shards per day for 3 days
        SortedMap<KeyExtent,String> locations = createDistributedLocations(tableName);
        // for shards from "daysAgo", peg them to first shard
        String tserverId = "1";
        Text prevEndRow = new Text();
        String date = DateHelper.format(System.currentTimeMillis() - (daysAgo * DateUtils.MILLIS_PER_DAY));
        for (int currShard = 0; currShard < SHARDS_PER_DAY; currShard++) {
            locations.put(new KeyExtent(tableName, new Text(date + "_" + currShard), prevEndRow), tserverId);
        }
        
        return locations;
    }
    
    private SortedMap<KeyExtent,String> simulateMissingSplitsForDay(int daysAgo, String tableName) throws IOException {
        // start with a well distributed set of shards per day for 3 days
        SortedMap<KeyExtent,String> locations = createDistributedLocations(tableName);
        // for shards from "daysAgo", remove them
        Text prevEndRow = new Text();
        String date = DateHelper.format(System.currentTimeMillis() - (daysAgo * DateUtils.MILLIS_PER_DAY));
        for (int currShard = 0; currShard < SHARDS_PER_DAY; currShard++) {
            locations.remove(new KeyExtent(tableName, new Text(date + "_" + currShard), prevEndRow));
        }
        
        return locations;
    }
    
    private SortedMap<KeyExtent,String> createDistributedLocations(String tableName) {
        SortedMap<KeyExtent,String> locations = new TreeMap<>();
        long now = System.currentTimeMillis();
        int tserverId = 1;
        Text prevEndRow = new Text();
        for (int daysAgo = 0; daysAgo <= 2; daysAgo++) {
            String day = DateHelper.format(now - (daysAgo * DateUtils.MILLIS_PER_DAY));
            for (int currShard = 0; currShard < SHARDS_PER_DAY; currShard++) {
                locations.put(new KeyExtent(tableName, new Text(day + "_" + currShard), prevEndRow), Integer.toString(tserverId++));
            }
        }
        return locations;
    }
    
}
