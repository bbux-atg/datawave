package datawave.ingest.util;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.job.ShardedTableMapFile;
import datawave.util.time.DateHelper;

/**
 * 
 * This utility will validate that the splits for the days specified have been created and are balanced
 */
public class ValidateSplits {
    
    private static final Logger log = Logger.getLogger(ValidateSplits.class);
    
    public static final String NUM_DAYS_TO_CHECK = "nd";
    public static final String CONFIG_DIRECTORY_LOCATION_OVERRIDE = "cd";
    public static final String CONFIG_SUFFIX_OVERRIDE = "cs";
    public static final String WORK_DIR_PATH = "wd";
    
    @SuppressWarnings("static-access")
    public static void main(String[] args) throws Exception {
        AccumuloCliOptions accumuloOptions = new AccumuloCliOptions();
        Options options = accumuloOptions.getOptions();
        options.addOption(OptionBuilder.isRequired(true).hasArg().withDescription("Config directory path").create(CONFIG_DIRECTORY_LOCATION_OVERRIDE));
        options.addOption(OptionBuilder.isRequired(true).hasArg().withDescription("Work dir path").create(WORK_DIR_PATH));
        options.addOption(OptionBuilder.isRequired(false).hasArg().withDescription("Config file suffix").create(CONFIG_SUFFIX_OVERRIDE));
        options.addOption(OptionBuilder.isRequired(false).hasArg().withDescription("Days back to check, default is only to check today, or arg of 0")
                        .create(NUM_DAYS_TO_CHECK));
        Configuration conf = new Configuration();
        CommandLine cl;
        String configDirectory = null;
        String configSuffix;
        String workDir = null;
        int numDays = 0;
        
        try {
            cl = new BasicParser().parse(options, args);
            configDirectory = cl.getOptionValue(CONFIG_DIRECTORY_LOCATION_OVERRIDE);
            workDir = cl.getOptionValue(WORK_DIR_PATH);
            if (cl.hasOption(NUM_DAYS_TO_CHECK)) {
                String numStr = cl.getOptionValue(NUM_DAYS_TO_CHECK, Integer.toString(numDays));
                try {
                    numDays = Integer.parseInt(numStr);
                } catch (NumberFormatException e) {
                    log.warn("Invalid format for number of days to check arg: -" + NUM_DAYS_TO_CHECK + " " + numStr);
                    throw new IllegalArgumentException(e);
                }
            }
            if (cl.hasOption(CONFIG_SUFFIX_OVERRIDE)) {
                configSuffix = cl.getOptionValue(CONFIG_SUFFIX_OVERRIDE);
            } else {
                configSuffix = "config.xml";
            }
            ConfigurationFileHelper.setConfigurationFromFiles(conf, configDirectory, configSuffix);
            String[] tables = conf.getStrings(ShardedDataTypeHandler.SHARDED_TNAMES);
            if (null == tables) {
                throw new IllegalArgumentException("Unable to load sharded table names from config, verify " + ShardedDataTypeHandler.SHARDED_TNAMES
                                + " is set.");
            }
            // needed for sharded table map file
            conf.set(ShardedTableMapFile.SPLIT_WORK_DIR, workDir);
            // make sure accumulo creds are configured
            accumuloOptions.setCommandLine(cl);
            accumuloOptions.setAccumuloConfiguration(conf);
            
            if (validate(conf, tables, numDays)) {
                System.exit(0);
            } else {
                System.exit(-1);
            }
            
        } catch (ParseException ex) {
            log.error("Error parsing options", ex);
            printHelp(options);
        }
    }
    
    private static void printHelp(Options options) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("Validates shard split creation and balancing for specified number of days", options);
    }
    
    private static boolean validate(Configuration conf, String[] tables, int daysToVerify) throws IOException {
        ShardSplitValidator shardSplitValidator = new ShardSplitValidator(conf);
        boolean shardTablesSplitsValid = shardSplitValidator.allTablesValid(tables, daysToVerify);
        return shardTablesSplitsValid;
    }
    
    /**
     * Performs validation of creation and balance of shard splits
     */
    static class ShardSplitValidator {
        private ShardIdFactory shardIdFactory;
        private Configuration conf;
        
        /**
         * @param conf
         *            to use
         */
        public ShardSplitValidator(Configuration conf) {
            setConf(conf);
        }
        
        /**
         * @param conf
         *            to use
         */
        public void setConf(Configuration conf) {
            // this is needed so that the setupFile knows what tables to setup
            conf.set(ShardedTableMapFile.TABLE_NAMES, conf.get(ShardedDataTypeHandler.SHARDED_TNAMES));
            try {
                ShardedTableMapFile.setupFile(conf);
            } catch (Exception e) {
                e.printStackTrace();
                log.warn("Unable to execute shard table map file setup!");
            }
            this.conf = conf;
            shardIdFactory = new ShardIdFactory(conf);
        }
        
        /**
         * Verifies both that the shards have been created and are balanced for the specified tables for the last daysToVerify days.Balanced shards are spread
         * out evenly across the available tservers. Unbalanced shards will have multiple shards assigned to the same tserver.
         * 
         * @param tables
         *            to validate the shards for
         * @param daysToVerify
         *            the number of days back to validate
         * @return if all of the tables are valid for the daysToVerify
         * @throws IOException
         *             when unable to load the {@code ShardedTableMapFile}
         */
        boolean allTablesValid(String[] tables, int daysToVerify) throws IOException {
            // assume true unless proven otherwise
            boolean isValid = true;
            // go through all the tables and don't short circuit, so that we can log all issues
            for (String tableName : tables) {
                if (!isValid(tableName, daysToVerify)) {
                    isValid = false;
                }
            }
            return isValid;
        }
        
        /**
         * Verifies both that the shards have been created and are balanced for the specified table for the last daysToVerify days. If any splits in the last
         * daysToVerify days has missing or unbalanced shards this method will return false. Balanced shards are spread out evenly across the available
         * tservers. Unbalanced shards will have multiple shards assigned to the same tserver.
         * 
         * @param tableName
         *            to verify
         * @param daysToVerify
         *            the number of days back to validate
         * @return if the table has valid balanced shards for last daysToVerify
         * @throws IOException
         *             when unable to load the {@code ShardedTableMapFile}
         */
        boolean isValid(String tableName, int daysToVerify) throws IOException {
            TreeMap<Text,String> shardIdToLocation = ShardedTableMapFile.getShardIdToLocations(conf, tableName);
            // assume true unless proven otherwise
            boolean isValid = true;
            for (int daysAgo = 0; daysAgo <= daysToVerify; daysAgo++) {
                long inMillis = System.currentTimeMillis() - (daysAgo * DateUtils.MILLIS_PER_DAY);
                String datePrefix = DateHelper.format(inMillis);
                int expectedNumberOfShards = shardIdFactory.getNumShards(datePrefix);
                boolean shardsExist = shardsExistForDate(shardIdToLocation, datePrefix, expectedNumberOfShards);
                if (!shardsExist) {
                    log.warn("Shards for " + datePrefix + " for table " + tableName + " do not exist!");
                    isValid = false;
                    continue;
                }
                boolean shardsAreBalanced = shardsAreBalanced(shardIdToLocation, datePrefix);
                if (!shardsAreBalanced) {
                    log.warn("Shards for " + datePrefix + " for table " + tableName + " are not balanced!");
                    isValid = false;
                }
            }
            return isValid;
        }
        
        /**
         * Existence check for the shard splits for the specified date
         * 
         * @param locations
         *            mapping of shard to tablet
         * @param datePrefix
         *            to check
         * @param expectedNumberOfShards
         *            that should exist
         * @return if the number of shards for the given date are as expected
         */
        private boolean shardsExistForDate(TreeMap<Text,String> locations, String datePrefix, int expectedNumberOfShards) {
            int count = 0;
            byte[] prefixBytes = datePrefix.getBytes();
            for (Text key : locations.keySet()) {
                if (prefixMatches(prefixBytes, key.getBytes(), key.getLength())) {
                    count++;
                }
            }
            return count == expectedNumberOfShards;
        }
        
        /**
         * Checks that the shard splits for the given date have been assigned to unique tablets.
         * 
         * @param locations
         *            mapping of shard to tablet
         * @param datePrefix
         *            to check
         * @return if the shards are distributed in a balanced fashion
         */
        private boolean shardsAreBalanced(TreeMap<Text,String> locations, String datePrefix) {
            // assume true unless proven wrong
            boolean dateIsBalanced = true;
            
            Set<String> tabletsSeenForDate = new HashSet<>();
            byte[] prefixBytes = datePrefix.getBytes();
            
            for (Entry<Text,String> entry : locations.entrySet()) {
                Text key = entry.getKey();
                // only check entries for specified date
                if (prefixMatches(prefixBytes, key.getBytes(), key.getLength())) {
                    String value = entry.getValue();
                    // if we have already seen this tablet assignment, then the shards are not balanced
                    if (tabletsSeenForDate.contains(value)) {
                        log.warn("Multiple Shards for " + datePrefix + " assigned to tablet " + value);
                        dateIsBalanced = false;
                    }
                    tabletsSeenForDate.add(value);
                }
            }
            
            return dateIsBalanced;
        }
        
        private boolean prefixMatches(byte[] prefixBytes, byte[] keyBytes, int keyLen) {
            // if key length is less than prefix size, no use comparing
            if (prefixBytes.length > keyLen) {
                return false;
            }
            for (int i = 0; i < prefixBytes.length; i++) {
                if (prefixBytes[i] != keyBytes[i]) {
                    return false;
                }
            }
            // at this point didn't fail match, so should be good
            return true;
        }
    }
    
}
