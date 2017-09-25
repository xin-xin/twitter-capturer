package twitter.capturer.starter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import twitter.capturer.bolt.QueueBolt;
import twitter.capturer.bolt.SaveHDFSBolt;
import twitter.capturer.bolt.SaveNOSQLBolt;
import twitter.capturer.bolt.SaveRDBMSBolt;
import twitter.capturer.spout.StreamSpout;

public class TwitterTopology {
		
	public static Map <String, Object> getServerConf() throws IOException {
        Yaml yaml = new Yaml();
        
        InputStream _hdfsConfig = TwitterTopology.class.getResourceAsStream("/hdfs.config");
        @SuppressWarnings("unchecked")
		Map<String, Object> conf = (Map<String, Object>) yaml.load(_hdfsConfig);           
        _hdfsConfig.close();
        
        return conf;
	}	
	
	public static void main(String[] args) throws Exception {	
		
		String TOPOLOGY_NAME = args[0];		
		String SUB_FOLDER = args[1];
		String[] QUEUE_INPUT = args[2].split(",");
		String[] QUEUE_OUTPUT = args[3].split(",");
		
		TopologyBuilder builder = new TopologyBuilder();
		Config conf = new Config();		
	    conf.setNumWorkers(1);
        conf.put("hdfs.config", getServerConf());            
	    
        SaveHDFSBolt sb = new SaveHDFSBolt(SUB_FOLDER);
        
		builder.setSpout("stream_spout", new StreamSpout(QUEUE_INPUT));
		builder.setBolt("save_bolt", sb.bolt()).shuffleGrouping("stream_spout");
		builder.setBolt("queue_bolt", new QueueBolt(QUEUE_OUTPUT)).shuffleGrouping("stream_spout");
		builder.setBolt("save_db_bolt", new SaveRDBMSBolt()).shuffleGrouping("stream_spout");
		builder.setBolt("save_nosql_bolt", new SaveNOSQLBolt()).shuffleGrouping("stream_spout");
		
//		Set up for production
	    StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());			    
	}	
}
