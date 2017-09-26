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
        
        InputStream hdfsConfig = TwitterTopology.class.getResourceAsStream("/hdfs.config");
        @SuppressWarnings("unchecked")
		Map<String, Object> conf = (Map<String, Object>) yaml.load(hdfsConfig);           
        hdfsConfig.close();
        
        return conf;
	}	
	
	public static void main(String[] args) throws Exception {	
		
		String topologyNAME = args[0];		
		String subFOLDER = args[1];
		String[] queueINPUT = args[2].split(",");
		String[] queueOUTPUT = args[3].split(",");
		String streamSpout = "stream_spout";
		
		TopologyBuilder builder = new TopologyBuilder();
		Config conf = new Config();		
	    conf.setNumWorkers(1);
        conf.put("hdfs.config", getServerConf());            
	    
        SaveHDFSBolt sb = new SaveHDFSBolt(subFOLDER);
        
		builder.setSpout(streamSpout, new StreamSpout(queueINPUT));
		builder.setBolt("save_bolt", sb.bolt()).shuffleGrouping("stream_spout");
		builder.setBolt("queue_bolt", new QueueBolt(queueOUTPUT)).shuffleGrouping(streamSpout);
		builder.setBolt("save_db_bolt", new SaveRDBMSBolt()).shuffleGrouping(streamSpout);
		builder.setBolt("save_nosql_bolt", new SaveNOSQLBolt()).shuffleGrouping(streamSpout);
		
//		Set up for production
	    StormSubmitter.submitTopology(topologyNAME, conf, builder.createTopology());			    
	}	
}
