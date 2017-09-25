package twitter.capturer.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import data.common.queue.DataQueue;
import data.common.queue.ProducerHelper;

public class QueueBolt implements IRichBolt {
	private static final long serialVersionUID = -1082738376742471393L;
	private static final Logger log = Logger.getLogger(QueueBolt.class);
	private String[] queueNames;
	private List<DataQueue> queues = new ArrayList<>();

	public QueueBolt(String[] queueOUTPUT) {
		queueNames = queueOUTPUT;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map arg0, TopologyContext arg1, OutputCollector arg2) {
		ProducerHelper helper = ProducerHelper.getHelper("queue.xml");
		for (String name : queueNames) {
			queues.add(helper.getQueue(name));
		}
	}

	@Override
	public void cleanup() {
		log.debug("Cleanup here.");
	}

	@Override
	public void execute(Tuple t) {
		String tweet = (String) t.getValue(0);
		for (DataQueue q : queues) {
			q.push(tweet);
			log.debug("Queue " + q.getQueueName());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		log.debug("declareOutputFields here.");
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		log.debug("getComponentConfiguration here.");
		return null;
	}

}
