package twitter.capturer.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import data.common.domain.nosql.Tweet;
import data.common.queue.DataQueue;
import data.common.queue.ProducerHelper;
import data.common.util.JsonUtil;

public class StreamSpout extends BaseRichSpout {
	private static final long serialVersionUID = -2463135276315437828L;
	private Logger log = Logger.getLogger(this.getClass());
	private SpoutOutputCollector socCollector;

	private String[] queueNames;
	private List<DataQueue> queues = new ArrayList<>();
	private TreeSet<String> wordslist = new TreeSet<>();

	public StreamSpout(String[] queueINPUT) {
		queueNames = queueINPUT;
	}

	private void initWordlist() {
		InputStream in = StreamSpout.class.getResourceAsStream(File.separator + "wordlist");
		Scanner scanner = new Scanner(new BufferedReader(new InputStreamReader(in)));

		while (scanner.hasNext()) {
			String line = scanner.nextLine();
			if (line == null || "".equals(line)) {
				continue;
			}
			String[] words = line.split("\\t");
			for (String word : words) {
				if (word != null && !"".equals(word)) {
					if (word.charAt(word.length() - 1) == '*') {
						word = word.substring(0, word.length() - 1);
					}
					wordslist.add(word.trim().toLowerCase());
				}
			}
		}
		scanner.close();
		try {
			in.close();
		} catch (Exception ex) {
			log.error("Close affect words file error: " + ex.getMessage());
		}
	}

	private void initQueueInput() {
		ProducerHelper helper = ProducerHelper.getHelper("queue.xml");
		for (String name : queueNames) {
			queues.add(helper.getQueue(name));
		}
	}

	private boolean checkWords(String text) {
		if (text == null || text.length() == 0)
			return false;

		String[] words = text.toLowerCase().split("\\W+");
		for (String word : words) {
			if (wordslist.floor(word) != null)
				return true;
		}

		return false;
	}

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		initWordlist();
		initQueueInput();
		socCollector = collector;
	}

	public void nextTuple() {
		for (DataQueue q : queues) {
			List<String> twtJson = q.poll();

			if (twtJson == null || twtJson.isEmpty()) {
				Utils.sleep(100/* _interval */);
			} else {
				for (String content : twtJson) {
					Tweet tweet = JsonUtil.fromJson(content, Tweet.class);
					tweet.convertType();

					if (checkWords(tweet.getText())) {
						socCollector.emit(new Values(twtJson));
					}
				}
			}
		}
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void close() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
