package twitter.capturer.bolt;

import java.sql.Timestamp;
import java.util.Map;

import org.springframework.context.support.GenericXmlApplicationContext;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import data.common.domain.nosql.Tweet;
import data.common.domain.nosql.TweetHashtag;
import data.common.domain.rdbms.TweetData;
import data.common.rdbms.TweetDataRepository;
import data.common.util.JsonUtil;

public class SaveRDBMSBolt implements IRichBolt{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1036212917247901397L;

	private GenericXmlApplicationContext rdbContext;
	private TweetDataRepository tdr;
		
	@Override
	public void cleanup() {
		if (rdbContext != null) {
			rdbContext.close();
			rdbContext = null;
		}
	}

	@Override
	public void execute(Tuple t) {
		String msg = (String) t.getValue(0);

		try {
			Tweet tweet = JsonUtil.fromJson(msg, Tweet.class);
			tweet.convertType();
			tdr.save(getTweetDataEntity(tweet));  
		} catch (Exception ex) {
		}
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map arg0, TopologyContext arg1, OutputCollector arg2) {
		rdbContext = new GenericXmlApplicationContext("database.xml");
		tdr = rdbContext.getBean(TweetDataRepository.class);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private TweetData getTweetDataEntity(Tweet tweet) {
		TweetData entity = tdr.findOne(tweet.getId_str());
		if (entity == null) {			
			entity = new TweetData();
			entity.setTweet_id(tweet.getId_str());
			entity.setContent(tweet.getText());
			entity.setTweet_created(new Timestamp(tweet.getCreated_time()));
			entity.setReply_to_id(tweet.getIn_reply_to_status_id_str());
			entity.setReply_to_uid(tweet.getIn_reply_to_user_id_str());
			entity.setUser_id(String.valueOf(tweet.getUser().getId()));
			entity.setScreen_name(tweet.getUser().getScreenName());
			entity.setCaptured_at(new Timestamp(tweet.getCaptured_at().getTime()));		
			entity.setCaptured_by(tweet.getCaptured_by());
			entity.setOrigin_code(tweet.getOrigin_code());
			for (TweetHashtag tag : tweet.getEntities().getHashtags()) {
				entity.addHashtag(tag.getText());
			}			
		}

		entity.addTopic(tweet.getTopic());
		entity.addCategory(tweet.getCategory());

		return entity;
	}
}