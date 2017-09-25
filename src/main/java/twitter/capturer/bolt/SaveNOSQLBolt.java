package twitter.capturer.bolt;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.beanutils.PropertyUtilsBean;
import org.apache.log4j.Logger;
import org.springframework.context.support.ConversionServiceFactoryBean;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.convert.converter.Converter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import data.common.domain.nosql.Tweet;
import data.common.domain.nosql.TwitterUser;
import data.common.nosql.BooleanIntConverter;
import data.common.nosql.DateConverter;
import data.common.nosql.GeoConverter;
import data.common.nosql.IntBooleanConverter;
import data.common.nosql.repo.TweetRepository;
import data.common.nosql.repo.TwitterUserRepository;
import data.common.util.JsonUtil;

public class SaveNOSQLBolt implements IRichBolt{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1036212917247901397L;
	private static final Logger log = Logger.getLogger(SaveNOSQLBolt.class);
	
	private GenericXmlApplicationContext context;
	private TwitterUserRepository twitterUserRepo;
	private TweetRepository tweetRepo;
	
	@Override
	public void cleanup() {
		if (context != null) {
			context.close();
			context = null;
		}
	}

	@Override
	public void execute(Tuple t) {
		String msg = (String) t.getValue(0);

		Tweet tweet = JsonUtil.fromJson(msg, Tweet.class);
		
		if (tweetRepo.exists(tweet.getId())) {				
			log.error("Found duplicate tweet: " + tweet.getId_str());
			return;
		}		
				
		// insert to nosql database		
		TwitterUser user = tweet.getUser();
		Date date = new Date();
		user.setLastUpdated(date);

		if (twitterUserRepo.exists(user.getId())) {
			try {
				TwitterUser exist = twitterUserRepo.findOne(user.getId());
				PropertyUtilsBean beanUtil = new PropertyUtilsBean();
				Date capturedAt = exist.getCapturedAt();
				beanUtil.copyProperties(exist, user);
				exist.setCapturedAt(capturedAt);
				twitterUserRepo.save(exist);
			} catch (Exception ex) {
				log.error(ex.getMessage());
				twitterUserRepo.save(user);
			}
		} else {
			twitterUserRepo.save(user);
		}

		tweet.convertType();
		tweetRepo.save(tweet);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map arg0, TopologyContext arg1, OutputCollector arg2) {
		context = new GenericXmlApplicationContext("nosql-default.xml");				
		ConversionServiceFactoryBean csf = context.getBean(ConversionServiceFactoryBean.class);				
		@SuppressWarnings("rawtypes")
		Set<Converter> converters = new HashSet<>();
		converters.add(new DateConverter());
		converters.add(new GeoConverter());
		converters.add(new IntBooleanConverter());
		converters.add(new BooleanIntConverter());
		csf.setConverters(converters);
		twitterUserRepo = context.getBean(TwitterUserRepository.class);
		tweetRepo = context.getBean(TweetRepository.class);	
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
	
}
