package twitter.capturer.starter;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.beanutils.PropertyUtilsBean;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.springframework.context.support.ConversionServiceFactoryBean;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.convert.converter.Converter;

import data.common.domain.nosql.Tweet;
import data.common.domain.nosql.TweetHashtag;
import data.common.domain.nosql.TwitterUser;
import data.common.domain.rdbms.TweetData;
import data.common.nosql.BooleanIntConverter;
import data.common.nosql.DateConverter;
import data.common.nosql.GeoConverter;
import data.common.nosql.IntBooleanConverter;
import data.common.nosql.repo.TweetRepository;
import data.common.nosql.repo.TwitterUserRepository;
import data.common.rdbms.TweetDataRepository;
import data.common.util.JsonUtil;

public class App {
	private Logger log = Logger.getLogger(this.getClass());
	private TreeSet<String> wordslist = new TreeSet<String>();

	@Test	
	public void testCassandraConnection() {
		System.out.println("Hi");
		GenericXmlApplicationContext context = new GenericXmlApplicationContext("nosql-default.xml");				
		ConversionServiceFactoryBean csf = context.getBean(ConversionServiceFactoryBean.class);				
		Set<Converter> converters = new HashSet<>();
		converters.add(new DateConverter());
		converters.add(new GeoConverter());
		converters.add(new IntBooleanConverter());
		converters.add(new BooleanIntConverter());
		csf.setConverters(converters);
		TwitterUserRepository twitterUserRepo = context.getBean(TwitterUserRepository.class);
		TweetRepository tweetRepo = context.getBean(TweetRepository.class);		
		
    	String msg = "{\r\n    \"id\": 606305834759356400,\r\n    \"text\": \"...\",\r\n    \"created_at\": \"Thu Jun 04 03:45:50 +0000 2015\",\r\n    \"id_str\": \"606305834759356416\",\r\n    \"source\": \"<a href=\\\"http://twitter.com\\\" rel=\\\"nofollow\\\">Twitter Web Client</a>\",\r\n    \"truncated\": false,\r\n    \"in_reply_to_status_id\": 0,\r\n    \"in_reply_to_user_id\": 0,\r\n    \"retweet_count\": 0,\r\n    \"favorite_count\": 0,\r\n    \"favorited\": false,\r\n    \"retweeted\": false,\r\n    \"possibly_sensitive\": false,\r\n    \"filter_level\": \"low\",\r\n    \"lang\": \"en\",\r\n    \"user\": {\r\n        \"id\": 3122949415,\r\n        \"name\": \"Nikki Schwerdtfeger\",\r\n        \"screen_name\": \"Badger_Bitch14\",\r\n        \"location\": \"Amsterdam, North Holland\",\r\n        \"description\": \"ED SHEERAN \u274C| 88 \u274C |BRISTON\u2716\uFE0F|ENGLAND \u2716\uFE0F| ANCORA IMPARO|\",\r\n        \"_protected\": false,\r\n        \"followers_count\": 336,\r\n        \"friends_count\": 929,\r\n        \"listed_count\": 0,\r\n        \"created_at\": \"Wed Apr 01 04:32:49 +0000 2015\",\r\n        \"favourites_count\": 1266,\r\n        \"utc_offset\": \"-25200\",\r\n        \"time_zone\": \"Pacific Time (US & Canada)\",\r\n        \"geo_enabled\": true,\r\n        \"verified\": false,\r\n        \"statuses_count\": 2820,\r\n        \"lang\": \"en-gb\",\r\n        \"contributors_enabled\": false,\r\n        \"is_translator\": false,\r\n        \"is_translation_enabled\": false,\r\n        \"profile_background_color\": \"000000\",\r\n        \"profile_background_image_url\": \"http://pbs.twimg.com/profile_background_images/598014532917694464/to4KmBjx.jpg\",\r\n        \"profile_background_image_url_https\": \"https://pbs.twimg.com/profile_background_images/598014532917694464/to4KmBjx.jpg\",\r\n        \"profile_background_tile\": false,\r\n        \"profile_image_url\": \"http://pbs.twimg.com/profile_images/604690579675430913/o_IDNpTU_normal.jpg\",\r\n        \"profile_image_url_https\": \"https://pbs.twimg.com/profile_images/604690579675430913/o_IDNpTU_normal.jpg\",\r\n        \"profile_link_color\": \"4A913C\",\r\n        \"profile_sidebar_border_color\": \"000000\",\r\n        \"profile_sidebar_fill_color\": \"000000\",\r\n        \"profile_text_color\": \"000000\",\r\n        \"profile_use_background_image\": true,\r\n        \"default_profile\": false,\r\n        \"default_profile_image\": false,\r\n        \"follow_request_sent\": false,\r\n        \"captured_at\": \"Jun 3, 2015 8:45:49 PM\"\r\n    },\r\n    \"userid\": 0,\r\n    \"captured_at\": \"Jun 3, 2015 8:45:49 PM\",\r\n    \"captured_by\": \"stream\",\r\n    \"entities\": {\r\n        \"hashtags\": [],\r\n        \"user_mentions\": []\r\n    },\r\n    \"origin_code\": \"SG\"\r\n}";		
    	Date date = null;

		try {
			Tweet tweet = JsonUtil.fromJson(msg, Tweet.class);
			
			if (tweetRepo.exists(tweet.getId())) {				
				log.error("Found duplicate tweet: " + tweet.getId_str());
				return;
			}

			// insert to nosql database		
			TwitterUser user = tweet.getUser();
			date = new Date();
			user.setLastUpdated(date);

			if (twitterUserRepo.exists(user.getId())) {
				try {
					TwitterUser exist = twitterUserRepo.findOne(user.getId());
					PropertyUtilsBean beanUtil = new PropertyUtilsBean();
					Date captured_at = exist.getCapturedAt();
					beanUtil.copyProperties(exist, user);
					exist.setCapturedAt(captured_at);
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

			log.debug("One message has been done.");
			
		} catch (Exception ex) {
			log.debug(msg);
			log.error(ex.getMessage(), ex);
		}				
	}
	
	public void testStringInput() {
		String[] args = {"one", "two", "three,four"};
		
		String test = args[2];
		String[] tlist = test.split(",");

		for (int i=0; i<tlist.length; i++) 
			System.out.println(tlist[i]);		
	}
	
	private void loadWordlist() {
		InputStream in = App.class.getResourceAsStream(File.separator + "wordlist");		
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
	
	private boolean checkWords(String text) {
		if (text == null || text.length() == 0)
			return false;

		text = text.toLowerCase();
		String[] words = text.split("\\W+");
		for (String word : words) {
			if (wordslist.floor(word) != null)
				return true;
		}

		return false;
	}	
		
	public void affectFilter() {
		loadWordlist();
			
    	String msg = "{\r\n    \"id\": 606305834759356400,\r\n    \"text\": \"...\",\r\n    \"created_at\": \"Thu Jun 04 03:45:50 +0000 2015\",\r\n    \"id_str\": \"606305834759356416\",\r\n    \"source\": \"<a href=\\\"http://twitter.com\\\" rel=\\\"nofollow\\\">Twitter Web Client</a>\",\r\n    \"truncated\": false,\r\n    \"in_reply_to_status_id\": 0,\r\n    \"in_reply_to_user_id\": 0,\r\n    \"retweet_count\": 0,\r\n    \"favorite_count\": 0,\r\n    \"favorited\": false,\r\n    \"retweeted\": false,\r\n    \"possibly_sensitive\": false,\r\n    \"filter_level\": \"low\",\r\n    \"lang\": \"en\",\r\n    \"user\": {\r\n        \"id\": 3122949415,\r\n        \"name\": \"Nikki Schwerdtfeger\",\r\n        \"screen_name\": \"Badger_Bitch14\",\r\n        \"location\": \"Amsterdam, North Holland\",\r\n        \"description\": \"ED SHEERAN \u274C| 88 \u274C |BRISTON\u2716\uFE0F|ENGLAND \u2716\uFE0F| ANCORA IMPARO|\",\r\n        \"_protected\": false,\r\n        \"followers_count\": 336,\r\n        \"friends_count\": 929,\r\n        \"listed_count\": 0,\r\n        \"created_at\": \"Wed Apr 01 04:32:49 +0000 2015\",\r\n        \"favourites_count\": 1266,\r\n        \"utc_offset\": \"-25200\",\r\n        \"time_zone\": \"Pacific Time (US & Canada)\",\r\n        \"geo_enabled\": true,\r\n        \"verified\": false,\r\n        \"statuses_count\": 2820,\r\n        \"lang\": \"en-gb\",\r\n        \"contributors_enabled\": false,\r\n        \"is_translator\": false,\r\n        \"is_translation_enabled\": false,\r\n        \"profile_background_color\": \"000000\",\r\n        \"profile_background_image_url\": \"http://pbs.twimg.com/profile_background_images/598014532917694464/to4KmBjx.jpg\",\r\n        \"profile_background_image_url_https\": \"https://pbs.twimg.com/profile_background_images/598014532917694464/to4KmBjx.jpg\",\r\n        \"profile_background_tile\": false,\r\n        \"profile_image_url\": \"http://pbs.twimg.com/profile_images/604690579675430913/o_IDNpTU_normal.jpg\",\r\n        \"profile_image_url_https\": \"https://pbs.twimg.com/profile_images/604690579675430913/o_IDNpTU_normal.jpg\",\r\n        \"profile_link_color\": \"4A913C\",\r\n        \"profile_sidebar_border_color\": \"000000\",\r\n        \"profile_sidebar_fill_color\": \"000000\",\r\n        \"profile_text_color\": \"000000\",\r\n        \"profile_use_background_image\": true,\r\n        \"default_profile\": false,\r\n        \"default_profile_image\": false,\r\n        \"follow_request_sent\": false,\r\n        \"captured_at\": \"Jun 3, 2015 8:45:49 PM\"\r\n    },\r\n    \"userid\": 0,\r\n    \"captured_at\": \"Jun 3, 2015 8:45:49 PM\",\r\n    \"captured_by\": \"stream\",\r\n    \"entities\": {\r\n        \"hashtags\": [],\r\n        \"user_mentions\": []\r\n    },\r\n    \"origin_code\": \"SG\"\r\n}";
    	Tweet tweet = JsonUtil.fromJson(msg, Tweet.class);
    	tweet.convertType();
    	
    	if (checkWords(tweet.getText())) {
    		System.out.println("Hello IF");
    	} else {
    		System.out.println("Hello ELSE");		
		}
	}
	
	public void insertIntoDB() {	
		System.out.println("Start Test");
		
    	GenericXmlApplicationContext rdbContext = new GenericXmlApplicationContext("database.xml");		
    	TweetDataRepository tdr = rdbContext.getBean(TweetDataRepository.class);		
    	    	
    	String msg = "{\r\n    \"id\": 606305834759356400,\r\n    \"text\": \"you want me to\",\r\n    \"created_at\": \"Thu Jun 04 03:45:50 +0000 2015\",\r\n    \"id_str\": \"606305834759356416\",\r\n    \"source\": \"<a href=\\\"http://twitter.com\\\" rel=\\\"nofollow\\\">Twitter Web Client</a>\",\r\n    \"truncated\": false,\r\n    \"in_reply_to_status_id\": 0,\r\n    \"in_reply_to_user_id\": 0,\r\n    \"retweet_count\": 0,\r\n    \"favorite_count\": 0,\r\n    \"favorited\": false,\r\n    \"retweeted\": false,\r\n    \"possibly_sensitive\": false,\r\n    \"filter_level\": \"low\",\r\n    \"lang\": \"en\",\r\n    \"user\": {\r\n        \"id\": 3122949415,\r\n        \"name\": \"Nikki Schwerdtfeger\",\r\n        \"screen_name\": \"Badger_Bitch14\",\r\n        \"location\": \"Amsterdam, North Holland\",\r\n        \"description\": \"ED SHEERAN \u274C| 88 \u274C |BRISTON\u2716\uFE0F|ENGLAND \u2716\uFE0F| ANCORA IMPARO|\",\r\n        \"_protected\": false,\r\n        \"followers_count\": 336,\r\n        \"friends_count\": 929,\r\n        \"listed_count\": 0,\r\n        \"created_at\": \"Wed Apr 01 04:32:49 +0000 2015\",\r\n        \"favourites_count\": 1266,\r\n        \"utc_offset\": \"-25200\",\r\n        \"time_zone\": \"Pacific Time (US & Canada)\",\r\n        \"geo_enabled\": true,\r\n        \"verified\": false,\r\n        \"statuses_count\": 2820,\r\n        \"lang\": \"en-gb\",\r\n        \"contributors_enabled\": false,\r\n        \"is_translator\": false,\r\n        \"is_translation_enabled\": false,\r\n        \"profile_background_color\": \"000000\",\r\n        \"profile_background_image_url\": \"http://pbs.twimg.com/profile_background_images/598014532917694464/to4KmBjx.jpg\",\r\n        \"profile_background_image_url_https\": \"https://pbs.twimg.com/profile_background_images/598014532917694464/to4KmBjx.jpg\",\r\n        \"profile_background_tile\": false,\r\n        \"profile_image_url\": \"http://pbs.twimg.com/profile_images/604690579675430913/o_IDNpTU_normal.jpg\",\r\n        \"profile_image_url_https\": \"https://pbs.twimg.com/profile_images/604690579675430913/o_IDNpTU_normal.jpg\",\r\n        \"profile_link_color\": \"4A913C\",\r\n        \"profile_sidebar_border_color\": \"000000\",\r\n        \"profile_sidebar_fill_color\": \"000000\",\r\n        \"profile_text_color\": \"000000\",\r\n        \"profile_use_background_image\": true,\r\n        \"default_profile\": false,\r\n        \"default_profile_image\": false,\r\n        \"follow_request_sent\": false,\r\n        \"captured_at\": \"Jun 3, 2015 8:45:49 PM\"\r\n    },\r\n    \"userid\": 0,\r\n    \"captured_at\": \"Jun 3, 2015 8:45:49 PM\",\r\n    \"captured_by\": \"stream\",\r\n    \"entities\": {\r\n        \"hashtags\": [],\r\n        \"user_mentions\": []\r\n    },\r\n    \"origin_code\": \"SG\"\r\n}";
    	Tweet tweet = JsonUtil.fromJson(msg, Tweet.class);
    	tweet.convertType();
    	    	
		TweetData entity = tdr.findOne(tweet.getId_str());
		if (entity == null) {
			entity = new TweetData();
			entity.setTweet_id(tweet.getId_str());
			entity.setContent(tweet.getText());
			entity.setTweet_created(new Timestamp(tweet.getCreated_time())); // this one!
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

		tdr.save(entity);  		
	}	
}