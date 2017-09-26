package twitter.capturer.twitter;

import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import data.common.domain.nosql.Tweet;
import data.common.util.JsonUtil;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;

public class TwitterListener implements StatusListener {
	private Logger log = Logger.getLogger(this.getClass());
	private LinkedBlockingQueue<String> queue;

	public TwitterListener(LinkedBlockingQueue<String> queue) {
		this.queue = queue;
	}

	public LinkedBlockingQueue<String> getQueue() {
		return queue;
	}

	public void onException(Exception arg0) {
		log.debug("onException: " + arg0.getMessage());
	}

	public void onDeletionNotice(StatusDeletionNotice arg0) {
		log.debug("onDeletionNotice: " + arg0.getStatusId());
	}

	public void onScrubGeo(long arg0, long arg1) {
		log.debug("onScrubGeo: " + arg0 + ", " + arg1);
	}

	public void onStallWarning(StallWarning arg0) {
		log.debug("onStallWarning: " + arg0.getMessage());
	}

	public void onTrackLimitationNotice(int arg0) {
		log.debug("onTrackLimitationNotice: " + arg0);
	}

	public void onStatus(Status status) {
		String statusJson = TwitterObjectFactory.getRawJSON(status);
		Tweet tweet = JsonUtil.fromJson(statusJson, Tweet.class);
		Date date = new Date();
		tweet.setCaptured_at(date);
		tweet.setCaptured_by("stream");
		tweet.getUser().setCapturedAt(date);
		tweet.setOrigin_code("SG");

		statusJson = JsonUtil.toJson(tweet);
		boolean result = queue.offer(statusJson);
		log.debug("queue offer result: " + result);
	}
}