package twitter.capturer.twitter;

import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

import data.common.domain.nosql.Tweet;
import data.common.util.JsonUtil;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.json.DataObjectFactory;

@SuppressWarnings("deprecation")
public class TwitterListener implements StatusListener {
	private LinkedBlockingQueue<String> _queue;

	public TwitterListener(LinkedBlockingQueue<String> queue) {
		_queue = queue;
	}

	public LinkedBlockingQueue<String> getQueue() {
		return _queue;
	}

	public void onException(Exception arg0) {
		// TODO Auto-generated method stub

	}

	public void onDeletionNotice(StatusDeletionNotice arg0) {
		// TODO Auto-generated method stub

	}

	public void onScrubGeo(long arg0, long arg1) {
		// TODO Auto-generated method stub

	}

	public void onStallWarning(StallWarning arg0) {
		// TODO Auto-generated method stub

	}

	public void onStatus(Status status) {
		String statusJson = DataObjectFactory.getRawJSON(status);
		Tweet tweet = JsonUtil.fromJson(statusJson, Tweet.class);
		Date date = new Date();
		tweet.setCaptured_at(date);
		tweet.setCaptured_by("stream");
		tweet.getUser().setCapturedAt(date);
		tweet.setOrigin_code("SG");

		statusJson = JsonUtil.toJson(tweet);
		_queue.offer(statusJson);
	}

	public void onTrackLimitationNotice(int arg0) {
		// TODO Auto-generated method stub

	}

}