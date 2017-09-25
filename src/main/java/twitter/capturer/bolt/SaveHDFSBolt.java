package twitter.capturer.bolt;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

public class SaveHDFSBolt {
	private HdfsBolt _bolt;
		
	public SaveHDFSBolt(String tenantFolder) throws ConfigurationException {
		PropertiesConfiguration hdfsProperties = new PropertiesConfiguration("hdfs.setting");
		
		String hdfsUrl = hdfsProperties.getString("hdfs.url");
	    String fileExt = hdfsProperties.getString("file.ext");
	    String fileDelim = hdfsProperties.getString("file.delim");
	    int syncCount = Integer.parseInt(hdfsProperties.getString("sync.count"));
	    Float maxFileSize = Float.parseFloat(hdfsProperties.getString("max.file.size"));				
	    	    
	    FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(tenantFolder).withExtension(fileExt);			
	    RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(fileDelim);
	    SyncPolicy syncPolicy = new CountSyncPolicy(syncCount);
	    FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(maxFileSize, Units.MB);
	    
	    _bolt = new HdfsBolt()        
	        .withConfigKey("hdfs.config")
	        .withFsUrl(hdfsUrl)
	        .withFileNameFormat(fileNameFormat)
	        .withRecordFormat(format)
	        .withRotationPolicy(rotationPolicy)
	        .withSyncPolicy(syncPolicy);
	}

	public HdfsBolt bolt() {
		return _bolt;
	}
}