package cn.com.dtmobile.hadoop.biz.exception.mr;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import cn.com.dtmobile.hadoop.constants.TablesConstants;
import cn.com.dtmobile.hadoop.util.StringUtils;

/**
 * @完成功能:将回填Etype功能Map合并
 * @创建时间:2017年3月27日 上午9:38:27
 * @param
 */
public class CommnoProcessMap extends Mapper<Object, Text, Text, Text> {
	private final Text key = new Text();

	@Override
	protected void map(Object inKey, Text value,
			Context context)
			throws IOException, InterruptedException {
		Path path = null;
		path = ((FileSplit) (context.getInputSplit())).getPath();
		// 判断是否是GxRx
		if (path != null && path.toString().toUpperCase().contains(TablesConstants.HIVE_GXRX)) {
			if (value.getLength() > 0) {
				String[] words = value.toString().split(StringUtils.DELIMITER_INNER_ITEM,-1);
				if (words.length > 24) {
					if(StringUtils.isNotEmpty(words[5])){
						key.set(words[5]);
						context.write(key, new Text(value+ StringUtils.DELIMITER_BETWEEN_ELEMENT + TablesConstants.HIVE_GXRX));
					}
				}
			}        // 判断是否是MW
		}else if (path != null && path.toString().toUpperCase().contains(TablesConstants.HIVE_MW)) {
			if (value.getLength() > 0) {
				String[] words = value.toString().split(StringUtils.DELIMITER_INNER_ITEM, -1);
				if (words.length >= 67) {
						if(StringUtils.isNotEmpty(words[5])){
							key.set(words[5]);
							context.write(key, new Text(value+ StringUtils.DELIMITER_BETWEEN_ELEMENT+ TablesConstants.HIVE_MW));
					}
				}
			} 	// 判断是否是S1mme
		}else if (path != null && path.toString().toUpperCase().contains(TablesConstants.HIVE_S1MME)) {
			if (value.getLength() > 0) {
				String[] values = value.toString().split(StringUtils.DELIMITER_INNER_ITEM,-1);
				if (values.length > 36) {
					if(StringUtils.isNotEmpty(values[5])){
						key.set(values[5]);
						context.write(key, new Text(value+ StringUtils.DELIMITER_BETWEEN_ELEMENT+ TablesConstants.HIVE_S1MME));
					}
				}
			}// 判断是否是UU
		}else if (path != null && path.toString().toUpperCase().contains(TablesConstants.HIVE_UU)) {
			if (value.getLength() > 0) {
				String[] words = value.toString().split(StringUtils.DELIMITER_COMMA,-1);
				if (words.length > 26) {
					if (StringUtils.isNotEmpty(words[5])) {
						key.set(words[5]);
						context.write(key, new Text(value+ StringUtils.DELIMITER_BETWEEN_ELEMENT+ TablesConstants.HIVE_UU));
					}
				}
			}
			// 判断是否是X2
		}else if (path != null && path.toString().toUpperCase().contains(TablesConstants.HIVE_X2)) {
			if (value.getLength() > 0) {
				String[] words = value.toString().split(StringUtils.DELIMITER_COMMA,-1);
				if (words.length > 20) {
					if (StringUtils.isNotEmpty(words[5])) {
							key.set(words[5]);
							context.write(key, new Text(value+ StringUtils.DELIMITER_BETWEEN_ELEMENT+ TablesConstants.HIVE_X2));
					}
				}
			}

		}else{
			return ;
		}
		
		

	}

}
