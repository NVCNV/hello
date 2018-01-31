package cn.com.dtmobile.hadoop.biz.train.mr.votleuser;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.com.dtmobile.hadoop.model.MwXdr;
import cn.com.dtmobile.hadoop.util.StringUtils;

;

public class VotleUserMap extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		Text mk = new Text();
		Text mv = new Text();

		String[] line = value.toString()
				.split(StringUtils.DELIMITER_INNER_ITEM);

		SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
		Date date = new Date();
		String curDate = format.format(date);
		if (line.length >= 66 && line[2].length() == 2) {
			MwXdr mwXdr = new MwXdr(line);
			if (!mwXdr.getImsi().isEmpty()) {
				mk.set(mwXdr.getImsi());
				mv.set(mwXdr.getImsi() + StringUtils.DELIMITER_COMMA
						+ StringUtils.DELIMITER_COMMA + curDate
						+ StringUtils.ARGS_DELIMITER
						+ context.getConfiguration().get("hour"));
				context.write(mk, mv);
			}
		}

	}

}
