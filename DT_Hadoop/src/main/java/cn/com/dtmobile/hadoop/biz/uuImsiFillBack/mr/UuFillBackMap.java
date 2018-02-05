package cn.com.dtmobile.hadoop.biz.uuImsiFillBack.mr;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import cn.com.dtmobile.hadoop.constants.TablesConstants;
import cn.com.dtmobile.hadoop.biz.uuImsiFillBack.model.S1mmeXdr;
import cn.com.dtmobile.hadoop.biz.uuImsiFillBack.model.UuXdr;
import cn.com.dtmobile.hadoop.util.StringUtils;

public class UuFillBackMap extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		Text mk = new Text();
		Text mv = new Text();
		Path p = ((FileSplit) context.getInputSplit()).getPath();

		if (p.toString().toUpperCase().contains(TablesConstants.TABLE_UU_XDR)) {
			String[] uu = value.toString().split(StringUtils.DELIMITER_COMMA);

			if (uu != null && uu.length >= 61) {
				UuXdr uuXdr = new UuXdr(uu);
//				 if (uuXdr.getmTmsi() != null ) {
				mk.set(uuXdr.getmTmsi());
				mv.set(uuXdr.toString() + StringUtils.DELIMITER_INNER_ITEM1
						+ TablesConstants.TABLE_UU_XDR);
//				 }
			}
		} else if (p.toString().toUpperCase()
				.contains(TablesConstants.TABLE_S1MME_XDR)) {
			String[] s1mme = value.toString().split(
					StringUtils.DELIMITER_INNER_ITEM);
			if (s1mme != null && s1mme.length >= 37) {
				S1mmeXdr s1mmeXdr = new S1mmeXdr(s1mme);
				if (s1mmeXdr.getmTmsi() != null) {
					mk.set(s1mmeXdr.getmTmsi());
				} else if (s1mmeXdr.getOldMtmsi() != null) {
					mk.set(s1mmeXdr.getOldMtmsi());
				}

				mv.set(s1mmeXdr.toString() + StringUtils.DELIMITER_INNER_ITEM0
						+ TablesConstants.TABLE_S1MME_XDR);
			}
		}
		context.write(mk, mv);
	}
}
