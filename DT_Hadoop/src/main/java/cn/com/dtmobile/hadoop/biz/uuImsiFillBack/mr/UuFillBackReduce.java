package cn.com.dtmobile.hadoop.biz.uuImsiFillBack.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cn.com.dtmobile.hadoop.constants.TablesConstants;
import cn.com.dtmobile.hadoop.biz.uuImsiFillBack.model.S1mmeXdr;
import cn.com.dtmobile.hadoop.biz.uuImsiFillBack.model.UuXdr;
import cn.com.dtmobile.hadoop.util.StringUtils;

public class UuFillBackReduce extends Reducer<Text, Text, NullWritable, Text> {

	@SuppressWarnings("unchecked")
	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,
			Context arg2)
			throws IOException, InterruptedException {

		List<S1mmeXdr> s1mmeXdrList = new ArrayList<S1mmeXdr>();
		List<UuXdr> uuXdrList = new ArrayList<UuXdr>();
		List<UuXdr> containImsi = new ArrayList<UuXdr>();

		for (Text value : arg1) {
			if (value.toString().contains(TablesConstants.TABLE_UU_XDR)) {
				String uu = value.toString().split(
						StringUtils.DELIMITER_INNER_ITEM1)[0];
				String[] uu2 = uu.split(StringUtils.DELIMITER_INNER_ITEM);
				
				if (uu2.length >= 26) {
					UuXdr uuXdr = new UuXdr(uu2);
					if (!(uuXdr.getImsi().isEmpty() && uuXdr.getInterfaces() == 1
							&& uuXdr.getProcedureType().equals("1") && Integer
							.valueOf(uuXdr.getProcedureStatus()) > 0)) {

						containImsi.add(uuXdr);

					} else {
						uuXdrList.add(uuXdr);
					}

				}

			} else if (value.toString().contains(TablesConstants.TABLE_S1MME_XDR)) {
				String s1mme = value.toString().split(StringUtils.DELIMITER_INNER_ITEM0)[0];
				String[] s1mme2 = s1mme.split(StringUtils.DELIMITER_INNER_ITEM);
				S1mmeXdr s1mmeXdr = new S1mmeXdr(s1mme2);
				s1mmeXdrList.add(s1mmeXdr);
			}
		}

		Collections.sort(s1mmeXdrList, new Comparator<S1mmeXdr>() {
			@Override
			public int compare(S1mmeXdr a, S1mmeXdr b) {
				long timeOne = Long.valueOf(a.getProcedureStartTime());
				long timeTwo = Long.valueOf(b.getProcedureStartTime());
				return (int) -(timeOne - timeTwo);
			}

		});

		
		
		List<UuXdr> tmpUU = new ArrayList<UuXdr>();

		for (UuXdr uu : uuXdrList) {
			/**
			 long rangeTime = 0;
			 if (uu.getImsi() == null && uu.getInterfaces() == 1 &&
			 uu.getProcedureType().equals("1")
			 && Integer.valueOf(uu.getProcedureStatus()) > 0) {
			 rangeTime = uu.getRangeTime();

			 if (rangeTime-3600<=uu.getRangeTime()) {
			 rangeTime++;
			*/
			
			boolean bFind = false;

			for (S1mmeXdr s1mme : s1mmeXdrList) {

				if (s1mme.getCommXdr().getImsi() != null
						&& uu.getmTmsi().equals(s1mme.getmTmsi())) {
					uu.setImsi(s1mme.getCommXdr().getImsi());
					tmpUU.add(uu);
					bFind = true;
					break;
				} else if (s1mme.getCommXdr().getImsi() != null
						&& uu.getmTmsi().equals(s1mme.getOldMtmsi())) {
					uu.setImsi(s1mme.getCommXdr().getImsi());
					tmpUU.add(uu);
					bFind = true;
					break;
				}
			}
			if (!bFind) {
				tmpUU.add(uu);
			}

		}

		/**
		 }
		 Collections.sort(tmpUU, new Comparator<UuXdr>() {
		 @Override
		 public int compare(UuXdr a, UuXdr b) {
		 long timeOne = Long.valueOf(a.getRangeTime());
		 long timeTwo = Long.valueOf(b.getRangeTime());
		 return (int) (timeOne - timeTwo);
		 }
		 });

		 UuXdr endUU = null;
		 if (tmpUU.size() > 0) {
		 endUU = tmpUU.get(tmpUU.size() - 1);
		 arg2.write(NullWritable.get(), new Text(endUU.toString2()));
		 } else {
		
		 }
	*/	
		
		tmpUU.addAll(containImsi);
		Collections.sort(tmpUU, new Comparator<UuXdr>() {
			@Override
			public int compare(UuXdr a, UuXdr b) {
				long timeOne = Long.valueOf(a.getProcedureStartTime());
				long timeTwo = Long.valueOf(b.getProcedureStartTime());
				return (int) (timeOne - timeTwo);
			}
		});

		for (UuXdr uu : tmpUU) {
			arg2.write(NullWritable.get(), new Text(uu.toString2()));
		}

	}

}
