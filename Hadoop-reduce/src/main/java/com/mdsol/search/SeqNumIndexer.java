package com.mdsol.search;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;

public class SeqNumIndexer extends Configured implements Tool {

	/**
	 * Maps words from line of text into 2 key-value pairs; one key-value pair
	 * for counting the word, another for counting its length.
	 */
	public static class SequenceNumberMapper extends
			Mapper<Object, IntWritable, IntWritable, IntWritable> {

		/**
		 * Emits 2 key-value pairs for counting the word and its length. Outputs
		 * are (Text, LongWritable).
		 * 
		 * @param value
		 *            This will be a line of text coming in from our input file.
		 */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			while (itr.hasMoreTokens()) {
				String string = itr.nextToken();
				IntWritable seqNum = new IntWritable(new Integer(string));
				IntWritable index = new IntWritable(new Integer(string.substring(0, 3)));
				context.write(index, seqNum);
			}
		}
	}

	public static class SeqNumReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		private LongWritable sum = new LongWritable();

		/**
		 * Sums all the individual values within the iterator and writes them to
		 * the same key.
		 * 
		 * @param key
		 *            This will be one of 2 constants: LENGTH_STR or COUNT_STR.
		 * @param values
		 *            This will be an iterator of all the values associated with
		 *            that key.
		 */
		public void reduce(IntWritable index, Iterable<IntWritable> seqNums,
				Context context) throws IOException, InterruptedException {

			Set<Integer> sequenceNums = new HashSet<Integer>();
			for (IntWritable val : seqNums) {
				sequenceNums.add(val.get());
			}
			context.write(index, new Text(StringUtils.join(sequenceNums, ",")));
		}
	}

}
