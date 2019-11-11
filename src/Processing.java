import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

 
public class Processing { 

	public static void main(String[] args) throws Exception {
		
						
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
	    conf.set("mapreduce.output.textoutputformat.separator", ",");
		
		
		Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        
        /*Path tmp_c = new Path("/cleanData");
        Path tmp_p = new Path("/prepData");
        Path tmp1 = new Path("/temp1");
        Path tmp2 = new Path("/temp2");
        Path tmp3 = new Path("/temp3");*/
        
        Path tmp_c = new Path("/user/vagrant/cleanData");
        Path tmp_p = new Path("/user/vagrant/prepData");
        Path tmp1 = new Path("/user/vagrant/temp1");
        Path tmp2 = new Path("/user/vagrant/temp2");
        Path tmp3 = new Path("/user/vagrant/temp3");
        
        /* 
        cleanning 
        */
        
        Job job = Job.getInstance(conf,"cleaning");
        
		job.setJarByClass(Processing.class);
		job.setMapperClass(cleanMapper.class);
		job.setReducerClass(cleanReducer.class);
		
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, tmp_c);
		
		job.waitForCompletion(true);
		
		
		
		/* 
		 preparation 
		*/
		
		Job job_p = Job.getInstance(conf, "job2");
		job_p.setJarByClass(Processing.class);
		job_p.setJobName("SecondarySortDriver");
        	
		job_p.setMapperClass(prepMapper.class);
		
		job_p.setNumReduceTasks(0);
		
		job_p.setOutputKeyClass(Text.class);
		job_p.setOutputValueClass(Text.class);
		
		job_p.setInputFormatClass(TextInputFormat.class);
		job_p.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job_p, tmp_c);
		FileOutputFormat.setOutputPath(job_p,tmp_p);
		
		job_p.waitForCompletion(true);
		
		/* 
		 normalization
		*/
		
		/*
		  job 1
		*/
		Job job1 = Job.getInstance(conf, "job1");
        job1.setJarByClass(Processing.class);
        job1.setJobName("SecondarySortDriver1");
        
        job1.setMapOutputKeyClass(CompositeKey.class);
        job1.setMapOutputValueClass(Text.class);
           
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapperClass(NormalizeMapperOne.class);
        job1.setReducerClass(NormalizeReducerOne.class);
           
        job1.setPartitionerClass(CompositePartitioner.class);
        job1.setGroupingComparatorClass(CompositeGroupingComparator.class);
        
        FileInputFormat.setInputPaths(job1, tmp_p);
        FileOutputFormat.setOutputPath(job1, tmp1);
		
        job1.waitForCompletion(true);
        
        /*
		  job 2
		*/
      
	    Job job2 = Job.getInstance(conf, "job2");
      job2.setJarByClass(Processing.class);
      job2.setJobName("SecondarySortDriver");          

      job2.setMapOutputKeyClass(CompositeKeyDescend.class);
      job2.setMapOutputValueClass(Text.class);
         
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);

      job2.setMapperClass(NormalizeMapperTwo.class);
      job2.setReducerClass(NormalizeReducerTwo.class);
         
      job2.setPartitionerClass(CompositePartitioner.class);
      job2.setGroupingComparatorClass(CompositeGroupingComparator.class);
      
      FileInputFormat.setInputPaths(job2, tmp1);
      FileOutputFormat.setOutputPath(job2, tmp2);
		
      //System.exit(job2.waitForCompletion(true) ? 0 : 1);
      job2.waitForCompletion(true);
      fs.delete(tmp1, true);
  
	    /*
		  job 3
		*/
      Job job3 = Job.getInstance(conf, "job3");
      job3.setJarByClass(Processing.class);
      job3.setJobName("SecondarySortDriver");          

      job3.setMapOutputKeyClass(CompositeKey.class);
      job3.setMapOutputValueClass(Text.class);
         
      job3.setOutputKeyClass(Text.class);
      job3.setOutputValueClass(Text.class);

      job3.setMapperClass(NormalizeMapperThree.class);
      job3.setReducerClass(NormalizeReducerThree.class);
         
      job3.setPartitionerClass(CompositePartitioner.class);
      job3.setGroupingComparatorClass(CompositeGroupingComparator.class);
      
      FileInputFormat.setInputPaths(job3, tmp2);
      FileOutputFormat.setOutputPath(job3, tmp3);
		
      job3.waitForCompletion(true);
      fs.delete(tmp2, true);
      
      
      /*
		  job 4 final index
		*/
      
      Job job4 = Job.getInstance(conf,"final_index");
		job4.setJarByClass(Processing.class);
				
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		
		job4.setMapperClass(FinalIndexMapper.class);
		job4.setReducerClass(FinalIndexReducer.class);
		//job4.setNumReduceTasks(0);
		
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job4, tmp3);
		FileOutputFormat.setOutputPath(job4,outputPath);
      
      int exitCode = job4.waitForCompletion(true) ? 0:1;
      fs.delete(tmp3, true);
      System.exit(exitCode);
  }
}