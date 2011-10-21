/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hcatalog.templeton.tool;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Submit a jar job from the command line.  The default Hadoop tool
 * submits the job and waits for completion and we want to just put it
 * in the queue and return a job id.
 */
public class TempletonJarJob extends Configured implements Tool {
    public static final String HADOOP
        = System.getenv("HADOOP_HOME") + "/bin/hadoop";

    public static final String JAR_NAME       = "templeton.jar";
    public static final String CLASS_NAME     = "templeton.class";
    public static final String STATUSDIR_NAME = "templeton.statusdir";
    public static final String JAR_ARGS_NAME  = "templeton.args";

    public static final int WATCHER_TIMEOUT_SECS = 10;

    private static TrivialExecService execService = TrivialExecService.getInstance();

    public static class LaunchMapper
        extends Mapper<NullWritable, NullWritable, Text, Text>
    {
        protected Process startJob(Context context)
            throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();
            Path jar = copyLocal(JAR_NAME, conf);
            String mainClass = conf.get(CLASS_NAME);
            String[] jarArgs = splitAndUnEscape(conf.get(JAR_ARGS_NAME));

            ArrayList<String> cmd = new ArrayList<String>();
            cmd.add(HADOOP);
            cmd.add("jar");
            cmd.add(jar.getName());
            if (TempletonUtils.isset(mainClass))
                cmd.add(mainClass);
            if (TempletonUtils.isset(jarArgs))
                cmd.addAll(Arrays.asList(jarArgs));

            return execService.run(cmd);
        }

        private Path copyLocal(String var, Configuration conf)
            throws IllegalArgumentException, IOException
        {
            String filename = conf.get(var);
            if (filename == null)
                throw new IllegalArgumentException("Missing param " + var);
            Path src = new Path(filename);
            Path dst = new Path(src.getName());
            FileSystem fs = src.getFileSystem(conf);
            System.err.println("--- copy " + src + " => " + dst);
            fs.copyToLocalFile(src, dst);

            return dst;
        }

        @Override
        public void run(Context context)
            throws IOException, InterruptedException
        {
            Process proc = startJob(context);

            Configuration conf = context.getConfiguration();
            String statusdir = conf.get(STATUSDIR_NAME);

            ExecutorService pool = Executors.newCachedThreadPool();
            executeWatcher(pool, conf, proc.getInputStream(), statusdir, "stdout");
            executeWatcher(pool, conf, proc.getErrorStream(), statusdir, "stderr");

            proc.waitFor();
            pool.shutdown();
            if (! pool.awaitTermination(WATCHER_TIMEOUT_SECS, TimeUnit.SECONDS))
                pool.shutdownNow();
        }

        private void executeWatcher(ExecutorService pool, Configuration conf,
                                    InputStream in, String statusdir, String name)
            throws IOException
        {
            Watcher w = new Watcher(conf, in, statusdir, name);
            pool.execute(w);
        }
    }

    public static class Watcher implements Runnable {
        private InputStream in;
        private OutputStream out;

        public Watcher(Configuration conf, InputStream in,
                       String statusdir, String name)
            throws IOException
        {
            this.in = in;

            if (name.equals("stderr"))
                out = System.err;
            else
                out = System.out;

            if (TempletonUtils.isset(statusdir)) {
                Path p = new Path(statusdir, name);
                FileSystem fs = p.getFileSystem(conf);
                out = fs.create(p);
                System.err.println("--- Writing status to " + p);
            }
        }

        @Override
        public void run() {
            try {
                byte[] buf = new byte[512];
                int len = 0;
                while ((len = in.read(buf)) >= 0)
                    out.write(buf, 0, len);
            } catch (IOException e) {
                System.err.println("--- execute error: " + e);
            }
        }
    }

    public static class MonitorReducer
        extends Reducer<Text, Text, NullWritable, NullWritable>
    {
        @Override
        public void reduce(Text name, Iterable<Text> values, Context context)
            throws IOException
        {
        }
    }


    public static class NullSplit extends InputSplit implements Writable {
        public long getLength() { return 0; }

        public String[] getLocations() throws IOException {
            return new String[]{};
        }

        @Override
        public void write(DataOutput out) throws IOException {}

        @Override
        public void readFields(DataInput in) throws IOException {}
    }

    public static class NullRecordReader
        extends RecordReader<NullWritable, NullWritable>
    {
        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException
        {}

        @Override
        public void close() throws IOException {}

        @Override
        public NullWritable getCurrentKey() {
            return NullWritable.get();
        }

        @Override
        public NullWritable getCurrentValue() {
            return NullWritable.get();
        }

        @Override
        public float getProgress() { return 1.0f; }

        @Override
        public boolean nextKeyValue() throws IOException {
            return false;
        }
    }

    public static class SingleInputFormat
        extends InputFormat<NullWritable, NullWritable>
    {
        public List<InputSplit> getSplits(JobContext job)
            throws IOException
        {
            List<InputSplit> res = new ArrayList<InputSplit>();
            res.add(new NullSplit());
            return res;
        }

        public RecordReader<NullWritable, NullWritable>
            createRecordReader(InputSplit split,
                               TaskAttemptContext context)
            throws IOException
        {
            return new NullRecordReader();
        }
    }

    public static String joinAndEscape(String[] plain) {
        String[] escaped = new String[plain.length];

        for (int i = 0; i < plain.length; ++i)
            escaped[i] = StringUtils.escapeString(plain[i]);

        return StringUtils.arrayToString(escaped);
    }

    public static String[] splitAndUnEscape(String s) {
        String[] escaped = StringUtils.split(s);
        String[] plain = new String[escaped.length];

        for (int i = 0; i < escaped.length; ++i)
            plain[i] = StringUtils.unEscapeString(escaped[i]);

        return plain;
    }

    private String cleanCliArg(String s) {
        if (s.startsWith("*"))
            return s.substring(1);
        else
            return s;
    }

    /**
     * Enqueue the job and print out the job id for later collection.
     */
    @Override
    public int run(String[] args)
        throws IOException, InterruptedException, ClassNotFoundException
    {
        Configuration conf = getConf();
        conf.set(JAR_NAME, args[0]);
        conf.set(CLASS_NAME, cleanCliArg(args[1]));
        conf.set(STATUSDIR_NAME, cleanCliArg(args[2]));
        String[] childArgs = Arrays.copyOfRange(args, 3, args.length);
        conf.set(JAR_ARGS_NAME, joinAndEscape(childArgs));
        Job job = new Job(conf);
        job.setJarByClass(TempletonJarJob.class);
        job.setJobName("TempletonJarJob");
        job.setMapperClass(LaunchMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(SingleInputFormat.class);
        NullOutputFormat<NullWritable, NullWritable> of
            = new NullOutputFormat<NullWritable, NullWritable>();
        job.setOutputFormatClass(of.getClass());
        job.setReducerClass(MonitorReducer.class);
        job.setNumReduceTasks(1);

        job.submit();
        TempletonUtils.printTaggedJobID(job.getJobID());
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new TempletonJarJob(), args);
        if (ret != 0)
            System.err.println("TempletonJarJob failed!");
        System.exit(ret);
    }
}
