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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A Map Reduce job that will start another job.
 */
public class TempletonQueuerJob extends Configured implements Tool {
    public static final String COPY_NAME      = "templeton.copy";
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
            copyLocal(COPY_NAME, conf);
            String[] jarArgs
                = TempletonUtils.decodeArray(conf.get(JAR_ARGS_NAME));

            return execService.run(Arrays.asList(jarArgs));
        }

        private void copyLocal(String var, Configuration conf)
            throws IOException
        {
            String[] filenames = TempletonUtils.decodeArray(conf.get(var));
            if (filenames != null) {
                for (String filename : filenames) {
                    Path src = new Path(filename);
                    Path dst = new Path(src.getName());
                    FileSystem fs = src.getFileSystem(conf);
                    System.err.println("templeton: copy " + src + " => " + dst);
                    fs.copyToLocalFile(src, dst);
                }
            }
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
                System.err.println("templeton: Writing status to " + p);
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
                System.err.println("templeton: execute error: " + e);
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

    /**
     * Enqueue the job and print out the job id for later collection.
     */
    @Override
    public int run(String[] args)
        throws IOException, InterruptedException, ClassNotFoundException
    {
        Configuration conf = getConf();
        conf.set(COPY_NAME, TempletonUtils.decodeCliArg(args[0]));
        conf.set(STATUSDIR_NAME, TempletonUtils.decodeCliArg(args[1]));
        String[] childArgs = Arrays.copyOfRange(args, 2, args.length);
        conf.set(JAR_ARGS_NAME, TempletonUtils.encodeArray(childArgs));
        Job job = new Job(conf);
        job.setJarByClass(TempletonQueuerJob.class);
        job.setJobName("TempletonQueuerJob");
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
        int ret = ToolRunner.run(new TempletonQueuerJob(), args);
        if (ret != 0)
            System.err.println("TempletonQueuerJob failed!");
        System.exit(ret);
    }
}
