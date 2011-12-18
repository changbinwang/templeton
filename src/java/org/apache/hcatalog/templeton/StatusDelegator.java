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
package org.apache.hcatalog.templeton;

import java.io.IOException;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobProfile;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.TempletonJobTracker;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hcatalog.templeton.tool.JobState;

/**
 * Fetch the status of a given job id in the queue.
 */
public class StatusDelegator extends TempletonDelegator {
    public StatusDelegator(AppConfig appConf, ExecService execService) {
        super(appConf, execService);
    }

    public QueueStatusBean run(String user, String id)
        throws NotAuthorizedException, BadParam, IOException
    {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
        TempletonJobTracker tracker = null;
        JobState state = null;
        try {
            tracker = new TempletonJobTracker(ugi,
                                              JobTracker.getAddress(appConf),
                                              appConf);
            JobID jobid = JobID.forName(id);
            state = new JobState(id, appConf);
            return StatusDelegator.makeStatus(tracker, jobid, state);
        } catch (IllegalStateException e) {
            throw new BadParam(e.getMessage());
        } finally {
            if (tracker != null)
                tracker.close();
            if (state != null)
                state.close();
        }
    }

    public static QueueStatusBean makeStatus(TempletonJobTracker tracker,
                                             JobID jobid,
                                             String childid,
                                             JobState state)
        throws IOException
    {
        JobID bestid = jobid;
        if (childid != null)
            bestid = JobID.forName(childid);

        JobStatus status = tracker.getJobStatus(bestid);
        JobProfile profile = tracker.getJobProfile(bestid);
        return new QueueStatusBean(state, status, profile);
    }

    public static QueueStatusBean makeStatus(TempletonJobTracker tracker,
                                             JobID jobid,
                                             JobState state)
        throws IOException
    {
        return makeStatus(tracker, jobid, state.getChildId(), state);
    }

}
