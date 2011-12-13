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

/**
 * Delete a job
 */
public class DeleteDelegator extends TempletonDelegator {
    public DeleteDelegator(AppConfig appConf, ExecService execService) {
        super(appConf, execService);
    }

    public QueueStatusBean run(String user, String id)
        throws NotAuthorizedException, BadParam, IOException
    {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
        TempletonJobTracker tracker = null;
        try {
            tracker = new TempletonJobTracker(ugi,
                                              JobTracker.getAddress(appConf),
                                              appConf);
            JobID jobid = JobID.forName(id);
            tracker.killJob(jobid);
            JobStatus status = tracker.getJobStatus(jobid);
            JobProfile profile = tracker.getJobProfile(jobid);
            return new QueueStatusBean(status, profile);
        } catch (IllegalStateException e) {
            throw new BadParam(e.getMessage());
        } finally {
            if (tracker != null)
                tracker.close();
        }
    }
}
