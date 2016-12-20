/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.configurations.jobtype;

import com.linkedin.drelephant.analysis.ApplicationType;
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.linkedin.drelephant.analysis.JobType;


/**
 * This class manages the job type configurations
 */
public class JobTypeConfiguration {
  private static final Logger logger = Logger.getLogger(JobTypeConfiguration.class);
  private static final int TYPE_LEN_LIMIT = 20;

  private Map<ApplicationType, List<JobType>> _appTypeToJobTypeList = new HashMap<ApplicationType, List<JobType>>();

  public JobTypeConfiguration(Element configuration) {
    parseJobTypeConfiguration(configuration);
  }

  public Map<ApplicationType, List<JobType>> getAppTypeToJobTypeList() {
    return _appTypeToJobTypeList;
  }

  private void parseJobTypeConfiguration(Element configuration) {

    Map<ApplicationType, JobType> defaultMap = new HashMap<ApplicationType, JobType>();

    NodeList nodes = configuration.getChildNodes();
    int n = 0;
    for (int i = 0; i < nodes.getLength(); i++) {
      Node node = nodes.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        n++;
        Element jobTypeNode = (Element) node;

        String jobTypeName;
        Node jobTypeNameNode = jobTypeNode.getElementsByTagName("name").item(0);
        if (jobTypeNameNode == null) {
          throw new RuntimeException("No tag 'jobtype' in jobtype " + n);
        }
        jobTypeName = jobTypeNameNode.getTextContent();
        if (jobTypeName.equals("")) {
          throw new RuntimeException("Empty tag 'jobtype' in jobtype " + n);
        }
        // Truncate jobtype length for db constraint
        if (jobTypeName.length() > TYPE_LEN_LIMIT) {
          logger.info("Truncate type " + jobTypeName.length());
          jobTypeName = jobTypeName.substring(0, TYPE_LEN_LIMIT);
        }

        String jobConfName;
        Node jobConfNameNode = jobTypeNode.getElementsByTagName("conf").item(0);
        if (jobConfNameNode == null) {
          throw new RuntimeException("No tag 'conf' in jobtype " + jobTypeName);
        }
        jobConfName = jobConfNameNode.getTextContent();
        if (jobConfName.equals("")) {
          throw new RuntimeException("Empty tag 'conf' in jobtype " + jobTypeName);
        }

        String jobConfValue;
        Node jobConfValueNode = jobTypeNode.getElementsByTagName("value").item(0);
        if (jobConfValueNode == null) {
          // Default regex. match any char one or more times
          jobConfValue = ".*";
        } else {
          jobConfValue = jobConfValueNode.getTextContent();
          if (jobConfValue.equals("")) {
            jobConfValue = ".*";
          }
        }

        String appTypeName;
        Node appTypeNameNode = jobTypeNode.getElementsByTagName("applicationtype").item(0);
        if (appTypeNameNode == null) {
          throw new RuntimeException("No tag 'applicationtype' in jobtype " + jobTypeName);
        }
        appTypeName = appTypeNameNode.getTextContent();
        ApplicationType appType = new ApplicationType(appTypeName);

        boolean isDefault = jobTypeNode.getElementsByTagName("isDefault").item(0) != null;

        JobType newJobType = null;
        try {
          newJobType = new JobType(jobTypeName, jobConfName, jobConfValue);
        } catch (PatternSyntaxException e) {
          throw new RuntimeException(
              "Error processing this pattern.  Pattern:" + jobConfValue + " jobtype:" + jobTypeName);
        }

        String newJobTypeStr = String
            .format("jobType:%s, for application type:%s, isDefault:%s, confName:%s, confValue:%s.", jobTypeName,
                appTypeName, isDefault, jobConfName, jobConfValue);
        logger.info("Loaded " + newJobTypeStr);

        if (isDefault) {
          if (defaultMap.containsKey(appType)) {
            throw new RuntimeException(
                "Each application type should have one and only one default job type. Duplicate default job type: "
                    + newJobTypeStr + " for application type: " + appType.getName());
          } else {
            defaultMap.put(appType, newJobType);
          }
        } else {
          List<JobType> jobTypes = getJobTypeList(appType);
          jobTypes.add(newJobType);
        }
      }
    }

    // Append default maps to the end of each job type list
    for (Map.Entry<ApplicationType, JobType> entry : defaultMap.entrySet()) {
      ApplicationType appType = entry.getKey();
      JobType jobType = entry.getValue();
      List<JobType> jobTypes = getJobTypeList(appType);
      jobTypes.add(jobType);
    }

    // Sanity check
    for(ApplicationType appType : _appTypeToJobTypeList.keySet()) {
      if (!defaultMap.containsKey(appType)) {
        throw new RuntimeException("Each application type should have one and only one default job type, there is"
            + " none for application type: " + appType.getName() + ". Use <isDefault/> to tag one.");
      }
    }

    Integer jobTypesSize = 0;
    for (List<JobType> jobTypes : _appTypeToJobTypeList.values() ) {
      jobTypesSize += jobTypes.size();
    }
    logger.info("Loaded total " + jobTypesSize + " job types for " + _appTypeToJobTypeList.size() + " app types");
  }

  private List<JobType> getJobTypeList(ApplicationType appType) {
    List<JobType> jobTypes = _appTypeToJobTypeList.get(appType);
    if (jobTypes == null) {
      jobTypes = new ArrayList<JobType>();
      _appTypeToJobTypeList.put(appType, jobTypes);
    }
    return jobTypes;
  }
}
