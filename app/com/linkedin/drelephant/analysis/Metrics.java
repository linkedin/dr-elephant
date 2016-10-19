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

package com.linkedin.drelephant.analysis;

public enum Metrics {

    // Currently supported metrics
    USED_RESOURCES("Used Resources", "resources", "The resources used by the job"),
    WASTED_RESOURCES("Wasted Resources", "resources", "The resources wasted by the job"),
    RUNTIME("Run Time", "time", "The run time of the job"),
    WAIT_TIME("Wait Time", "time", "The wait time of the job");

    private String text;
    private String type;
    private String description;

    Metrics(String text, String type, String description) {
        this.text = text;
        this.type = type;
        this.description = description;
    }

    /**
     * Returns the value of the text for the metrics
     * @return The text value
     */
    public String getText() {
        return text;
    }

    /**
     * Returns the type of the metrics. It can be one of resources or time
     * @return The type of the metrics.
     */
    public String getType() {
        return type;
    }

    /**
     * Returns the description of the metrics
     * @return The description of the metrics
     */
    public String getDescription() {
        return description;
    }
}
