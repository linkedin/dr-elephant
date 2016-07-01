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

package controllers;

/**
 * Created by skakker on 09/06/16.
 */
class HtmlUtil {
  /**
   * Make the class constructor private for this utility class.
   */
  private HtmlUtil() {
  }

  /**
   * Extract the HTML tags from the given input to make the text content more readable.
   *
   * @param html The HTML whose content we want to make more readable
   * @return The text content extracted from the HTML
   */
  public static String toText(String html) {
    String newline = System.getProperty("line.separator");
    return html == null ? "" : html.replaceAll("<br/>", newline)
        .replaceAll("<ul>", newline + newline)
        .replaceAll("</ul>", newline)
        .replaceAll("<li>", "* ")
        .replaceAll("</li>", newline);
  }
}