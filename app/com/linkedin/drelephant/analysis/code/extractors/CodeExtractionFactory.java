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

package com.linkedin.drelephant.analysis.code.extractors;

import com.linkedin.drelephant.analysis.code.CodeExtractor;
import models.AppResult;
import org.apache.log4j.Logger;


/**
 * Factory class for code extractor . By default extractor is AzkabanJarvis
 * ,which will take extract file Name from Azkaban and Code from Jarvis
 */
public class CodeExtractionFactory {
  private static final Logger logger = Logger.getLogger(CodeExtractionFactory.class);

  public static CodeExtractor getCodeExtractor(AppResult appResult) {
    logger.info(" Default code extractor is AzkabanJarvis");
    return new AzkabanJarvisCodeExtractor();
  }
}
