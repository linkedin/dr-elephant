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

import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import com.linkedin.drelephant.DrElephant;
import com.linkedin.drelephant.ElephantContext;
import com.sun.security.sasl.util.AbstractSaslImpl;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import play.Application;
import play.GlobalSettings;
import play.Logger;
import play.api.mvc.Handler;
import play.mvc.Http;


/**
 * This class manages all the global settings
 */
public class Global extends GlobalSettings {

  DrElephant _drElephant;

  private final String INVALID_PORT = "-1";
  private final String HTTPS_PROTOCOL = "https://";
  private final String HTTP_PORT_CONFIG_NAME = "http_port";
  private final String HTTPS_PORT_CONFIG_NAME = "https_port";

  private String HTTP_PORT;
  private String HTTPS_PORT;

  public void onStart(Application app) {
    Logger.info("Starting Application...");

    fixJavaKerberos();

    HTTP_PORT = ElephantContext.instance().getElephnatConf()
        .getProperty(HTTP_PORT_CONFIG_NAME);
    HTTPS_PORT = ElephantContext.instance().getElephnatConf()
        .getProperty(HTTPS_PORT_CONFIG_NAME);

    try {
      _drElephant = new DrElephant();
      _drElephant.start();
    } catch (IOException e) {
      Logger.error("Application start failed...", e);
    }
  }

  public void onStop(Application app) {
    Logger.info("Stopping application...");
    if (_drElephant != null) {
      _drElephant.kill();
    }
  }

  /**
   * This hack is done in order to fix a problem in Java 1.6 when using Kerberos
   * <p/>
   * Specific error:
   * java.lang.NullPointerException
   * at com.sun.security.sasl.util.AbstractSaslImpl.traceOutput(AbstractSaslImpl.java:241)
   * at com.sun.security.sasl.gsskerb.GssKrb5Client.evaluateChallenge(GssKrb5Client.java:180)
   */
  private static void fixJavaKerberos() {
    try {
      Field loggerField = AbstractSaslImpl.class.getDeclaredField("logger");
      loggerField.setAccessible(true);

      java.util.logging.Logger logger = (java.util.logging.Logger) loggerField.get(null);
      if (logger == null) {
        logger = java.util.logging.Logger.getLogger("javax.security.sasl");
        setFinalStatic(loggerField, logger);
      }
      //Prevent the code in GssKrb5Client.evaluateChallenge to call traceOutput()
      logger.setLevel(Level.OFF);
    } catch (Exception e) {
      Logger.error("Error trying to fix Kerberos connection", e);
    }

  }

  /**
   * This method is overridden to re-direct all the requests to HTTP endpoint of Dr.Elephant to
   * the respective HTTPS enabled endpoint(If such endpoints exists)
   * @param requestHeader Request Header
   * @return The Handler to appropriately handle the given request
   */
  @Override
  public Handler onRouteRequest(Http.RequestHeader requestHeader) {
    if (!Strings.isNullOrEmpty(HTTP_PORT) && !Strings.isNullOrEmpty(HTTPS_PORT)) {
      String port = INVALID_PORT;
      if (!Strings.isNullOrEmpty(requestHeader.host())) {
        HostAndPort hostAndPort = HostAndPort.fromString(requestHeader.host())
            .withDefaultPort(Integer.parseInt(HTTP_PORT));
        port = String.valueOf(hostAndPort.getPort());
      }
      if (!port.equals(INVALID_PORT) && port.equals(HTTP_PORT)) {
        return controllers.Default.redirect(HTTPS_PROTOCOL + requestHeader.host().replace(port, HTTPS_PORT)
            + requestHeader.uri());
      }
    }
    return super.onRouteRequest(requestHeader);
  }

  static void setFinalStatic(Field field, Object newValue) throws Exception {
    field.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    field.set(null, newValue);
  }
}
