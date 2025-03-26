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

import com.linkedin.drelephant.DrElephant;
import com.sun.security.sasl.util.AbstractSaslImpl;

import play.Application;
import play.GlobalSettings;
import play.Logger;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.logging.Level;

import play.libs.F.Promise;
import play.mvc.Action;
import play.mvc.Http;
import play.mvc.SimpleResult;

import play.mvc.Result;


/**
 * This class manages all the global settings
 */
public class Global extends GlobalSettings {

  DrElephant _drElephant;

  public void onStart(Application app) {
    Logger.info("Starting Application...");

    fixJavaKerberos();

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

  static void setFinalStatic(Field field, Object newValue) throws Exception {
    field.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    field.set(null, newValue);
  }
  
  
  
  /* CORS filter */

    private class ActionWrapper extends Action.Simple {
        public ActionWrapper(Action<?> action) {
            this.delegate = action;
        }

        @Override
        public Promise<SimpleResult> call(Http.Context ctx) throws java.lang.Throwable {
            Promise<SimpleResult> result = this.delegate.call(ctx);
            Http.Response response = ctx.response();
            response.setHeader("Access-Control-Allow-Origin", "*"); //http://tu-gjt-q01:5777
            response.setHeader("Access-Control-Allow-Headers", "X-Requested-With, accept, content-type, Origin, X-Json");
            response.setHeader("Access-Control-Allow-Methods", "POST, GET, PUT, DELETE, OPTIONS");
            response.setHeader("Access-Control-Request-Headers", "X-Requested-With, accept, content-type, Origin, X-Json");

            return result;
        }
    }

    @Override
    public Action<?> onRequest(Http.Request request, java.lang.reflect.Method actionMethod) {
        Logger.info("before each request..." + request.toString());
        return new ActionWrapper(super.onRequest(request, actionMethod));
    }

  /* CORS filter */
  
  
  
  
  
  
  
  
  
  
  
  
}
