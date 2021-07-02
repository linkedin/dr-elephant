package com.linkedin.drelephant.util;

import com.linkedin.drelephant.math.Statistics;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ThreadContextMR2 {
    private static final Logger logger = Logger.getLogger(com.linkedin.drelephant.util.ThreadContextMR2.class);

    private static final AtomicInteger THREAD_ID = new AtomicInteger(1);

    private static final ThreadLocal<Integer> _LOCAL_THREAD_ID = new ThreadLocal<Integer>() {
        @Override
        public Integer initialValue() {
            return THREAD_ID.getAndIncrement();
        }
    };

    private static final ThreadLocal<Long> _LOCAL_LAST_UPDATED = new ThreadLocal<Long>();
    private static final ThreadLocal<Long> _LOCAL_UPDATE_INTERVAL = new ThreadLocal<Long>();

    private static final ThreadLocal<Pattern> _LOCAL_DIAGNOSTIC_PATTERN = new ThreadLocal<Pattern>() {
        @Override
        public Pattern initialValue() {
            // Example: "Task task_1443068695259_9143_m_000475 failed 1 times"
            return Pattern.compile(
                    ".*[\\s\\u00A0]+(task_[0-9]+_[0-9]+_[m|r]_[0-9]+)[\\s\\u00A0]+.*");
        }
    };

    private static final ThreadLocal<AuthenticatedURL.Token> _LOCAL_AUTH_TOKEN =
            new ThreadLocal<AuthenticatedURL.Token>() {
                @Override
                public AuthenticatedURL.Token initialValue() {
                    _LOCAL_LAST_UPDATED.set(System.currentTimeMillis());
                    // Random an interval for each executor to avoid update token at the same time
                    _LOCAL_UPDATE_INTERVAL.set(Statistics.MINUTE_IN_MS * 30 + new Random().nextLong()
                            % (3 * Statistics.MINUTE_IN_MS));
                    logger.info("Executor " + _LOCAL_THREAD_ID.get() + " update interval " + _LOCAL_UPDATE_INTERVAL.get() * 1.0
                            / Statistics.MINUTE_IN_MS);
                    return new AuthenticatedURL.Token();
                }
            };

    private static final ThreadLocal<AuthenticatedURL> _LOCAL_AUTH_URL = new ThreadLocal<AuthenticatedURL>() {
        @Override
        public AuthenticatedURL initialValue() {
            return new AuthenticatedURL();
        }
    };

    private static final ThreadLocal<ObjectMapper> _LOCAL_MAPPER = new ThreadLocal<ObjectMapper>() {
        @Override
        public ObjectMapper initialValue() {
            return new ObjectMapper();
        }
    };

    private ThreadContextMR2() {
        // Empty on purpose
    }

    public static Matcher getDiagnosticMatcher(String diagnosticInfo) {
        return _LOCAL_DIAGNOSTIC_PATTERN.get().matcher(diagnosticInfo);
    }

    public static JsonNode readJsonNode(URL url) throws IOException, AuthenticationException {
        HttpURLConnection conn = _LOCAL_AUTH_URL.get().openConnection(url, _LOCAL_AUTH_TOKEN.get());
        return _LOCAL_MAPPER.get().readTree(conn.getInputStream());
    }

    public static void updateAuthToken() {
        long curTime = System.currentTimeMillis();
        if (curTime - _LOCAL_LAST_UPDATED.get() > _LOCAL_UPDATE_INTERVAL.get()) {
            logger.info("Executor " + _LOCAL_THREAD_ID.get() + " updates its AuthenticatedToken.");
            _LOCAL_AUTH_TOKEN.set(new AuthenticatedURL.Token());
            _LOCAL_AUTH_URL.set(new AuthenticatedURL());
            _LOCAL_LAST_UPDATED.set(curTime);
        }
    }
}