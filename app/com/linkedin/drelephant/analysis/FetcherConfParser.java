package com.linkedin.drelephant.analysis;

import org.w3c.dom.*;
import javax.xml.parsers.*;
import java.io.*;

public class FetcherConfParser {
    public static String extractEventLogLocationUri() {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new File("app-conf/FetcherConf.xml"));

            // Get the root element
            Element root = document.getDocumentElement();

            // Get the fetcher elements
            NodeList fetchers = root.getElementsByTagName("fetcher");

            // Iterate over the fetcher elements
            for (int i = 0; i < fetchers.getLength(); i++) {
                Element fetcher = (Element) fetchers.item(i);

                // Get the applicationtype
                String applicationType = fetcher.getElementsByTagName("applicationtype")
                        .item(0)
                        .getTextContent();

                // Check if it is the 'spark' application type
                if (applicationType.equals("spark")) {
                    // Get the event_log_location_uri
                    String eventLogLocationUri = fetcher.getElementsByTagName("event_log_location_uri")
                            .item(0)
                            .getTextContent();

                    return eventLogLocationUri;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}
