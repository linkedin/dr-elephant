package controllers;

public class HtmlUtil {
    /**
     * Make the class constructor private for this utility class.
     */
    private HtmlUtil() { }

    /**
     * Extract the HTML tags from the given input to make the text content more readable.
     *
     * @param html The HTML whose content we want to make more readable
     * @return The text content extracted from the HTML
     */
    public static String toText(String html) {
        String newline = System.getProperty("line.separator");
        return html == null ? "" : html
                .replaceAll("<br/>", newline)
                .replaceAll("<ul>", newline + newline)
                .replaceAll("</ul>", newline)
                .replaceAll("<li>", "* ")
                .replaceAll("</li>", newline);
    }
}
