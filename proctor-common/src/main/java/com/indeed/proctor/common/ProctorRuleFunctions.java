package com.indeed.proctor.common;

import com.indeed.util.core.ReleaseVersion;
import org.apache.log4j.Logger;

import java.util.Collection;

/**
 * Library of functions to make available to EL rules
 * @author ketan
 *
 */
@SuppressWarnings("UnusedDeclaration")
public class ProctorRuleFunctions {
    private static final Logger LOGGER = Logger.getLogger(RemoteProctorLoader.class);

    public static boolean contains(final Collection c, final Object element) {
        return c.contains(element);
    }

    public static boolean matches(final String value, final String regex) {
        return value.matches(regex);
    }

    public static long now() {
        return System.currentTimeMillis();
    }

    public static ReleaseVersion version(final String versionString) {
        return ReleaseVersion.fromString(versionString);
    }

    public static <T extends Comparable<T>> boolean inRange(final T value, final T closedLowerBound, final T openUpperBound) {
        return value.compareTo(closedLowerBound) >= 0 && openUpperBound.compareTo(value) > 0;
    }

    public static boolean versionInRange(final ReleaseVersion version, final String startInclusive, final String endExclusive) {
        final ReleaseVersion start = ReleaseVersion.fromString(startInclusive);
        final ReleaseVersion end = ReleaseVersion.fromString(endExclusive);
        if (end.getMatchPrecision() != ReleaseVersion.MatchPrecision.BUILD) {
            throw new IllegalStateException("Cannot use wildcard as open upper bound of range: " + endExclusive);
        }
        return inRange(version, start, end);
    }

    public static boolean endsWith(String input, String substring) {
        if (input == null) input = "";
        if (substring == null) substring = "";
        return input.endsWith(substring);
    }

    public static String hdidToDigit(String hdid) {
        String result = "";
        if (hdid == null || hdid.isEmpty()) {
            return result;
        }
        int digitLength = 8;
        String formatHdid = hdid.replace("-", "").toLowerCase();
        if (formatHdid.length() > digitLength) {
            formatHdid = formatHdid.substring(formatHdid.length() - digitLength);
        }
        try {
            result = String.format("%02d", Long.valueOf(formatHdid, 16));
        } catch (NumberFormatException e) {
            LOGGER.error(String.format("hdidToDigit failed with hdid %s: ", hdid) + e.toString());
        }
        return result;
    }
}
