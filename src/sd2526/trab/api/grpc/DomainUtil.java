package sd2526.trab.api.grpc;

public final class DomainUtil {
    private DomainUtil() {}

    public static String domainFromHost(String host) {
        if (host == null || host.isBlank())
            return null;
        int idx = host.indexOf('.');
        String domain = idx >= 0 ? host.substring(idx + 1) : host;
        return normalizeRuntimeDomainSuffix(domain);
    }

    public static String normalizeRuntimeDomainSuffix(String domain) {
        if (domain == null || domain.isBlank())
            return domain;

        int lastDash = domain.lastIndexOf('-');
        if (lastDash < 0 || lastDash == domain.length() - 1)
            return domain;

        String tail = domain.substring(lastDash + 1);
        if (!isNumeric(tail))
            return domain;

        String prefix = domain.substring(0, lastDash);
        int prevDash = prefix.lastIndexOf('-');
        if (prevDash < 0 || prevDash == prefix.length() - 1)
            return domain;

        String prevTail = prefix.substring(prevDash + 1);
        if (!isNumeric(prevTail))
            return domain;

        return prefix;
    }

    public static boolean isNumeric(String s) {
        if (s == null || s.isEmpty())
            return false;
        for (int i = 0; i < s.length(); i++)
            if (!Character.isDigit(s.charAt(i)))
                return false;
        return true;
    }
}
