package sd2526.trab.api.server.proxy;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Zoho Mail API client using ScribeJava OAuth2.
 * Stores/retrieves Message objects as emails with metadata encoded in subject/body.
 */
public class ZohoMailClient {
    private static final Logger Log = Logger.getLogger(ZohoMailClient.class.getName());
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String MAIL_API_BASE = "https://mail.zoho.eu/api";

    private final OAuth20Service service;
    private final String refreshToken;
    private OAuth2AccessToken cachedToken;
    private long tokenIssuedAt;
    private String accountId;
    private String mailboxAddress;

    public ZohoMailClient(String clientId, String clientSecret, String refreshToken) {
        this.refreshToken = refreshToken;
        // Explicitly load JDK cacerts for external HTTPS (Zoho) since the
        // javax.net.ssl.trustStore property points to our custom internal truststore
        try {
            String javaHome = System.getProperty("java.home");
            String cacertsPath = javaHome + "/lib/security/cacerts";
            KeyStore cacerts = KeyStore.getInstance(KeyStore.getDefaultType());
            try (FileInputStream fis = new FileInputStream(cacertsPath)) {
                cacerts.load(fis, "changeit".toCharArray());
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(cacerts);
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, tmf.getTrustManagers(), null);
            HttpsURLConnection.setDefaultSSLSocketFactory(ctx.getSocketFactory());
        } catch (Exception e) {
            Log.warning("Could not set default SSL factory for Zoho: " + e.getMessage());
        }
        this.service = new ServiceBuilder(clientId)
                .apiSecret(clientSecret)
                .defaultScope("ZohoMail.messages.ALL,ZohoMail.accounts.READ,ZohoMail.folders.READ")
                .build(ZohoApi20.instance());
    }

    private String getAccessToken() throws Exception {
        if (cachedToken == null || isExpiredSoon()) {
            cachedToken = service.refreshAccessToken(refreshToken);
            tokenIssuedAt = System.currentTimeMillis();
            Log.info("Zoho access token refreshed.");
        }
        return cachedToken.getAccessToken();
    }

    private boolean isExpiredSoon() {
        if (cachedToken == null || cachedToken.getExpiresIn() == null) return true;
        long elapsed = (System.currentTimeMillis() - tokenIssuedAt) / 1000;
        return elapsed >= cachedToken.getExpiresIn() - 120;
    }

    private void signRequest(OAuthRequest request) throws Exception {
        service.signRequest(new OAuth2AccessToken(getAccessToken()), request);
    }

    public void init() throws Exception {
        OAuthRequest request = new OAuthRequest(Verb.GET, MAIL_API_BASE + "/accounts");
        signRequest(request);

        try (Response response = service.execute(request)) {
            if (response.isSuccessful()) {
                JsonNode root = mapper.readTree(response.getBody());
                JsonNode data = root.get("data");
                if (data != null && data.isArray() && !data.isEmpty()) {
                    accountId = data.get(0).get("accountId").asText();
                    mailboxAddress = data.get(0).get("primaryEmailAddress").asText();
                    Log.info("Zoho account: " + accountId + " / " + mailboxAddress);
                }
            } else {
                Log.warning("Failed to get Zoho account: " + response.getCode() + " " + response.getBody());
            }
        }
    }

    /**
     * Sends an email to self to store a message. The subject encodes the message ID.
     * The body contains the serialized Message JSON.
     */
    public boolean storeMessage(String emailSubject, String emailBody) throws Exception {
        if (accountId == null) return false;

        String url = MAIL_API_BASE + "/accounts/" + accountId + "/messages";
        String jsonBody = mapper.writeValueAsString(new SendMailRequest(mailboxAddress, mailboxAddress, emailSubject, emailBody));

        OAuthRequest request = new OAuthRequest(Verb.POST, url);
        request.addHeader("Content-Type", "application/json");
        request.setPayload(jsonBody);
        signRequest(request);

        try (Response response = service.execute(request)) {
            if (response.isSuccessful()) return true;
            Log.warning("Failed to store email: " + response.getCode() + " " + response.getBody());
            return false;
        }
    }

    /**
     * Gets all stored message emails using search API (no folder scope needed).
     * Searches for emails with subject prefix "MSG:" which is our storage format.
     */
    public String getInboxEmails() throws Exception {
        if (accountId == null) return null;

        // Use search API - requires only messages scope, not folders scope
        String url = MAIL_API_BASE + "/accounts/" + accountId + "/messages/search?searchKey=subject:MSG:&limit=200";
        OAuthRequest request = new OAuthRequest(Verb.GET, url);
        signRequest(request);

        try (Response response = service.execute(request)) {
            if (response.isSuccessful()) return response.getBody();
            Log.warning("Failed to search inbox: " + response.getCode() + " " + response.getBody());
            return null;
        }
    }

    /**
     * Gets a specific email by messageId and folderId.
     */
    public String getEmail(String zohoMessageId, String folderId) throws Exception {
        if (accountId == null || folderId == null) return null;

        String url = MAIL_API_BASE + "/accounts/" + accountId + "/folders/" + folderId + "/messages/" + zohoMessageId + "/content";
        OAuthRequest request = new OAuthRequest(Verb.GET, url);
        signRequest(request);

        try (Response response = service.execute(request)) {
            if (response.isSuccessful()) return response.getBody();
            Log.warning("Failed to get email: " + response.getCode() + " " + response.getBody());
            return null;
        }
    }

    /**
     * Permanently deletes messages.
     */
    public void deleteEmails(List<String> zohoMessageIds) throws Exception {
        if (accountId == null || zohoMessageIds == null || zohoMessageIds.isEmpty()) return;

        // First move to trash
        String trashUrl = MAIL_API_BASE + "/accounts/" + accountId + "/messages/trash";
        String jsonBody = "{\"messageId\": [\"" + String.join("\",\"", zohoMessageIds) + "\"]}";

        OAuthRequest trashRequest = new OAuthRequest(Verb.PUT, trashUrl);
        trashRequest.addHeader("Content-Type", "application/json");
        trashRequest.setPayload(jsonBody);
        signRequest(trashRequest);

        try (Response response = service.execute(trashRequest)) {
            if (!response.isSuccessful()) {
                Log.warning("Failed to trash emails: " + response.getCode() + " " + response.getBody());
            }
        }

        // Then permanently delete
        String deleteUrl = MAIL_API_BASE + "/accounts/" + accountId + "/messages";
        OAuthRequest deleteRequest = new OAuthRequest(Verb.DELETE, deleteUrl);
        deleteRequest.addHeader("Content-Type", "application/json");
        deleteRequest.setPayload(jsonBody);
        signRequest(deleteRequest);

        try (Response response = service.execute(deleteRequest)) {
            if (response.isSuccessful()) return;
            Log.warning("Failed to permanently delete emails: " + response.getCode() + " " + response.getBody());
        }
    }

    /**
     * Empties the inbox by repeatedly fetching and deleting all messages.
     */
    public void clearInbox() throws Exception {
        for (int round = 0; round < 5; round++) {
            String inbox = getInboxEmails();
            if (inbox == null) return;

            JsonNode root = mapper.readTree(inbox);
            JsonNode data = root.get("data");
            if (data == null || !data.isArray() || data.isEmpty()) return;

            List<String> ids = new ArrayList<>();
            for (JsonNode email : data) {
                ids.add(email.get("messageId").asText());
            }
            if (ids.isEmpty()) return;
            deleteEmails(ids);
            Log.info("clearInbox: deleted " + ids.size() + " emails (round " + (round + 1) + ")");
        }
    }

    // JSON request body for sending mail
    private record SendMailRequest(String fromAddress, String toAddress, String subject, String content) {}
}
