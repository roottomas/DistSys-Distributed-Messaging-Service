package sd2526.trab.api.server.proxy;

import com.github.scribejava.core.builder.api.DefaultApi20;
import com.github.scribejava.core.oauth2.bearersignature.BearerSignature;
import com.github.scribejava.core.oauth2.clientauthentication.ClientAuthentication;
import com.github.scribejava.core.oauth2.clientauthentication.RequestBodyAuthenticationScheme;
import com.github.scribejava.core.model.OAuthRequest;

/**
 * Zoho OAuth2 API definition for ScribeJava.
 */
public class ZohoApi20 extends DefaultApi20 {

    private ZohoApi20() {}

    private static class InstanceHolder {
        private static final ZohoApi20 INSTANCE = new ZohoApi20();
    }

    public static ZohoApi20 instance() {
        return InstanceHolder.INSTANCE;
    }

    @Override
    public String getAccessTokenEndpoint() {
        return "https://accounts.zoho.eu/oauth/v2/token";
    }

    @Override
    protected String getAuthorizationBaseUrl() {
        return "https://accounts.zoho.eu/oauth/v2/auth";
    }

    @Override
    public BearerSignature getBearerSignature() {
        return ZohoBearerSignature.INSTANCE;
    }

    @Override
    public ClientAuthentication getClientAuthentication() {
        return RequestBodyAuthenticationScheme.instance();
    }

    private static class ZohoBearerSignature implements BearerSignature {
        static final ZohoBearerSignature INSTANCE = new ZohoBearerSignature();

        @Override
        public void signRequest(String accessToken, OAuthRequest request) {
            request.addHeader("Authorization", "Zoho-oauthtoken " + accessToken);
        }
    }
}
