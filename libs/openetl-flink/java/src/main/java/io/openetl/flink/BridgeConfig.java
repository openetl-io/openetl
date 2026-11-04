package io.openetl.flink;

import java.io.Serializable;
import java.util.Locale;

/** Where the runner's bridge is, the SHA-256 fingerprint of its certificate, and the attempt's token. */
final class BridgeConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    final String host;
    final int port;
    final String fingerprint;
    final String token;

    BridgeConfig(String address, String fingerprint, String token) {
        int colon = address.lastIndexOf(':');
        if (colon <= 0 || colon == address.length() - 1) {
            throw new IllegalArgumentException("the bridge's address " + address + " isn't host:port");
        }
        String h = address.substring(0, colon);
        if (h.startsWith("[") && h.endsWith("]")) {
            h = h.substring(1, h.length() - 1);
        }
        this.host = h;
        try {
            this.port = Integer.parseInt(address.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("the bridge's address " + address + " isn't host:port", e);
        }
        this.fingerprint = fingerprint.toLowerCase(Locale.ROOT).replace(":", "");
        if (!this.fingerprint.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("the bridge's fingerprint isn't a SHA-256 in hex");
        }
        if (token.isEmpty()) {
            throw new IllegalArgumentException("the bridge's token is empty");
        }
        this.token = token;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
