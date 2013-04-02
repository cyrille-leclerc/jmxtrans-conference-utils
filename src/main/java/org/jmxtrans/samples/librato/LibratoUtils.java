package org.jmxtrans.samples.librato;

import com.fasterxml.jackson.core.Base64Variants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:cleclerc@xebia.fr">Cyrille Le Clerc</a>
 */
public class LibratoUtils {

    String username = "cyrille@cyrilleleclerc.com";
    String token = "aef0210ce3a01a931264c356e5743a470f1fcfc03d640939a79a6642f2ed393f";

    public List<String> listMetrics() throws IOException {
        String basicAuthentication = Base64Variants.getDefaultVariant().encode((username + ":" + token).getBytes(Charset.forName("US-ASCII")));
        HttpURLConnection urlConnection = (HttpURLConnection) new URL("https://metrics-api.librato.com/v1/metrics").openConnection();
        urlConnection.setRequestProperty("Authorization", "Basic " + basicAuthentication);
        urlConnection.setDoInput(true);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readValue(urlConnection.getInputStream(), JsonNode.class);
        List<String> result = new ArrayList<String>();
        for (JsonNode metric : rootNode.path("metrics")) {
            String name = metric.get("name").asText();
            result.add(name);
        }


        return result;
    }

    /**
     * The multi delete did not work.
     * @param metrics
     * @throws IOException
     */
    public void deleteMetrics(List<String> metrics) throws IOException {
        String basicAuthentication = Base64Variants.getDefaultVariant().encode((username + ":" + token).getBytes(Charset.forName("US-ASCII")));

        for (String metric : metrics) {

            URL url = new URL("https://metrics-api.librato.com/v1/metrics/" + metric);

            HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();

            urlConnection.setRequestMethod("DELETE");
            urlConnection.setRequestProperty("Authorization", "Basic " + basicAuthentication);
            int responseCode = urlConnection.getResponseCode();
            System.out.println("Metric '" + metric + "' deleted: " + responseCode);
            InputStream err = urlConnection.getErrorStream();
            if (err != null)
                ByteStreams.copy(err, System.out);
            ByteStreams.copy(urlConnection.getInputStream(), System.out);
        }
    }
}
