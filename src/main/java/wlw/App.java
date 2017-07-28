package wlw;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Hello world!
 *
 */
public class App
{

    public static void main(String[] args) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
        server.createContext("/test", new MyHandler());
        server.setExecutor(null);
        server.start();
    }
    static class MyHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange he) throws IOException {
            System.out.println("Serving the request");

            if (he.getRequestMethod().equalsIgnoreCase("POST")) {
                try {
                    Headers requestHeaders = he.getRequestHeaders();
                    Set<Map.Entry<String, List<String>>> entries = requestHeaders.entrySet();

                    int contentLength = Integer.parseInt(requestHeaders.getFirst("Content-length"));
                    System.out.println(""+requestHeaders.getFirst("Content-length"));

                    InputStream is = he.getRequestBody();

                    byte[] data = new byte[contentLength];
                    int length = is.read(data);

                    System.out.print("data:" + new String(data));
                    Headers responseHeaders = he.getResponseHeaders();

                    he.sendResponseHeaders(HttpURLConnection.HTTP_OK, contentLength);

                    OutputStream os = he.getResponseBody();

                    os.write(data);
                    he.close();

                } catch (NumberFormatException | IOException e) {
                }
            }

        }
    }
}

