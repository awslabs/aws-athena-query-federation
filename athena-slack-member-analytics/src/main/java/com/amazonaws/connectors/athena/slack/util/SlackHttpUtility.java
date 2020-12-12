/*-
 * #%L
 * athena-slack-member-analytics
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.connectors.athena.slack.util;

import org.apache.http.*;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONObject;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
import java.io.Reader;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;
import java.lang.RuntimeException;


public class SlackHttpUtility {

    private static final Logger logger = LoggerFactory.getLogger(SlackHttpUtility.class);
    private static CloseableHttpClient client;

    /**
     * Makes an HTTP request using GET method to the specified URL.
     *
     * @param requestURL
     *            the URL of the remote server
     * @param headers HashMap<String,String> of request Headers
     * @return An CloseableHttpResponse object
     * @throws IOException
     *             thrown if any I/O error occurred
     */
    public static CloseableHttpResponse doGetRequest(URIBuilder requestURI, HashMap<String, String> headers)
            throws Exception {

        logger.info("doGetRequest: enter - {}", requestURI.toString());
        HttpGet httpGet = new HttpGet(requestURI.build());
        for (String i : headers.keySet()) {
            httpGet.setHeader(i,headers.get(i));
        }

        client = HttpClients.createDefault();
        CloseableHttpResponse response = client.execute(httpGet);
        isRequestOk(response);

        logger.info("doGetRequest: exit");
        return response;

    }

    /**
     * Makes an HTTP request using POST method to the specified URL.
     *
     * @param requestURL the URL of the remote server
     * @param headers HashMap<String,String> of request Headers
     * @param params HashMap<String, String> of post parameters
     * @return An CloseableHttpResponse object
     * @throws IOException
     *             thrown if any I/O error occurred
     */
    public static CloseableHttpResponse doPostRequest(String requestURL,
                                                      HashMap<String, String> params,
                                                      HashMap<String, String> headers)
            throws Exception {

        logger.info("doPostRequest: enter - {}", requestURL);
        HttpPost httpPost = new HttpPost(requestURL);

        if (params != null && params.size() > 0) {
            List<NameValuePair> nameValuePairs = new ArrayList<>(params.size());
            for (String i : params.keySet()){
                logger.debug("doPostRequest - params {}:{}", i, params.get(i));
                nameValuePairs.add(new BasicNameValuePair(i,params.get(i)));
            }
            httpPost.setEntity(new UrlEncodedFormEntity(nameValuePairs));
        }

        if(headers != null && headers.size()>0){
            for (String i : headers.keySet()) {
                logger.debug("doPostRequest - headers {}:{}", i, headers.get(i));
                httpPost.setHeader(i,headers.get(i));
            }
        }
        client = HttpClients.createDefault();
        CloseableHttpResponse response = client.execute(httpPost);

        isRequestOk(response);

        logger.info("doPostRequest: exit");
        return response;

    }


    /**
     * Checks and log status of HTTP request.
     *
     * @param response
     *            the CloseableHttpResponse object.
     * @return True if request status is 200.
     *
     */
    private static boolean isRequestOk(CloseableHttpResponse response) 
        throws Exception{
        logger.info("isRequestOk: enter");
        if (response == null) {
            logger.warn("isRequestOK: Null response.");
            return false;
        }
        int responseStatus = response.getStatusLine().getStatusCode();
        logger.info("isRequestOK: Status " + response.getStatusLine().toString());
        if (responseStatus!=200){
            String e = response.getStatusLine().getReasonPhrase();
            response.close();
            disconnect();
            throw new RuntimeException("isRequestOK: Error - " + e);
        }
        return true;
    }

    /**
     * Makes a HTTP Get request with gzip encoding headers. Expects a gzip resonse.
     * Decompresses gzip response and returns string.
     *
     * @param requestURI the URL of the remote server
     * @param headers Map<String, String> with additional headers
     * @return BufferedReader with source records.
     */
    public static BufferedReader getData(URIBuilder requestURI, HashMap<String, String> headers)
            throws Exception {
        logger.info("getData: enter");

        BufferedReader reader = null;
        headers.put(HttpHeaders.ACCEPT_ENCODING, "gzip");
        
        CloseableHttpResponse response = doGetRequest(requestURI, headers);
        
        HttpEntity entity = response.getEntity();
    
        ContentType contentType = ContentType.getOrDefault(entity);
        String mimeType = contentType.getMimeType();
        logger.info("getData: Content Type=" + mimeType);
        switch(mimeType){
            /**
             * If slack endpoint returns application/json, file might be empty or there is an error.
             * Logging error as WARNING without throwing an exception, just return empty records.
             */
            case "application/json":
                String data = EntityUtils.toString(entity);
                JSONObject jsonResponse = new JSONObject(data);
                if (jsonResponse.has("ok") && !jsonResponse.getBoolean("ok")){
                    logger.warn("getData: " + data);
                }else {
                    logger.info("getData: Processing uncompressed response....");
                    Reader inputString = new StringReader(data);
                    reader = new BufferedReader(inputString);
                }
                break;
            case "application/gzip":
                logger.info("getData: Processing compressed response...");
                GZIPInputStream gzIs = new GZIPInputStream(entity.getContent());
                reader = new BufferedReader(new InputStreamReader(gzIs));
                break;
            default:
                response.close();
                disconnect();
                throw new RuntimeException("Unsupported mime type returned by Slack Analytics endpoint.");
        }

        logger.info("getData: exit");

        return reader;
    }
    
    /**
     * Closes the client if opened
     */
    public static void disconnect() {
        logger.info("disconnect: enter");
        if (client != null) {
            try{
                client.close();
                logger.info("disconnect: connection closed");
            } catch (IOException ioe) {
                logger.warn("disconnect: Not able to close client. " + ioe.getMessage());
            }
        }
    }
    
}