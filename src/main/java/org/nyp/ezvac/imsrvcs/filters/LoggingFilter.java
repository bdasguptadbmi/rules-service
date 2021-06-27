/*
 * Send the userid with the header
 * curl --header "userid: dasgupt" localhost:8080/imsrces/ImmunizationRecommendation?empi=2000079083
 * curl --header "userid: dasgupt" --header "appid: al localhost:8080/imsrvcs/services/ImmunizationRecommendation?empi=2000079083
 *
 */
package org.nyp.ezvac.imsrvcs.filters;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author bdasgupt
 */
public class LoggingFilter implements Filter {

    private static Logger logger = LoggerFactory.getLogger(LoggingFilter.class);

    public void init(FilterConfig config) throws ServletException {
        // Require user id and app name
        System.out.println("*** Filter init");
    }

    public void doFilter(ServletRequest request,
                         ServletResponse response,
                         FilterChain chain)
            throws java.io.IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String ipAddress = request.getRemoteAddr();
        String uid = "";
        String appId = "";

        if (httpRequest.getMethod().equalsIgnoreCase("GET")) {
            HttpServletResponse httpResponse = (HttpServletResponse) response;

            Enumeration headerNames = httpRequest.getHeaderNames();
            while (headerNames.hasMoreElements()) {
                String headerName = (String) headerNames.nextElement();
                if (headerName.equalsIgnoreCase("userid")) {
                    uid = httpRequest.getHeader(headerName);
                }
                if (headerName.equalsIgnoreCase("appid")) {
                    appId = httpRequest.getHeader(headerName);
                }
            }
            String logString = "{ip:" + httpRequest.getRemoteAddr() + ", "
                    + "host:" + httpRequest.getRemoteHost() + ", "
                    + "uid:" + uid + ", "
                    + "appId:" + appId + ", "
                    + "uri: \"" + getFullURL(httpRequest) + "\"}";
            System.out.println(logString);
            logger.info(logString);
            System.out.println("Done");

            //request.removeAttribute("uid");
            //request.removeAttribute("appid");

        }

        chain.doFilter(parseRequest(httpRequest), response);
    }

    public void destroy() {
    }

    private HttpServletRequest parseRequest(HttpServletRequest request) throws ServletException {
        Map<String, String[]> parameterMap = new HashMap<String, String[]>();
        Enumeration<String> parameterNames = request.getParameterNames();

        while (parameterNames.hasMoreElements()) {
            String paramName = parameterNames.nextElement();
            String[] paramValues = request.getParameterValues(paramName);

            if (!paramName.equalsIgnoreCase("uid") && !paramName.equalsIgnoreCase("appid")) {
                parameterMap.put(paramName, new String[]{paramValues[0]});
            }
        }

        return wrapRequest(request, parameterMap);
    }

    private static HttpServletRequest wrapRequest(HttpServletRequest request, final Map<String, String[]> parameterMap) {
        return new HttpServletRequestWrapper(request) {
            public Map<String, String[]> getParameterMap() {
                return parameterMap;
            }

            public String[] getParameterValues(String name) {
                return parameterMap.get(name);
            }

            public String getParameter(String name) {
                String[] params = getParameterValues(name);
                return params != null && params.length > 0 ? params[0] : null;
            }

            public Enumeration<String> getParameterNames() {
                return Collections.enumeration(parameterMap.keySet());
            }
        };
    }

    private static String getFullURL(HttpServletRequest request) {
        StringBuffer requestURL = request.getRequestURL();
        String queryString = request.getQueryString();

        if (queryString == null) {
            return requestURL.toString();
        } else {
            return requestURL.append('?').append(queryString).toString();
        }
    }
}
