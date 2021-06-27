/**
 * Services
 *
 * @author Balendu Dasgupta
 * @version 2.0
 * <p>
 * This file registers all the FHIR Resource types that are supported by this package.  At present, the following
 * resource types are supported
 * 1. Patient
 * 2. Immunization
 * 3. ImmunizationRecommendation
 * <p>
 * Revision History
 */

package org.nyp.ezvac.imsrvcs.services;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.interceptor.LoggingInterceptor;
import ca.uhn.fhir.rest.server.interceptor.ResponseHighlighterInterceptor;

import java.util.ArrayList;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;

import org.nyp.ezvac.imsrvcs.providers.ImmunizationRecommendationResourceProvider;
import org.nyp.ezvac.imsrvcs.providers.ImmunizationResourceProvider;
import org.nyp.ezvac.imsrvcs.providers.LocationResourceProvider;
import org.nyp.ezvac.imsrvcs.providers.OrganizationResourceProvider;
import org.nyp.ezvac.imsrvcs.providers.PatientResourceProvider;
import org.nyp.ezvac.imsrvcs.providers.PractionerResourceProvider;

/**
 * @author bdasgupt
 */
@WebServlet(name = "Services", urlPatterns = {"/services/*"}, displayName = "Immunization Services")
public class Services extends RestfulServer {

    private static final long serialVersionUID = 1L;

    public Services() {
        super(FhirContext.forDstu2()); // Support DSTU2
    }

    @Override
    public void initialize() throws ServletException {
        /* Register some interceptors */
        LoggingInterceptor loggingInterceptor = new LoggingInterceptor();
        registerInterceptor(loggingInterceptor);
        loggingInterceptor.setLoggerName("test.accesslog");
        loggingInterceptor.setMessageFormat("Source[${remoteAddr}] Operation[${operationType} ${idOrResourceName}] UA[${requestHeader.user-agent}] Params[${requestParameters}]");


        /*
         * The servlet defines any number of resource providers, and
         * configures itself to use them by calling
         * setResourceProviders()
         */
        List<IResourceProvider> resourceProviders = new ArrayList<IResourceProvider>();
        resourceProviders.add(new PatientResourceProvider());
        resourceProviders.add(new ImmunizationResourceProvider());
        resourceProviders.add(new ImmunizationRecommendationResourceProvider());
        resourceProviders.add(new PractionerResourceProvider());
        resourceProviders.add(new LocationResourceProvider());
        resourceProviders.add(new OrganizationResourceProvider());
        setResourceProviders(resourceProviders);

        registerInterceptor(new ResponseHighlighterInterceptor());
        setDefaultPrettyPrint(true);
    }
}
