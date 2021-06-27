/**
 * PractionerResourceProvider
 *
 * @author Balendu Dasgupta
 * @version 2.0
 * <p>
 * This file implements the Practioner FHIR resource type. Given a provider id, it
 * returns all the associated information for the provider.
 * <p>
 * When running it on your local machine, you can use the below URL as an
 * example
 * <p>
 * http://localhost:8080/imsrvcs/services/Practitioner/72218 - This gets a
 * specific provider record
 * <p>
 * We might want to add something to get practioners from a particular facility
 */
package org.nyp.ezvac.imsrvcs.providers;

import ca.uhn.fhir.model.dstu2.composite.HumanNameDt;
import ca.uhn.fhir.model.dstu2.composite.IdentifierDt;
import ca.uhn.fhir.model.dstu2.resource.OperationOutcome;
import ca.uhn.fhir.model.dstu2.resource.Practitioner;
import ca.uhn.fhir.model.dstu2.valueset.IssueSeverityEnum;
import ca.uhn.fhir.model.primitive.IdDt;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

/**
 *
 * @author bdasgupt
 */
public class PractionerResourceProvider implements IResourceProvider {

    private DataSource ds = null;
    private String PROVIDER_ID_LOOKUP = "SELECT * FROM improd.provider_table WHERE provider_id = ?";

    /**
     * Constructor
     */
    public PractionerResourceProvider() {
    }

    /**
     * The getResourceType method comes from IResourceProvider, and must be
     * overridden to indicate what type of resource this provider supplies.
     *
     * @return
     */
    @Override
    public Class<Practitioner> getResourceType() {
        return Practitioner.class;
    }

    /**
     * This function returns the provider record for a given provider id
     * @param theId - the provider id.
     * @return
     */
    @Read(version = true)
    public Practitioner getResourceById(@IdParam IdDt theId) throws Exception {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Practitioner p = new Practitioner();

        System.out.println("I have been called");

        try {
            Context ctx = new InitialContext();
            ds = (DataSource) ctx.lookup("java:comp/env/jdbc/nypis");
            conn = ds.getConnection();
            System.out.println(PROVIDER_ID_LOOKUP);
            System.out.println(theId.getIdPart());
            pstmt = conn.prepareStatement(PROVIDER_ID_LOOKUP);
            pstmt.setString(1, theId.getIdPart());
            rs = pstmt.executeQuery();
            while (rs.next()) {
                IdentifierDt providerId = new IdentifierDt();
                providerId.setSystem("ezvac_provider_id");
                providerId.setValue(rs.getString("provider_id"));
                p.addIdentifier(providerId);

                IdentifierDt providerUid = new IdentifierDt();
                providerUid.setSystem("cumc_id");
                if (rs.getString("provider_uid") != null)
                    providerUid.setValue(rs.getString("provider_uid").toLowerCase());
                p.addIdentifier(providerUid);

                p.setId(rs.getString("provider_id"));

                if ((rs.getString("provider_active") == null) || (rs.getString("provider_active").equalsIgnoreCase("y")))
                    p.setActive(true);
                else
                    p.setActive(false);
                HumanNameDt name = new HumanNameDt();
                name.addFamily(rs.getString("provider_lastname"));
                name.addGiven(rs.getString("provider_firstname"));

                p.setName(name);

            }

            rs.close();
            pstmt.close();
            conn.close();
        } catch (Exception e) {
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("Error in Provider lookup: " + e.getMessage());
            throw new InternalErrorException("Error in Provider lookup: " + e.getMessage(), oo);
        } finally {
            if (rs != null) {
                rs.close();
                rs = null;
            }
            if (pstmt != null) {
                pstmt.close();
                pstmt = null;
            }
            if (conn != null) {
                conn.close();
                conn = null;
            }
        }

        return p;
    }

}
