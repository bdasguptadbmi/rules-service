/**
 * OrganizationResourceProvider
 *
 * @author Balendu Dasgupta
 * @version 2.0
 * <p>
 * This file implements the Organization FHIR resource type. Given a organization id, it
 * returns all the associated information for the organization.  I am using organization to
 * get details on
 * - Manufacturer
 * <p>
 * When running it on your local machine, you can use the below URL as an
 * example
 * <p>
 * http://localhost:8080/imsrvcs/services/Organization/PMC - This gets a
 * specific location record
 */
package org.nyp.ezvac.imsrvcs.providers;

import ca.uhn.fhir.model.dstu2.composite.IdentifierDt;
import ca.uhn.fhir.model.dstu2.resource.OperationOutcome;
import ca.uhn.fhir.model.dstu2.resource.Organization;
import ca.uhn.fhir.model.dstu2.valueset.IssueSeverityEnum;
import ca.uhn.fhir.model.primitive.IdDt;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import com.ibm.db2.jcc.b.l;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

public class OrganizationResourceProvider implements IResourceProvider {
    private DataSource ds = null;
    private String MANUFACTURER_ID_LOOKUP = "SELECT * FROM improd.manufacturer_table WHERE manufacturer_id = ?";

    /**
     * Constructor
     */
    public OrganizationResourceProvider() {
    }

    /**
     * The getResourceType method comes from IResourceProvider, and must be
     * overridden to indicate what type of resource this provider supplies.
     *
     * @return
     */
    @Override
    public Class<Organization> getResourceType() {
        return Organization.class;
    }

    /**
     * This function returns the organization record for a given organization id
     * @param theId - the organization id.
     * @return
     */
    @Read(version = true)
    public Organization getResourceById(@IdParam IdDt theId) throws Exception {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Organization o = new Organization();

        System.out.println("I have been called");

        try {
            Context ctx = new InitialContext();
            ds = (DataSource) ctx.lookup("java:comp/env/jdbc/nypis");
            conn = ds.getConnection();
            pstmt = conn.prepareStatement(MANUFACTURER_ID_LOOKUP);
            pstmt.setString(1, theId.getIdPart());
            rs = pstmt.executeQuery();
            while (rs.next()) {
                IdentifierDt manufacturerId = new IdentifierDt();
                manufacturerId.setSystem("ezvac_manufacturer_id");
                manufacturerId.setValue(rs.getString("manufacturer_id"));
                o.addIdentifier(manufacturerId);

                IdentifierDt cirId = new IdentifierDt();
                cirId.setSystem("cir_manufacturer_id");
                cirId.setValue(rs.getString("cir_manufacturer_code"));
                o.addIdentifier(cirId);

                o.setName(rs.getString("manufacturer_name"));

                o.setActive(true);
            }

            rs.close();
            pstmt.close();
            conn.close();
        } catch (Exception e) {
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("Error in Organization lookup: " + e.getMessage());
            throw new InternalErrorException("Error in Organization lookup: " + e.getMessage(), oo);
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

        return o;
    }

}
