/**
 * LocatoinResourceProvider
 *
 * @author Balendu Dasgupta
 * @version 2.0
 * <p>
 * This file implements the Location FHIR resource type. Given a facility id, it
 * returns all the associated information for the location.
 * <p>
 * When running it on your local machine, you can use the below URL as an
 * example
 * <p>
 * http://localhost:8080/imsrvcs/services/Location/2 - This gets a
 * specific location record
 */
package org.nyp.ezvac.imsrvcs.providers;

import ca.uhn.fhir.model.dstu2.composite.AddressDt;
import ca.uhn.fhir.model.dstu2.composite.ContactPointDt;
import ca.uhn.fhir.model.dstu2.composite.IdentifierDt;
import ca.uhn.fhir.model.dstu2.resource.Location;
import ca.uhn.fhir.model.dstu2.resource.OperationOutcome;
import ca.uhn.fhir.model.dstu2.valueset.AddressUseEnum;
import ca.uhn.fhir.model.dstu2.valueset.ContactPointSystemEnum;
import ca.uhn.fhir.model.dstu2.valueset.IssueSeverityEnum;
import ca.uhn.fhir.model.dstu2.valueset.LocationModeEnum;
import ca.uhn.fhir.model.dstu2.valueset.LocationStatusEnum;
import ca.uhn.fhir.model.dstu2.valueset.LocationTypeEnum;
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
public class LocationResourceProvider implements IResourceProvider {
    private DataSource ds = null;
    private String LOCATION_ID_LOOKUP = "SELECT * FROM improd.facilitysite_table WHERE facilitysite_id = ?";

    /**
     * Constructor
     */
    public LocationResourceProvider() {
    }

    /**
     * The getResourceType method comes from IResourceProvider, and must be
     * overridden to indicate what type of resource this provider supplies.
     *
     * @return
     */
    @Override
    public Class<Location> getResourceType() {
        return Location.class;
    }

    /**
     * This function returns the provider record for a given provider id
     * @param theId - the provider id.
     * @return
     */
    @Read(version = true)
    public Location getResourceById(@IdParam IdDt theId) throws Exception {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Location l = new Location();

        System.out.println("I have been called");

        try {
            Context ctx = new InitialContext();
            ds = (DataSource) ctx.lookup("java:comp/env/jdbc/nypis");
            conn = ds.getConnection();
            pstmt = conn.prepareStatement(LOCATION_ID_LOOKUP);
            pstmt.setString(1, theId.getIdPart());
            rs = pstmt.executeQuery();
            while (rs.next()) {
                IdentifierDt ezvacId = new IdentifierDt();
                ezvacId.setSystem("ezvac_facility_id");
                ezvacId.setValue(rs.getString("facilitysite_id"));
                l.addIdentifier(ezvacId);

                IdentifierDt cirId = new IdentifierDt();
                cirId.setSystem("cir_facility_id");
                cirId.setValue(rs.getString("facility_doh_code"));
                l.addIdentifier(cirId);

                IdentifierDt orgId = new IdentifierDt();
                orgId.setSystem("org_site_id");
                orgId.setValue(rs.getString("orgsite_id"));
                l.addIdentifier(orgId);


                l.setStatus(LocationStatusEnum.ACTIVE);
                l.setName(rs.getString("facility_name"));

                AddressDt address = new AddressDt();
                address.setUse(AddressUseEnum.WORK);
                address.setCity(rs.getString("facility_city"));
                address.setState(rs.getString("facility_state"));
                address.addLine(rs.getString("facility_address1"));
                address.addLine(rs.getString("facility_address2"));
                address.setPostalCode(rs.getString("facility_zip"));
                l.setAddress(address);

                ContactPointDt phone = new ContactPointDt();
                phone.setSystem(ContactPointSystemEnum.PHONE);
                phone.setValue(rs.getString("facility_phone"));
                l.addTelecom(phone);

                ContactPointDt fax = new ContactPointDt();
                fax.setSystem(ContactPointSystemEnum.FAX);
                fax.setValue(rs.getString("facility_fax"));
                l.addTelecom(fax);

                l.setMode(LocationModeEnum.INSTANCE);
                l.setType(LocationTypeEnum.BUILDING);
            }

            rs.close();
            pstmt.close();
            conn.close();
        } catch (Exception e) {
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("Error in Location lookup: " + e.getMessage());
            throw new InternalErrorException("Error in Location lookup: " + e.getMessage(), oo);
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

        return l;
    }
}
