/**
 * PatientResourceProvider
 *
 * @author Balendu Dasgupta
 * @version 2.0
 * <p>
 * This file implements the Patient FHIR resource type. Given a EMPI, it returns
 * all the associated id's for the patient along with the location to which this
 * patient is attached. When running it on your local machine, you can use the
 * below URL as an example
 * <p>
 * Search by UPID - returns only 1 record
 * http://localhost:8080/imsrvcs/services/Patient/U000010923
 * <p>
 * Search by EMPI - returns one or more records
 * http://localhost:8080/imsrvcs/services/Patient?identifier=1000000383&idtype=empi
 * <p>
 * Search by MRN - returns one or mor records
 * http://localhost:8080/imsrvcs/services/Patient?identifier=3131313&idtype=mrn
 * <p>
 * Search by MRN and orgsite
 * http://localhost:8080/imsrvcs/services/Patient?identifier=3131313&idtype=mrn&org=CPMC
 * <p>
 * To get data back in JSON format
 * curl -H "Accept: application/json+fhir" "url"
 * <p>
 * Revision History
 */
package org.nyp.ezvac.imsrvcs.providers;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.dstu2.composite.AddressDt;
import ca.uhn.fhir.model.dstu2.composite.ContactPointDt;
import ca.uhn.fhir.model.dstu2.composite.HumanNameDt;
import ca.uhn.fhir.model.dstu2.composite.IdentifierDt;
import ca.uhn.fhir.model.dstu2.composite.ResourceReferenceDt;
import ca.uhn.fhir.model.dstu2.resource.OperationOutcome;
import ca.uhn.fhir.model.dstu2.resource.Patient;
import ca.uhn.fhir.model.dstu2.valueset.AddressUseEnum;
import ca.uhn.fhir.model.dstu2.valueset.AdministrativeGenderEnum;
import ca.uhn.fhir.model.dstu2.valueset.ContactPointSystemEnum;
import ca.uhn.fhir.model.dstu2.valueset.IdentifierUseEnum;
import ca.uhn.fhir.model.dstu2.valueset.IssueSeverityEnum;
import ca.uhn.fhir.model.dstu2.valueset.NameUseEnum;
import ca.uhn.fhir.model.primitive.DateDt;
import ca.uhn.fhir.model.primitive.IdDt;
import ca.uhn.fhir.model.primitive.StringDt;
import ca.uhn.fhir.model.primitive.UriDt;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.annotation.RequiredParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.param.StringParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

public class PatientResourceProvider implements IResourceProvider {

    private DataSource ds = null;
    private static final String FIELD_NAMES = "a.localpatient_id, a.empi, a.upid, a.orgsite_id, a.cir_num, a.source, a.cir_sync_time, "
            + "b.firstname, b.midname, b.lastname, b.aka_firstname, b.aka_lastname, b.dob, b.ssn, b.sex, b.primary_lang, b.secondary_lang, "
            + "b.address1, b.address2, b.city, b.state, b.zip, b.primary_tel, b.secondary_tel,  "
            + "b.mother_firstname, b.mother_midname, b.mother_lastname, b.mother_maidenname, b.mother_dob, b.mother_ssn, "
            + "b.father_firstname, b.father_midname, b.father_lastname, b.father_dob, b.father_ssn, "
            + "b.birth_name, b.race, b.religion, COALESCE(b.vfc_status,'V') vfc_status, email, privacy_indicator, privacy_indicator_dtm";
    private static final String TABLE_NAMES = "improd.mpi_table a, improd.patient_table b ";
    private static final String UPID_LOOKUP = "SELECT " + FIELD_NAMES + " FROM " + TABLE_NAMES
            + " WHERE a.upid = ? AND a.upid = b.upid AND nvl(b.patient_status, 'A')<>'N' ";
    private static final String EMPI_LOOKUP = "SELECT " + FIELD_NAMES + " FROM " + TABLE_NAMES
            + " WHERE a.empi = ? AND a.upid = b.upid AND nvl(b.patient_status, 'A')<>'N' ";
    private static final String MRN_LOOKUP = "SELECT " + FIELD_NAMES + " FROM " + TABLE_NAMES
            + " WHERE a.localpatient_id = ? AND a.upid = b.upid AND nvl(b.patient_status, 'A')<>'N' ";
    private static final String MRN_ORG_LOOKUP = "SELECT " + FIELD_NAMES + " FROM " + TABLE_NAMES
            + " WHERE a.localpatient_id = ? AND a.orgsite_id = ? AND a.upid = b.upid AND nvl(b.patient_status, 'A')<>'N' ";

    /**
     * Constructor
     */
    public PatientResourceProvider() {
    }

    /**
     * The getResourceType method comes from IResourceProvider, and must be overridden to indicate what type of resource this provider supplies.
     * @return
     */
    @Override
    public Class<Patient> getResourceType() {
        return Patient.class;
    }

    /**
     * Given a UPID return the corresponding patient record from the database.
     *
     * @param theId - The UPID
     * @return
     */
    @Read()
    public Patient getResourceById(@IdParam IdDt theId) {
        String upid = theId.getIdPart();

        try {
            if (upid != null) {
                List<Patient> patients = getPatientRecordsFromDatabase(upid, "UPID", null);
                if (patients.size() == 0) {
                    throw new ResourceNotFoundException(theId);
                } else {
                    return patients.get(0);
                }
            } else {
                OperationOutcome oo = new OperationOutcome();
                oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("No UPID specified");
                throw new InternalErrorException("No UPID specified", oo);
            }
        } catch (Exception e) {
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails(e.getMessage());
            throw new InternalErrorException(e.getMessage(), oo);
        }
    }

    /**
     * Given either a MRN or EMPI return the list of records that match the site criteria
     * @param id
     * @param idType
     * @return
     */
    @Search()
    public List<Patient> findPatientsById(@RequiredParam(name = Patient.SP_IDENTIFIER) StringParam id, @RequiredParam(name = "idtype") StringParam idType) {
        try {
            if (id != null) {
                List<Patient> patients = getPatientRecordsFromDatabase(id.getValue().toString(), idType.getValue().toString(), null);
                if (patients.size() == 0) {
                    throw new ResourceNotFoundException(id.getValue().toString());
                } else {
                    return patients;
                }
            } else {
                OperationOutcome oo = new OperationOutcome();
                oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("No id specified");
                throw new InternalErrorException("No id specified", oo);
            }
        } catch (Exception e) {
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails(e.getMessage());
            throw new InternalErrorException(e.getMessage(), oo);
        }
    }

    /**
     * Given a MRN and an organization site return the correct record.
     * @param id
     * @param idType - should be mrn
     * @param org - CPMC, NYPH
     * @return
     */
    @Search()
    public List<Patient> findPatientsById(@RequiredParam(name = Patient.SP_IDENTIFIER) StringParam id, @RequiredParam(name = "idtype") StringParam idType, @RequiredParam(name = "org") StringParam org) {
        try {
            if (id != null) {
                List<Patient> patients = getPatientRecordsFromDatabase(id.getValue().toString(), idType.getValue().toString(), org.getValue().toString());
                if (patients.size() == 0) {
                    throw new ResourceNotFoundException(id.getValue().toString());
                } else {
                    return patients;
                }
            } else {
                OperationOutcome oo = new OperationOutcome();
                oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("No id specified");
                throw new InternalErrorException("No id specified", oo);
            }
        } catch (Exception e) {
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails(e.getMessage());
            throw new InternalErrorException(e.getMessage(), oo);
        }
    }

    /**
     * Single function to encapsulate the data retrieval based on the id type
     * such as upid, empi, mrn, etc.
     *
     * @param id
     * @param idType
     * @return
     */
    private List<Patient> getPatientRecordsFromDatabase(String id, String idType, String orgSite) throws Exception {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<Patient> patients = new ArrayList();
        String query = null;
        boolean injectOrg = false;

        if ((id == null) || (idType == null)) {
            throw new Exception("id or idtype cannot be null");
        }

        /* Figure out which query to use */
        if (idType.equalsIgnoreCase("UPID")) {
            query = UPID_LOOKUP;
        } else if (idType.equalsIgnoreCase("EMPI")) {
            query = EMPI_LOOKUP;
        } else if (idType.equalsIgnoreCase("MRN")) {
            query = (orgSite == null) ? MRN_LOOKUP : MRN_ORG_LOOKUP;
            if (orgSite != null)
                injectOrg = true;
        } else {
            throw new Exception("Unknown lookup type - valid ones are upid, empi");
        }

        /* Get the data */
        try {
            Context ctx = new InitialContext();
            ds = (DataSource) ctx.lookup("java:comp/env/jdbc/nypis");
            conn = ds.getConnection();
            pstmt = conn.prepareStatement(query);
            pstmt.setString(1, id);
            if (injectOrg)
                pstmt.setString(2, orgSite);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                Patient patient = new Patient();
                // The Id's first
                ArrayList<IdentifierDt> idList = new ArrayList<IdentifierDt>();
                idList.add(new IdentifierDt("mrn", rs.getString("localpatient_id")));
                idList.add(new IdentifierDt("empi", rs.getString("empi")));
                idList.add(new IdentifierDt("upid", rs.getString("upid")));
                idList.add(new IdentifierDt("cir", rs.getString("cir_num")));
                patient.setIdentifier(idList);
                ResourceReferenceDt org = new ResourceReferenceDt();
                org.setDisplay(rs.getString("orgsite_id"));
                patient.setManagingOrganization(org);
                patient.setId(rs.getString("upid"));

                // Name, dob, sex, address
                HumanNameDt name = patient.addName();
                name.setUse(NameUseEnum.OFFICIAL);
                name.addFamily(rs.getString("lastname"));
                name.addGiven(rs.getString("firstname"));
                if (rs.getString("sex").equalsIgnoreCase("M")) {
                    patient.setGender(AdministrativeGenderEnum.MALE);
                } else {
                    patient.setGender(AdministrativeGenderEnum.FEMALE);
                }
                patient.setBirthDate(new DateDt(rs.getDate("dob")));
                AddressDt address = patient.addAddress();
                address.setUse(AddressUseEnum.HOME);
                ArrayList<StringDt> addressLines = new ArrayList();
                addressLines.add(new StringDt(rs.getString("address1")));
                addressLines.add(new StringDt(rs.getString("address2")));
                address.setLine(addressLines);
                address.setCity(rs.getString("city"));
                address.setState(rs.getString("state"));
                address.setPostalCode(rs.getString("zip"));
                ContactPointDt primaryTel = new ContactPointDt();
                primaryTel.setSystem(ContactPointSystemEnum.PHONE);
                primaryTel.setValue(rs.getString("primary_tel"));
                patient.addTelecom(primaryTel);
                ContactPointDt secondaryTel = new ContactPointDt();
                secondaryTel.setSystem(ContactPointSystemEnum.PHONE);
                secondaryTel.setValue(rs.getString("secondary_tel"));
                patient.addTelecom(secondaryTel);

                patient.setActive(true);
                patients.add(patient);
            }
            rs.close();
            pstmt.close();
            conn.close();
        } catch (Exception e) {
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("Error in UPID lookup: " + e.getMessage());
            throw new InternalErrorException("Error in UPID lookup: " + e.getMessage(), oo);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                rs = null;
                if (pstmt != null) {
                    pstmt.close();
                }
                pstmt = null;
                if (conn != null) {
                    conn.close();
                }
                conn = null;
            } catch (Exception e) {
            }
        }

        return patients;
    }
}
