/**
 * ImmunizationResourceProvider
 *
 * @author Balendu Dasgupta
 * @version 2.0
 * <p>
 * This file implements the Immunization FHIR resource type. Given a EMPI, it
 * returns all the associated immunizations for the patient. The shots returned
 * are coded with the CVX code, as that is what is stated in the FHIR standard.
 * <p>
 * When running it on your local machine, you can use the below URL as an
 * example
 * <p>
 * http://localhost:8080/imsrvcs/services/Immunization/2374717 - This gets a
 * specific immunization record
 * <p>
 * http://localhost:8080/imsrvcs/services/Immunization?identifier=2070246 - This
 * gets all the records for a UPID
 * <p>
 * http://localhost:8080/imsrvcs/services/Immunization?empi=1006833172&uid=dasgupt&appid=allscripts&status=Y
 * - This gets all the records for a UPID and status = N
 * <p>
 * http://localhost:8080/imsrvcs/services/Immunization?empi=1010428628 - This
 * gets the records for a given EMPI
 * <p>
 * To get the data back in JSON format...
 * curl -H "Accept:application/json+fhir" http://localhost:8080/imsrvcs/services/Immunization?identifier=U000010923
 * curl -H "Accept:application/json" "http://localhost:8080/imsrvcs/services/Immunization?identifier=U000010923"
 * Revision History
 */
package org.nyp.ezvac.imsrvcs.providers;

import ca.uhn.fhir.model.dstu2.composite.CodeableConceptDt;
import ca.uhn.fhir.model.dstu2.composite.CodingDt;
import ca.uhn.fhir.model.dstu2.composite.ResourceReferenceDt;
import ca.uhn.fhir.model.dstu2.composite.SimpleQuantityDt;
import ca.uhn.fhir.model.dstu2.resource.Immunization;
import ca.uhn.fhir.model.dstu2.resource.Immunization.VaccinationProtocol;
import ca.uhn.fhir.model.dstu2.resource.OperationOutcome;
import ca.uhn.fhir.model.dstu2.resource.Patient;
import ca.uhn.fhir.model.dstu2.valueset.IssueSeverityEnum;
import ca.uhn.fhir.model.dstu2.valueset.MedicationAdministrationStatusEnum;
import ca.uhn.fhir.model.primitive.CodeDt;
import ca.uhn.fhir.model.primitive.DateDt;
import ca.uhn.fhir.model.primitive.DateTimeDt;
import ca.uhn.fhir.model.primitive.IdDt;
import ca.uhn.fhir.model.primitive.StringDt;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.annotation.RequiredParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.param.StringParam;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.nyph.cdslibrary.CDSLibraryWrapper;
import org.nyph.cdslibrary.dto.HistoryStatusDTO;

public class ImmunizationResourceProvider implements IResourceProvider {

    private DataSource ds = null;
    private static boolean CONNECT_TO_PROD = true;
    private static final String FIELD_NAMES = "im.immunization_Id, im.vaccine_mixcode, "
            + "(select distinct(vaccine_mixdesc) from improd.vaccinemix_table where vaccine_mixcode = im.vaccine_mixcode) as vaccine_mixdesc, "
            + "ic.vaccine_medcode, "
            + "jvdg.vaccine_display_group, "
            + "(select distinct(vaccine_desc) from improd.vaccinemapper_table where vaccine_medcode = ic.vaccine_medcode) as vaccine_desc, "
            + "TO_CHAR(vaccine_date,'MM/DD/YYYY') vaccine_date, "
            + "vaccine_date vd, "
            + "f.facilitysite_id, "
            + "facility_name, "
            + "ic.vaccine_lotnum, "
            + "ic.vaccine_manufacturer, "
            + "mt.manufacturer_name, "
            + "TO_CHAR(ic.vaccine_expdate,'MM/DD/YYYY') expiration_date, "
            + "im.administrator, "
            + "(select body_desc from improd.bodysite_table where body_id = im.inject_site) as inject_site, "
            + "(select provider_lastname || ', ' || provider_firstname from improd.provider_table where provider_id = im.administrator) as administrator_name, "
            + "(select datasource_text from improd.datasource_table where datasource_id = im.datasource_id) as datasource, "
            + "(select provider_lastname || ', ' || provider_firstname from improd.provider_table where provider_id = im.attending_phys) as attending_name, "
            + "(select provider_lastname || ', ' || provider_firstname from improd.provider_table where provider_id = im.provider_id) as provider_name, "
            + "im.attending_phys, "
            + "im.provider_id, "
            + "im.upid, "
            + "im.display, "
            + "im.vis_date vis_published_date, "
            + "jvdg.display_order, "
            + "TO_CHAR(im.measurement_date,'MM/DD/YYYY') measurement_date, im.measurement1, im.measurement2, NVL(s.system_name,'EZVAC') system_name, "
            + "(select doh_code from improd.vaccinemapper_table where vaccine_medcode = ic.vaccine_medcode FETCH FIRST ROW ONLY) cvx_code, dosage, uom, "
            + "im.refusal, m.empi, m.localpatient_id, m.orgsite_id ";

    private static final String TABLE_NAMES = " improd.immunizationmix_table im, improd.immunizationcomp_table ic, improd.visit_table v, improd.facilitysite_table f, improd.jsp_vaccine_display_group jvdg, improd.systems_table s, improd.mpi_table m, improd.manufacturer_table mt ";

    private String IMMUNIZATION_ID_LOOKUP = "SELECT " + FIELD_NAMES + " FROM " + TABLE_NAMES
            + "WHERE (im.immunization_id = ic.immunization_id) and (im.visit_id = v.visit_id) and (v.facilitysite_id = f.facilitysite_id) "
            + "and (jvdg.vaccine_medcode = ic.vaccine_medcode) and NVL(im.system_id,1) = s.system_id AND NVL(ic.vaccine_manufacturer,'UNK') = mt.manufacturer_id AND im.upid = m.upid AND im.immunization_id = ?";
    private String IMMUNIZATION_UPID_LOOKUP = "SELECT " + FIELD_NAMES + " FROM " + TABLE_NAMES
            + "WHERE (im.immunization_id = ic.immunization_id) and (im.visit_id = v.visit_id) and (v.facilitysite_id = f.facilitysite_id) "
            + "and (jvdg.vaccine_medcode = ic.vaccine_medcode) and NVL(im.system_id,1) = s.system_id AND NVL(ic.vaccine_manufacturer,'UNK') = mt.manufacturer_id AND im.upid = m.upid AND im.upid = ?";
    private String IMMUNIZATION_UPID_STATUS_LOOKUP = "SELECT " + FIELD_NAMES + " FROM " + TABLE_NAMES
            + "WHERE (im.immunization_id = ic.immunization_id) and (im.visit_id = v.visit_id) and (v.facilitysite_id = f.facilitysite_id) "
            + "and (jvdg.vaccine_medcode = ic.vaccine_medcode) and NVL(im.system_id,1) = s.system_id AND NVL(ic.vaccine_manufacturer,'UNK') = mt.manufacturer_id AND im.upid = m.upid AND im.upid = ? AND display = ?";

    private String IMMUNIZATION_EMPI_LOOKUP = "SELECT " + FIELD_NAMES + " FROM " + TABLE_NAMES
            + "WHERE (im.immunization_id = ic.immunization_id) and (im.visit_id = v.visit_id) and (v.facilitysite_id = f.facilitysite_id) "
            + "and (jvdg.vaccine_medcode = ic.vaccine_medcode) and NVL(im.system_id,1) = s.system_id AND NVL(ic.vaccine_manufacturer,'UNK') = mt.manufacturer_id AND im.upid = m.upid AND m.empi = ?";
    private String IMMUNIZATION_EMPI_STATUS_LOOKUP = "SELECT " + FIELD_NAMES + " FROM " + TABLE_NAMES
            + "WHERE (im.immunization_id = ic.immunization_id) and (im.visit_id = v.visit_id) and (v.facilitysite_id = f.facilitysite_id) "
            + "and (jvdg.vaccine_medcode = ic.vaccine_medcode) and NVL(im.system_id,1) = s.system_id AND NVL(ic.vaccine_manufacturer,'UNK') = mt.manufacturer_id AND im.upid = m.upid AND m.empi = ? AND display = ?";

    /**
     * Constructor
     */
    public ImmunizationResourceProvider() {
    }

    /**
     * The getResourceType method comes from IResourceProvider, and must be
     * overridden to indicate what type of resource this provider supplies.
     *
     * @return
     */
    @Override
    public Class<Immunization> getResourceType() {
        return Immunization.class;
    }

    /**
     * This function returns the immunization record for a given immunizatoin id
     *
     * @param theId - the immunization id.
     * @return
     */
    @Read(version = true)
    public Immunization getResourceById(@IdParam IdDt theId) {
        String immunizationId = theId.getIdPart();

        try {
            if (immunizationId != null) {
                System.out.println("Id parameter:" + immunizationId);
                List<Immunization> immunizations = getImmunizationRecordsFromDatabase(immunizationId, null, null, null);
                if (immunizations.size() == 0) {
                    throw new ResourceNotFoundException("No immunization record present for immunization id = " + immunizationId);
                } else {
                    return immunizations.get(0);
                }
            } else {
                OperationOutcome oo = new OperationOutcome();
                oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("No EMPI specified");
                throw new InternalErrorException("No EMPI specified", oo);
            }
        } catch (ResourceNotFoundException ex) {
            System.out.println("Threw Resource not found exception");
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.INFORMATION).setDetails(ex.getMessage());
            throw new InternalErrorException(ex.getMessage(), oo);
        } catch (Exception e) {
            e.printStackTrace();
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails(e.getMessage());
            throw new InternalErrorException(e.getMessage(), oo);
        }
    }

    /**
     * Given a upid, return the entire immunization history for this patient.
     * The status is optional and can include the following values... Y - all
     * completed active displayable shots N - deleted shots I - inactive shots
     * Setting schedule to Y causes the recommendation engine to run. This will
     * allow the system to fill in the shot number as well as the status of the
     * various shots. Be warned, this can take some time so don't run it unless
     * you really really need it.
     *
     * @param upid
     * @param status - optional (Y - Yes, N - No, I - Invalid)
     * @param schedule - optional (Y compute shot number in schedule, N do not
     * compute shot number in schedule)
     * @return
     */
    @Search()
    public List<Immunization> findImmunizationsByUPID(@RequiredParam(name = Patient.SP_IDENTIFIER) StringDt upid,
                                                      //public List<Immunization> findImmunizationsByUPID(@RequiredParam(name = "patient") StringDt upid,
                                                      @OptionalParam(name = Immunization.SP_STATUS) StringParam status,
                                                      @OptionalParam(name = "schedule") StringParam schedule,
                                                      @OptionalParam(name = "auditdate") StringParam strAuditDate) {
        List<Immunization> retVal = new ArrayList<Immunization>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date auditDate = null;

        System.out.println("Parameter for audit date is " + strAuditDate);

        try {
            Date today = new Date();
            if (strAuditDate != null) {
                auditDate = sdf.parse(strAuditDate.getValue().toString());
                System.out.println("Parsed Audit Date: " + auditDate);
                if (auditDate.compareTo(today) > 0) {
                    OperationOutcome oo = new OperationOutcome();
                    oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("Invalid audit date, correct format is YYYYMMdd and audit date cannot be in the future");
                    throw new InternalErrorException("Invalid audit date, correct format is YYYYMMdd and audit date cannot be in the future", oo);
                }
            }
        } catch (Exception e) {
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("Invalid audit date, correct format is YYYYMMdd");
            throw new InternalErrorException("Invalid audit date, correct format is YYYYMMdd", oo);
        }

        try {
            if (upid != null) {
                List<Immunization> immunizations = null;
                if (status == null) {
                    immunizations = getImmunizationRecordsFromDatabase(null, upid.getValue().toString(), "upid", null);
                } else {
                    immunizations = getImmunizationRecordsFromDatabase(null, upid.getValue().toString(), "upid", status.getValue().toString());
                }
                if (immunizations.size() == 0) {
                    throw new ResourceNotFoundException("No immunizations found for upid = " + upid.getValue().toString());
                } else {
                    // Check to see if we need to run the recommendations engine against this history.
                    System.out.println("Checking on schedule");
                    if ((schedule != null) && schedule.getValue().toString().equalsIgnoreCase("Y")) {
                        immunizations = computeShotStatuses(upid.getValue().toString(), auditDate, immunizations);
                    }
                    return immunizations;
                }
            } else {
                OperationOutcome oo = new OperationOutcome();
                oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("No EMPI specified");
                throw new InternalErrorException("No EMPI specified", oo);
            }
        } catch (ResourceNotFoundException ex) {
            System.out.println("Threw Resource not found exception");
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.INFORMATION).setDetails(ex.getMessage());
            throw new ResourceNotFoundException(ex.getMessage(), oo);
        } catch (Exception e) {
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.ERROR).setDetails(e.getMessage());
            throw new InternalErrorException(e.getMessage(), oo);
        }
    }

    /**
     * Given a empi, return the entire immunization history for this patient.
     * The status is optional and can include the following values... Y - all
     * completed active displayable shots N - deleted shots I - inactive shots
     *
     * @param upid
     * @param status
     * @return
     */
    @Search()
    public List<Immunization> findImmunizationsByEMPI(@RequiredParam(name = "empi") StringDt empi,
                                                      @OptionalParam(name = Immunization.SP_STATUS) StringParam status) {
        List<Immunization> retVal = new ArrayList<Immunization>();

        try {
            if (empi != null) {
                List<Immunization> immunizations = null;
                if (status == null) {
                    immunizations = getImmunizationRecordsFromDatabase(null, empi.getValue().toString(), "empi", null);
                } else {
                    immunizations = getImmunizationRecordsFromDatabase(null, empi.getValue().toString(), "empi", status.getValue().toString());
                }
                if (immunizations.size() == 0) {
                    throw new ResourceNotFoundException("No immunizations present for empi = " + empi.getValue().toString());
                } else {
                    return immunizations;
                }
            } else {
                OperationOutcome oo = new OperationOutcome();
                oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("No EMPI specified");
                throw new InternalErrorException("No EMPI specified", oo);
            }
        } catch (ResourceNotFoundException ex) {
            System.out.println("Threw Resource not found exception");
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.INFORMATION).setDetails(ex.getMessage());
            throw new InternalErrorException(ex.getMessage(), oo);
        } catch (Exception e) {
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails(e.getMessage());
            throw new InternalErrorException(e.getMessage(), oo);
        }
    }

    /**
     * Generic function that will return a set of immunization records from the
     * database.
     *
     * @param id - the immunization id
     * @param upid - The upid
     * @param status - status of Y, N or I
     * @return
     * @throws Exception
     */
    private List<Immunization> getImmunizationRecordsFromDatabase(String id, String patientId, String patientIdType, String status) throws Exception {
        HashMap<Long, Immunization> uniqueImmunizations = new HashMap();
        List<Immunization> retVal = new ArrayList<Immunization>();
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String query = null;

        /* Figure out the query to use */
        if (id != null) {
            query = IMMUNIZATION_ID_LOOKUP;
        } else if (patientId != null) {
            if (patientIdType.equalsIgnoreCase("upid")) {
                if (status == null) {
                    query = IMMUNIZATION_UPID_LOOKUP;
                } else {
                    query = IMMUNIZATION_UPID_STATUS_LOOKUP;
                }
            } else if (status == null) {
                query = IMMUNIZATION_EMPI_LOOKUP;
            } else {
                query = IMMUNIZATION_EMPI_STATUS_LOOKUP;
            }

        } else {
            throw new Exception("Unknown lookup type - valid ones are id or upid");
        }

        // Initialize, query and populate the FHIR patient object 
        try {
            System.out.println("STATUS: The query is ");
            System.out.println(query);
            Context ctx = new InitialContext();
            ds = (DataSource) ctx.lookup("java:comp/env/jdbc/nypis");
            conn = ds.getConnection();
            pstmt = conn.prepareStatement(query);

            // Fill in the parameters
            if (id != null) {
                pstmt.setString(1, id);
            } else if (patientId != null) {
                pstmt.setString(1, patientId);
                if (status != null) {
                    pstmt.setString(2, status);
                }
            }

            rs = pstmt.executeQuery();
            while (rs.next()) {
                // Check to see if this is a duplicate record based on immunization_id, since we can get multiple rows for the
                // same immunization (components)
                Immunization im = uniqueImmunizations.get(rs.getLong("immunization_id"));
                if (im == null) {
                    im = new Immunization();
                    im.setId(new Long(rs.getLong("immunization_id")).toString());
                    im.setDate(new DateTimeDt(rs.getDate("vaccine_date")));

                    // Setup the coding system - medcode
                    CodingDt medcodeCoding = new CodingDt();
                    medcodeCoding.setSystem("medcode");
                    medcodeCoding.setCode(new Long(rs.getLong("vaccine_mixcode")).toString());
                    medcodeCoding.setDisplay(rs.getString("vaccine_mixdesc"));
                    medcodeCoding.setUserSelected(true);

                    // Setup the coding system - cvx
                    CodingDt cvxCoding = new CodingDt();
                    cvxCoding.setSystem("http://hl7.org/fhir/sid/cvx");
                    cvxCoding.setCode(new Long(rs.getLong("cvx_code")).toString());
                    cvxCoding.setDisplay(rs.getString("vaccine_mixdesc"));
                    cvxCoding.setUserSelected(true);

                    // Add these 2 codes to the record
                    CodeableConceptDt ccdt = new CodeableConceptDt();
                    ccdt.addCoding(medcodeCoding);
                    ccdt.addCoding(cvxCoding);

                    // The Reference to the patient object
                    ResourceReferenceDt patient = new ResourceReferenceDt();
                    patient.setReference("Patient/" + rs.getString("upid"));
                    im.setPatient(patient);

                    // The reference to the manufacturer object
                    ResourceReferenceDt manufacturer = new ResourceReferenceDt();
                    manufacturer.setDisplay(rs.getString("manufacturer_name"));
                    manufacturer.setReference("Organization/" + rs.getString("vaccine_manufacturer"));
                    im.setManufacturer(manufacturer);

                    // The reference to the facility object
                    ResourceReferenceDt facility = new ResourceReferenceDt();
                    facility.setDisplay(rs.getString("facility_name"));
                    facility.setReference("Location/" + rs.getString("facilitysite_id"));
                    im.setLocation(facility);

                    // The reference to the administrator
                    ResourceReferenceDt administrator = new ResourceReferenceDt();
                    administrator.setDisplay(rs.getString("administrator_name"));
                    administrator.setReference("Practitioner/" + rs.getString("administrator"));
                    im.setPerformer(administrator);

                    // The reference to the provider
                    ResourceReferenceDt provider = new ResourceReferenceDt();
                    provider.setDisplay(rs.getString("provider_name"));
                    provider.setReference("Practitioner/" + rs.getString("provider_id"));
                    im.setRequester(provider);

                    im.setVaccineCode(ccdt);

                    CodeDt statusCode = new CodeDt();
                    if (rs.getString("display").equalsIgnoreCase("Y")) {
                        statusCode.setValue(MedicationAdministrationStatusEnum.COMPLETED.toString());
                    } else if (rs.getString("display").equalsIgnoreCase("N") || rs.getString("display").equalsIgnoreCase("I")) {
                        statusCode.setValue(MedicationAdministrationStatusEnum.ENTERED_IN_ERROR.toString());
                    } else {
                        statusCode.setValue(MedicationAdministrationStatusEnum.STOPPED.toString());
                    }
                    im.setStatus(statusCode);

                    if (rs.getString("vaccine_lotnum") != null)
                        im.setLotNumber(rs.getString("vaccine_lotnum"));
                    if (rs.getDate("expiration_date") != null) {
                        im.setExpirationDate(new DateDt(rs.getDate("expiration_date")));
                    }

                    String bodySite = rs.getString("inject_site");
                    if (bodySite != null) {
                        CodingDt bc = new CodingDt();
                        bc.setCode(getManufactuerFHIRCode(rs.getString("inject_site")));
                        bc.setDisplay(rs.getString("inject_site"));
                        CodeableConceptDt bs = new CodeableConceptDt();
                        bs.addCoding(bc);
                        im.setSite(bs);
                    }

                    // Series name
                    VaccinationProtocol vp = new VaccinationProtocol();
                    vp.setSeries(rs.getString("vaccine_display_group"));
                    im.addVaccinationProtocol(vp);

                    // If this shot is an antibody, we need to add this in as a CIR recommendation rule to be counted
                    HashMap<Integer, String> antiBodyMap = getAntiBodyMap();
                    String antiBodyGroupDesc = antiBodyMap.get(rs.getInt("vaccine_mixcode"));
                    if (antiBodyGroupDesc != null) {
                        VaccinationProtocol antiBodyVP = new VaccinationProtocol();
                        antiBodyVP.setDescription("CIR recommendation rules");
                        antiBodyVP.setSeries(antiBodyGroupDesc);
                        im.addVaccinationProtocol(antiBodyVP);
                    }

                    // Dosage and unit of measure
                    SimpleQuantityDt sq = new SimpleQuantityDt();
                    sq.setUnit(rs.getString("uom"));
                    if (rs.getString("dosage") != null) {
                        Double d = Double.parseDouble(rs.getString("dosage").replaceAll(".*?([\\d.]+).*", "$1"));
                        sq.setValue(d);
                    }
                    im.setDoseQuantity(sq);

                    // Datasource
                    if (rs.getString("datasource") != null)
                        im.setReported((rs.getString("datasource").equalsIgnoreCase("parental report")) ? true : false);
                    else
                        im.setReported(false);

                    uniqueImmunizations.put(rs.getLong("immunization_id"), im);

                } else {
                    // Only populate the lotnum and expiration date and manufacturer
                    if (rs.getString("vaccine_lotnum") != null) {
                        if (im.getLocation() == null)
                            im.setLotNumber(rs.getString("vaccine_lotnum"));
                        else
                            im.setLotNumber(im.getLotNumber() + ", " + rs.getString("vaccine_lotnum"));
                    }
                    VaccinationProtocol vp = new VaccinationProtocol();
                    vp.setSeries(rs.getString("vaccine_display_group"));
                    im.addVaccinationProtocol(vp);
                    uniqueImmunizations.put(rs.getLong("immunization_id"), im);
                }
            }
            rs.close();
            pstmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("Error in Immunization lookup: " + e.getMessage());
            throw new InternalErrorException("Error in Immunization lookup: " + e.getMessage(), oo);
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

        // Convert HashMap to List
        retVal = new ArrayList<Immunization>(uniqueImmunizations.values());

        return retVal;
    }

    /**
     * Given a EzVac description for a manufacturer return the corresponding
     * FHIR equivalent code for that manufacturer. Some don't exist in the FHIR
     * standard, so this needs following up with FHIR.
     *
     * @param manufacturer
     * @return
     */
    private String getManufactuerFHIRCode(String manufacturer) {
        manufacturer = manufacturer.toLowerCase();
        if (manufacturer.equals("left arm")) {
            return "LA";
        } else if (manufacturer.equals("right arm")) {
            return "RA";
        } else if (manufacturer.equals("left leg")) {
            return "LL";
        } else if (manufacturer.equals("right leg")) {
            return "RL";
        } else if (manufacturer.equals("left buttock")) {
            return "LB";
        } else if (manufacturer.equals("right buttock")) {
            return "RB";
        } else if (manufacturer.equals("po")) {
            return "PO";
        } else if (manufacturer.equals("left deltoid")) {
            return "LD";
        } else if (manufacturer.equals("right deltoid")) {
            return "RD";
        } else if (manufacturer.equals("left thigh")) {
            return "LT";
        } else if (manufacturer.equals("right thigh")) {
            return "RT";
        } else if (manufacturer.equals("left upper arm")) {
            return "LUA";
        } else if (manufacturer.equals("right upper arm")) {
            return "RUA";
        } else if (manufacturer.equals("intranasal")) {
            return "I";
        } else {
            return "U";
        }
    }

    /**
     * Given a list of immunization histories, compute the shot number and shot
     * validity
     *
     * @param history
     * @return
     */
    private List<Immunization> computeShotStatuses(String upid, Date auditDate, List<Immunization> history) throws Exception {
        Connection conn = null;
        List<Immunization> retVal = new ArrayList();
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd");

        System.out.println("Schedule check called");

        if (auditDate == null)
            auditDate = new Date();

        try {
            Context ctx = new InitialContext();
            ds = (DataSource) ctx.lookup("java:comp/env/jdbc/nypis");
            conn = ds.getConnection();

            CDSLibraryWrapper cds = new CDSLibraryWrapper(conn, CONNECT_TO_PROD);
            cds.getSchedule(upid, auditDate);

            List<HistoryStatusDTO> statusHistory = null;
            statusHistory = cds.getHistory();

            System.out.println("Original History size is " + history.size());
            System.out.println("Processed status History size is " + statusHistory.size());

            for (int i = 0; i < history.size(); i++) {
                Immunization im = history.get(i);

                System.out.println("ID = " + im.getId().getIdPart());

                for (int j = 0; j < statusHistory.size(); j++) {
                    HistoryStatusDTO h = statusHistory.get(j);
                    System.out.println(">>> Immunization id: " + h.getImmunizationId());
                    if (h.getImmunizationId() == 11285275) System.out.println("I found immunization id = 11285275");
                    if (im.getId().getIdPart().equalsIgnoreCase(h.getImmunizationId().toString())) {
                        if (h.getImmunizationId() == 11285275) System.out.println("\tStep 1");
                        if (h.getSeries() != null) {
                            if (h.getImmunizationId() == 11285275) System.out.println("\tStep 2");
                            String tokens[] = h.getSeries().split(",");
                            for (int k = 0; k < tokens.length; k++) {
                                if (h.getImmunizationId() == 11285275) System.out.println("\tStep 3");
                                VaccinationProtocol vp = new VaccinationProtocol();
                                vp.setDoseSequence(h.getShotNumber());
                                vp.setDescription("CIR recommendation rules");
                                vp.setSeries(formatSeriesName(tokens[k]));

                                // Does this dose count towards shot
                                CodeableConceptDt cc = new CodeableConceptDt();
                                CodingDt count = new CodingDt();
                                count.setCode(h.isValid() ? "counts" : "nocount");
                                count.setDisplay(h.isValid() ? "Counts" : "Does Not Count");
                                cc.addCoding(count);
                                vp.setDoseStatus(cc);

                                // Why does this dose count or not count
                                String[] reasonTokens = h.getReason().split(",", -1);
                                System.out.println("For : " + im.getId().getIdPart() + ")----> We have ******** tokens " + vp.getSeries() + " -> " + tokens.length + " : " + reasonTokens.length + " [" + h.getReason() + "]");
                                //if (tokens.length == (reasonTokens.length-1)) {
                                CodeableConceptDt ccr = new CodeableConceptDt();
                                CodingDt reason = new CodingDt();
                                reason.setDisplay(reasonTokens[k]);
                                ccr.addCoding(reason);
                                System.out.println("\tSetting Reason: " + reasonTokens[k] + " where k = " + k);
                                vp.setDoseStatusReason(ccr);
                                //}
                                im.addVaccinationProtocol(vp);
                            }
                        }
                    }
                }

                //System.out.println("\tFor id = " + im.getId().getIdPart() + " I have " + im.getVaccinationProtocol().size() + " protocols");
                // If there are shots that are not fitting in the ICE model, we need to add them in as well with the series name being vaccine group
                // so Eric/Allscripts can parse.
                if (im.getVaccinationProtocol().size() == 1) {
                    String currentProtocol = im.getVaccinationProtocol().get(0).getSeries().toString();
                    VaccinationProtocol vp = new VaccinationProtocol();
                    vp.setDescription("CIR recommendation rules");
                    if (currentProtocol.equalsIgnoreCase("HepB"))
                        currentProtocol = "Hep B";
                    if (currentProtocol.equalsIgnoreCase("Pneumo"))
                        currentProtocol = "PCV";
                    boolean isMenBSpecialGroup = false;
                    for (int k = 0; k < im.getVaccineCode().getCoding().size(); k++) {
                        CodingDt coding = im.getVaccineCode().getCoding().get(k);
                        if (coding.getSystem().equalsIgnoreCase("http://hl7.org/fhir/sid/cvx")) {
                            if (coding.getCode().equals("162") || coding.getCode().equals("163") || coding.getCode().equals("164")) {
                                vp.setSeries("MenB Vaccine Group");
                                isMenBSpecialGroup = true;
                            }
                        }
                    }
                    if (!isMenBSpecialGroup)
                        vp.setSeries(currentProtocol + " Vaccine Group");
                    im.addVaccinationProtocol(vp);
                }

                retVal.add(im);
            }

        } catch (Exception e) {
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("Error in Immunization lookup: " + e.getMessage());
            throw new InternalErrorException("Error in Immunization lookup: " + e.getMessage(), oo);
        } finally {
            if (conn != null) {
                conn.close();
                conn = null;
            }
        }

        System.out.println("retval size is " + retVal.size());
        return retVal;
    }

    private String formatSeriesName(String series) {
        String ezvacSeries = series;
        if (ezvacSeries != null) {
            ezvacSeries = series.replaceAll("Immunization Evaluation Focus \\(", "");
            ezvacSeries = ezvacSeries.replaceAll("\\)", "");
        }
        return ezvacSeries;
    }

    private HashMap<Integer, String> getAntiBodyMap() {
        HashMap<Integer, String> antiBodyMap = new HashMap();

        antiBodyMap.put(32373, "Varicella Vaccine Group");
        antiBodyMap.put(32314, "Hep B Vaccine Group");
        antiBodyMap.put(32368, "MMR Vaccine Group");    // Measels Antibody
        antiBodyMap.put(32349, "MMR Vaccine Group");    // Mumps Antibody
        antiBodyMap.put(32067, "MMR Vaccine Group");    // Rubella Antibody
        antiBodyMap.put(32182, "Hep A Vaccine Group");

        return antiBodyMap;
    }
}
