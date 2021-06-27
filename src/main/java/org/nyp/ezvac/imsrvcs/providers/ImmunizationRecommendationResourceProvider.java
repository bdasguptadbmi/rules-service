/**
 * ImmunizationRecommendationResourceProvider
 *
 * @author Balendu Dasgupta
 * @version 2.0
 * <p>
 * This file implements the ImmunizationRecommendation resource type. Given a
 * EMPI, it will first query for the entire immunization history, and then
 * compute the recommendations by calling the CIR UTD service.
 * <p>
 * When running it on your local machine, you can use the below URL as an
 * example
 * http://localhost:8080/imsrvcs/services/ImmunizationRecommendation/2070246
 * <p>
 * Given upid and an audit date
 * http://localhost:8080/imsrvcs/services/ImmunizationRecommendation/?identifier=2070246&auditdate=20160715
 * <p>
 * Given an EMPI and icd9 and icd10 codes
 * http://localhost:8080/imsrvcs/services/ImmunizationRecommendation?empi=1005404275&icd9=745.6|748&icd10=123.4|67.9
 * <p>
 * * To get the data back in JSON format... curl -H
 * "Accept:application/json+fhir"
 * http://localhost:8080/imsrvcs/services/ImmunizationRecommendation?empi=1005404275
 * curl -H "Accept:application/json"
 * http://localhost:8080/imsrvcs/services/ImmunizationRecommendation?empi=1005404275
 * Revision History
 */
package org.nyp.ezvac.imsrvcs.providers;

import ca.uhn.fhir.model.dstu2.composite.CodeableConceptDt;
import ca.uhn.fhir.model.dstu2.composite.ResourceReferenceDt;
import ca.uhn.fhir.model.dstu2.resource.ImmunizationRecommendation;
import ca.uhn.fhir.model.dstu2.resource.ImmunizationRecommendation.Recommendation;
import ca.uhn.fhir.model.dstu2.resource.ImmunizationRecommendation.RecommendationDateCriterion;
import ca.uhn.fhir.model.dstu2.resource.ImmunizationRecommendation.RecommendationProtocol;
import ca.uhn.fhir.model.dstu2.resource.OperationOutcome;
import ca.uhn.fhir.model.dstu2.resource.Patient;
import ca.uhn.fhir.model.dstu2.valueset.IssueSeverityEnum;
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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.nyp.ezvac.imsrvcs.rules.NYPFluRules;
import org.nyp.ezvac.imsrvcs.rules.NYPRules;
import org.nyph.cdslibrary.CDSLibraryWrapper;
import org.nyph.cdslibrary.dto.HistoryStatusDTO;
import org.nyph.cdslibrary.dto.ImmunizationRecommendationDTO;

public class ImmunizationRecommendationResourceProvider implements IResourceProvider {

    private DataSource ds = null;
    private static boolean CONNECT_TO_PROD = true;
    private static final String FIELD_NAMES = "a.immunization_id, b.doh_code, a.vaccine_date ";
    private static final String TABLE_NAMES = "improd.immunizationmix_table a, improd.vaccinemapper_table b ";
    private static final String TABLE_NAMES_EMPI = "improd.immunizationmix_table a, improd.vaccinemapper_table b, improd.mpi_table c ";

    private static final String GET_IMMUNIZATIONS_BY = "SELECT " + FIELD_NAMES + " FROM " + TABLE_NAMES
            + " WHERE (a.vaccine_mixcode = b.vaccine_medcode) AND a.upid = ? AND a.display='Y'";

    private static final String GET_UPIDS_FROM_EMPI = "SELECT upid FROM improd.mpi_table WHERE empi = ? order by CIR_SYNC_TIME desc";

    private static boolean MENB_RULES_EXIST = false;
    private static String CIRSYNC_URL = "https://immunize.nyp.org/cir-sync-service/CIRSyncService?upid=";

    /**
     * Constructor
     */
    public ImmunizationRecommendationResourceProvider() {
    }

    /**
     * The getResourceType method comes from IResourceProvider, and must be
     * overridden to indicate what type of resource this provider supplies.
     *
     * @return
     */
    @Override
    public Class<ImmunizationRecommendation> getResourceType() {
        return ImmunizationRecommendation.class;
    }

    /**
     * Given a UPID, run the CIR recommendation rules against the history
     * derived from the database.
     *
     * @param theId
     * @param auditDate
     * @return
     */
    @Read()
    public ImmunizationRecommendation getResourceById(@IdParam IdDt theId) {
        ImmunizationRecommendation im = new ImmunizationRecommendation();
        List<ImmunizationRecommendation> recommendations = new ArrayList();
        List<HistoryStatusDTO> history = null;
        List<ImmunizationRecommendationDTO> cdsRecommendations = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Connection conn = null;
        Date auditDate = new Date();

        if ((theId == null)) {
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("No UPID specified");
            throw new InternalErrorException("No UPID specified", oo);
        }
        String upid = theId.getIdPart();

        try {
            if (auditDate == null) {
                auditDate = new Date();
            }
            Context ctx = new InitialContext();
            ds = (DataSource) ctx.lookup("java:comp/env/jdbc/nypis");
            conn = ds.getConnection();

            // Do a quick CIR sync first
            try {
                URL url = new URL(CIRSYNC_URL + upid);
                URLConnection urlc = url.openConnection();
                BufferedReader br = new BufferedReader(new InputStreamReader(urlc.getInputStream()));
                String l = null;
                while ((l = br.readLine()) != null) {
                    System.out.println(l);
                }
                br.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            CDSLibraryWrapper cds = new CDSLibraryWrapper(conn, CONNECT_TO_PROD);
            System.out.println("Using URL: " + cds.getCIREndPoint());
            cds.getSchedule(upid, new Date());
            history = cds.getHistory();
            cdsRecommendations = cds.getRecommendations();

            im.setId(upid);
            ResourceReferenceDt patient = new ResourceReferenceDt();
            patient.setReference("Patient/" + upid);
            im.setPatient(patient);

            for (int i = 0; i < cdsRecommendations.size(); i++) {
                ImmunizationRecommendationDTO ir = cdsRecommendations.get(i);
                System.out.println(ir);
                Recommendation r = new Recommendation();
                r.setDate(new DateTimeDt(auditDate));

                CodeableConceptDt groupName = new CodeableConceptDt();
                groupName.setText(formatSeriesName(ir.getEvaluatonDescription()));
                r.setVaccineCode(groupName);

                CodeableConceptDt recommendationName = new CodeableConceptDt();
                recommendationName.setText(ir.getRecommendatoinString());
                r.setForecastStatus(recommendationName);

                RecommendationProtocol rp = new RecommendationProtocol();
                rp.setSeries(formatSeriesName(ir.getEvaluatonDescription()));
                rp.setDescription(ir.getRecommendationCode() + " - " + ir.getInterpretationCode());
                r.setProtocol(rp);

                if (ir.getProposedDate() != null) {
                    RecommendationDateCriterion rdc = new RecommendationDateCriterion();
                    rdc.setValue(new DateTimeDt(ir.getProposedDate()));
                    r.addDateCriterion(rdc);
                }

                String supportingImmunizationLink = "Immunization?identifier=" + upid + "&schedule=Y&auditdate=" + sdf.format(auditDate);

                ResourceReferenceDt srr = new ResourceReferenceDt();
                srr.setReference(supportingImmunizationLink);

                ArrayList al = new ArrayList();
                al.add(srr);
                r.setSupportingImmunization(al);

                im.addRecommendation(r);

            }

            for (int i = 0; i < history.size(); i++) {
                System.out.println(history.get(i));
            }

        } catch (Exception e) {
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("Error in recommendations calculations: " + e.getMessage());
            throw new InternalErrorException("Error in recommendations calculations: " + e.getMessage(), oo);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
                conn = null;
            } catch (Exception e) {
            }
        }

        return im;
    }

    /**
     * Given a EMPI, run the CIR recommendation rules against the history
     * derived from the database.
     *
     * @param empi
     * @param icd9 - optional icd9 codes to help with the recommendations
     * @param icd10 - optional icd10 codes to help with the recommendations
     * @return
     */
    @Search()
    public ImmunizationRecommendation getResourceByEMPI(@RequiredParam(name = "empi") StringDt empi,
                                                        @OptionalParam(name = "icd9") StringParam icd9,
                                                        @OptionalParam(name = "icd10") StringParam icd10,
                                                        @OptionalParam(name = "auditdate") StringParam auditDateStr) {
        ImmunizationRecommendation im = new ImmunizationRecommendation();
        List<ImmunizationRecommendation> recommendations = new ArrayList();
        List<HistoryStatusDTO> history = null;
        List<ImmunizationRecommendationDTO> cdsRecommendations = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Date auditDate = new Date();
        String supportingImmunizationLink = null;

        if ((empi == null)) {
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("No UPID specified");
            throw new InternalErrorException("No EMPI specified", oo);
        }

        try {
            if (auditDateStr != null) {
                SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
                try {
                    auditDate = formatter.parse(auditDateStr.getValue());
                } catch (Exception e) {
                    OperationOutcome oo = new OperationOutcome();
                    oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("Invalid audit date format, correct format is yyyyMMdd");
                    throw new InternalErrorException("Invalid audit date format, correct format is yyyyMMdd", oo);
                }
            }
            Context ctx = new InitialContext();
            ds = (DataSource) ctx.lookup("java:comp/env/jdbc/nypis");
            conn = ds.getConnection();

            // Given an EMPI figure out the UPID's.  If there are more than one UPID's associated with it then we need to 
            // do a patient merge
            pstmt = conn.prepareStatement(GET_UPIDS_FROM_EMPI);
            pstmt.setString(1, empi.getValue().toString());
            rs = pstmt.executeQuery();
            String upid = null;
            if (rs.next()) {
                upid = rs.getString("upid");
            }
            rs.close();
            pstmt.close();

            if (upid != null) {
                // Do a quick CIR sync first
                try {
                    URL url = new URL(CIRSYNC_URL + upid);
                    URLConnection urlc = url.openConnection();
                    BufferedReader br = new BufferedReader(new InputStreamReader(urlc.getInputStream()));
                    String l = null;
                    while ((l = br.readLine()) != null) {
                        System.out.println(l);
                    }
                    br.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                CDSLibraryWrapper cds = new CDSLibraryWrapper(conn, CONNECT_TO_PROD);
                System.out.println("Using EMPI URL: " + cds.getCIREndPoint());
                cds.getSchedule(upid, auditDate);
                history = cds.getHistory();
                cdsRecommendations = cds.getRecommendations();

                im.setId(upid);
                ResourceReferenceDt patient = new ResourceReferenceDt();
                patient.setReference("Patient/" + upid);
                im.setPatient(patient);

                for (int i = 0; i < cdsRecommendations.size(); i++) {
                    ImmunizationRecommendationDTO ir = cdsRecommendations.get(i);
                    System.out.println(ir);
                    Recommendation r = new Recommendation();
                    System.out.println("**** Using Audit Date: " + auditDate);
                    r.setDate(new DateTimeDt(auditDate));

                    CodeableConceptDt groupName = new CodeableConceptDt();
                    groupName.setText(formatSeriesName(ir.getEvaluatonDescription()));
                    r.setVaccineCode(groupName);

                    CodeableConceptDt recommendationName = new CodeableConceptDt();
                    recommendationName.setText(ir.getRecommendatoinString());
                    r.setForecastStatus(recommendationName);

                    RecommendationProtocol rp = new RecommendationProtocol();
                    rp.setSeries(formatSeriesName(ir.getEvaluatonDescription()));
                    rp.setDescription(ir.getRecommendationCode() + " - " + ir.getInterpretationCode());
                    r.setProtocol(rp);

                    if (ir.getProposedDate() != null) {
                        RecommendationDateCriterion rdc = new RecommendationDateCriterion();
                        rdc.setValue(new DateTimeDt(ir.getProposedDate()));
                        r.addDateCriterion(rdc);
                    }

                    supportingImmunizationLink = "Immunization?identifier=" + upid + "&schedule=Y&auditdate=" + sdf.format(auditDate);

                    ResourceReferenceDt srr = new ResourceReferenceDt();
                    srr.setReference(supportingImmunizationLink);

                    ArrayList al = new ArrayList();
                    al.add(srr);
                    r.setSupportingImmunization(al);

                    im.addRecommendation(r);

                }

                for (int i = 0; i < history.size(); i++) {
                    System.out.println(history.get(i));
                }

                // Now apply our own rulesg 
                String icd9String = (icd9 == null) ? null : icd9.getValue().toString();
                String icd10String = (icd10 == null) ? null : icd10.getValue().toString();
                NYPRules nypRules = new NYPRules(conn, im, icd9String, icd10String, cds.getDateOfBirth(), cds.getGender(), auditDate, cds);
                Iterator<Recommendation> it = im.getRecommendation().iterator();
                while (it.hasNext()) {
                    Recommendation r = it.next();

                    if (r.getProtocol().getSeries().equalsIgnoreCase("Hep A Vaccine Group")) {
                        String result = nypRules.recommendationHepA();
                        if (result != null) {
                            r.getProtocol().setDescription(result);
                        }
                    }
                    if (r.getProtocol().getSeries().equalsIgnoreCase("Hib Vaccine Group")) {
                        String result = nypRules.recommendationHib();
                        if (result != null) {
                            r.getProtocol().setDescription(result);
                        }
                    }
                    if (r.getProtocol().getSeries().equalsIgnoreCase("Meningococcal Vaccine Group")) {
                        String result = nypRules.recommendationMeningoccocal();

                        if (result != null) {
                            r.getProtocol().setDescription(result);
                        }
                    }
                    if (r.getProtocol().getSeries().equalsIgnoreCase("HPV Vaccine Group")) {
                        String result = nypRules.recommendationHPV();

                        if (result != null) {
                            r.getProtocol().setDescription(result);
                        }
                    }
                    /*if (r.getProtocol().getSeries().equalsIgnoreCase("Meningococcal B Vaccine Group")) {
                        String result = nypRules.recommendationMenB_V2();

                        if (result != null) {
                            r.getProtocol().setDescription(result);
                        }
                    }
                    */
                    /*if (r.getProtocol().getSeries().equalsIgnoreCase("Rotavirus Vaccine Group")
                            || r.getProtocol().getSeries().equalsIgnoreCase("MMR Vaccine Group")
                            || r.getProtocol().getSeries().equalsIgnoreCase("Varicella Vaccine Group")) {
                        String result = nypRules.recommendationImmunoglobin();

                        if (result != null) {
                            r.getProtocol().setDescription(result);
                        }
                    }*/

                    // Skip the CIR rules for Flu and use our own, since there are issues with the CIR
                    // rules as of Dec 2017.
                    if (r.getProtocol().getSeries().equalsIgnoreCase("Influenza Vaccine Group")) {
                        Date seasonStartDate = null;
                        Date seasonEndDate = null;
                        DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
                        Calendar calendar = Calendar.getInstance();
                        calendar.setTime(auditDate);
                        try {
                            System.out.println("*** Audit Date: " + auditDate);
                            System.out.println("*** Current Month: " + calendar.get(Calendar.MONTH));
                            if (calendar.get(Calendar.MONTH) < 7) { // 0 based indexing
                                seasonStartDate = formatter.parse("08/01/" + (calendar.get(Calendar.YEAR) - 1));
                                seasonEndDate = formatter.parse("07/31/" + calendar.get(Calendar.YEAR));
                            } else {
                                seasonStartDate = formatter.parse("08/01/" + calendar.get(Calendar.YEAR));
                                seasonEndDate = formatter.parse("07/31/" + (calendar.get(Calendar.YEAR) + 1));
                            }
                            System.out.println("Flu Season : " + seasonStartDate + " to " + seasonEndDate);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }

                        NYPFluRules fluRules = new NYPFluRules(cds, auditDate, seasonStartDate, seasonEndDate);
                        String result = fluRules.recommendationFlu();

                        if (result != null) {
                            r.getProtocol().setDescription(result);
                            if (result.equalsIgnoreCase("NOT_RECOMMENDED - BELOW_REC_AGE_SERIES")) {
                                RecommendationDateCriterion rdc = new RecommendationDateCriterion();
                                rdc.setValue(new DateTimeDt(fluRules.getFutureRecommendedDate()));
                                r.addDateCriterion(rdc);
                                r.getForecastStatus().setText("Not Recommended");
                            } else if (result.equalsIgnoreCase("NOT_RECOMMENDED - COMPLETE")) {
                                r.getForecastStatus().setText("Not Recommended");
                            } else if (result.equalsIgnoreCase("RECOMMENDED - DUE_NOW")) {
                                r.getForecastStatus().setText("Recommended");
                            } else if (result.equalsIgnoreCase("FUTURE_RECOMMENDED - DUE_IN_FUTURE")) {
                                RecommendationDateCriterion rdc = new RecommendationDateCriterion();
                                rdc.setValue(new DateTimeDt(fluRules.getFutureRecommendedDate()));
                                r.addDateCriterion(rdc);
                                r.getForecastStatus().setText("Future Recommendation");
                            }
                        }
                    }
                }

                // Process for PPSV
                it = im.getRecommendation().iterator();
                while (it.hasNext()) {
                    Recommendation r = it.next();
                    if (r.getProtocol().getSeries().equalsIgnoreCase("PPSV Vaccine Group")) {
                        System.out.println("Original " + r.getProtocol().getSeries() + " -> " + r.getProtocol().getDescription());
                        String result = nypRules.recommendationPPSV();
                        if (result != null) {
                            r.getProtocol().setDescription(result);
                        }
                        System.out.println("New " + r.getProtocol().getSeries() + " -> " + r.getProtocol().getDescription());
                    }
                }

                // Process for PCV13
                it = im.getRecommendation().iterator();
                while (it.hasNext()) {
                    Recommendation r = it.next();
                    if (r.getProtocol().getSeries().equalsIgnoreCase("PCV Vaccine Group")) {
                        System.out.println("Original " + r.getProtocol().getSeries() + " -> " + r.getProtocol().getDescription());
                        String result = nypRules.recommendationPCV13();
                        if (result != null) {
                            r.getProtocol().setDescription(result);
                        }
                        System.out.println("New " + r.getProtocol().getSeries() + " -> " + r.getProtocol().getDescription());
                    }
                }

                // Last step of the rule is to add in the MenB recommendations which are not computed by
                // the CIR, but instead by NYP
                if (!MENB_RULES_EXIST) {
                    String rstr = nypRules.recommendationMenB();
                    if (rstr != null) {
                        Recommendation r = getMenBRecommendation(auditDate, rstr, supportingImmunizationLink);
                        im.addRecommendation(r);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("Error in recommendations calculations: " + e.getMessage());
            throw new InternalErrorException("Error in recommendations calculations: " + e.getMessage(), oo);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
                conn = null;
            } catch (Exception e) {
            }
        }

        return im;
    }

    @Search()
    public ImmunizationRecommendation getResourceById(@RequiredParam(name = Patient.SP_IDENTIFIER) StringDt id, @OptionalParam(name = "auditdate") StringParam strAuditDate) {
        ImmunizationRecommendation im = new ImmunizationRecommendation();
        List<ImmunizationRecommendation> recommendations = new ArrayList();
        List<HistoryStatusDTO> history = null;
        List<ImmunizationRecommendationDTO> cdsRecommendations = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Connection conn = null;
        Date auditDate = null;

        /* Get the audit date first */
        try {
            Date today = new Date();
            if (strAuditDate != null) {
                auditDate = sdf.parse(strAuditDate.getValue().toString());
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

        if ((id == null)) {
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("No UPID specified");
            throw new InternalErrorException("No UPID specified", oo);
        }
        String upid = id.getValue();

        try {
            if (auditDate == null) {
                auditDate = new Date();
            }
            Context ctx = new InitialContext();
            ds = (DataSource) ctx.lookup("java:comp/env/jdbc/nypis");
            conn = ds.getConnection();

            CDSLibraryWrapper cds = new CDSLibraryWrapper(conn, CONNECT_TO_PROD);
            System.out.println("Using URL: " + cds.getCIREndPoint());
            cds.getSchedule(upid, new Date());
            history = cds.getHistory();
            cdsRecommendations = cds.getRecommendations();

            im.setId(upid);
            ResourceReferenceDt patient = new ResourceReferenceDt();
            patient.setReference("Patient/" + upid);
            im.setPatient(patient);

            for (int i = 0; i < cdsRecommendations.size(); i++) {
                ImmunizationRecommendationDTO ir = cdsRecommendations.get(i);
                System.out.println(ir);
                Recommendation r = new Recommendation();
                r.setDate(new DateTimeDt(auditDate));

                CodeableConceptDt groupName = new CodeableConceptDt();
                groupName.setText(ir.getEvaluatonDescription());
                r.setVaccineCode(groupName);

                CodeableConceptDt recommendationName = new CodeableConceptDt();
                recommendationName.setText(ir.getRecommendatoinString());
                r.setForecastStatus(recommendationName);

                if (ir.getProposedDate() != null) {
                    RecommendationDateCriterion rdc = new RecommendationDateCriterion();
                    rdc.setValue(new DateTimeDt(ir.getProposedDate()));
                    r.addDateCriterion(rdc);
                }

                String supportingImmunizationLink = "Immunization?identifier=" + upid + "&schedule=Y&auditdate=" + sdf.format(auditDate);

                ResourceReferenceDt srr = new ResourceReferenceDt();
                srr.setReference(supportingImmunizationLink);

                ArrayList al = new ArrayList();
                al.add(srr);
                r.setSupportingImmunization(al);

                im.addRecommendation(r);

            }

            for (int i = 0; i < history.size(); i++) {
                System.out.println(history.get(i));
            }

        } catch (Exception e) {
            OperationOutcome oo = new OperationOutcome();
            oo.addIssue().setSeverity(IssueSeverityEnum.FATAL).setDetails("Error in recommendations calculations: " + e.getMessage());
            throw new InternalErrorException("Error in recommendations calculations: " + e.getMessage(), oo);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
                conn = null;
            } catch (Exception e) {
            }
        }

        return im;
    }

    private String formatSeriesName(String series) {
        String ezvacSeries = series;
        if (ezvacSeries != null) {
            ezvacSeries = series.replaceAll("Immunization Evaluation Focus \\(", "");
            ezvacSeries = ezvacSeries.replaceAll("\\)", "");
        }
        if (series.equalsIgnoreCase("Td")) {
            ezvacSeries = "DTP Vaccine Group";
        }
        return ezvacSeries;
    }

    private Recommendation getMenBRecommendation(Date auditDate, String recommendation, String supportingImmunizationLink) {
        Recommendation r = new Recommendation();
        r.setDate(new DateTimeDt(auditDate));

        CodeableConceptDt groupName = new CodeableConceptDt();
        groupName.setText("MenB Vaccine Group");
        r.setVaccineCode(groupName);

        CodeableConceptDt recommendationName = new CodeableConceptDt();
        String abbrRecommendation = null;
        if (recommendation.startsWith("FUTURE_RECOMMENDED")) {
            abbrRecommendation = "Future Recommendation";
        } else if (recommendation.startsWith("RECOMMENDED")) {
            abbrRecommendation = "Due Now";
        } else if (recommendation.startsWith("NOT_RECOMMENDED")) {
            abbrRecommendation = "Not Recommended";
        }
        recommendationName.setText(abbrRecommendation);
        r.setForecastStatus(recommendationName);

        RecommendationProtocol rp = new RecommendationProtocol();
        rp.setSeries("MenB Vaccine Group");
        rp.setDescription(recommendation);
        r.setProtocol(rp);

        ResourceReferenceDt srr = new ResourceReferenceDt();
        srr.setReference(supportingImmunizationLink);
        ArrayList al = new ArrayList();
        al.add(srr);
        r.setSupportingImmunization(al);

        return r;
    }
}
