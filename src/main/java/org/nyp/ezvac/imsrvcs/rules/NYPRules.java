/*
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nyp.ezvac.imsrvcs.rules;

import ca.uhn.fhir.model.dstu2.resource.ImmunizationRecommendation;
import ca.uhn.fhir.model.dstu2.resource.ImmunizationRecommendation.Recommendation;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Months;
import org.joda.time.Weeks;
import org.joda.time.Years;
import org.nyp.ezvac.commons.ICD;
import org.nyph.cdslibrary.CDSLibraryWrapper;
import org.nyph.cdslibrary.dto.HistoryStatusDTO;

/**
 * @author bdasgupt
 */
public class NYPRules {

    private Connection conn = null;
    private ImmunizationRecommendation recommendations;
    private HashSet<String> icd9Set;
    private HashSet<String> icd10Set;
    private Date dateOfBirth;
    private String gender;
    private Date auditDate;
    private CDSLibraryWrapper cds;

    private static boolean isICDValuesInitialized;

    // icd9 variables
    private static HashSet<String> hibICD9;
    private static HashSet<String> menBICD9;
    private static HashSet<String> meningoccocalICD9;
    private static HashSet<String> pcv13ICD9;
    private static HashSet<String> ppsvICD9;
    private static HashSet<String> hpvICD9;

    // icd10 variables
    private static HashSet<String> hibICD10;
    private static HashSet<String> menBICD10;
    private static HashSet<String> meningoccocalICD10;
    private static HashSet<String> pcv13ICD10;
    private static HashSet<String> ppsvICD10;
    private static HashSet<String> hpvICD10;

    public NYPRules(Connection conn, ImmunizationRecommendation recommendations, String icd9, String icd10, Date dateOfBirth, String gender, Date auditDate, CDSLibraryWrapper cds) {
        this.conn = conn;
        this.recommendations = recommendations;
        this.icd9Set = new HashSet();
        this.icd10Set = new HashSet();
        if (icd9 != null) {
            String[] icdTokens = icd9.split("\\|");
            for (int i = 0; i < icdTokens.length; i++) {
                System.out.println("icd9=" + icdTokens[i]);
                icd9Set.add(icdTokens[i]);
            }
        }
        if (icd10 != null) {
            String[] icdTokens = icd10.split("\\|");
            for (int i = 0; i < icdTokens.length; i++) {
                System.out.println("icd10=" + icdTokens[i]);
                icd10Set.add(icdTokens[i]);
            }
        }
        this.dateOfBirth = dateOfBirth;
        this.gender = gender;
        this.auditDate = auditDate;
        this.cds = cds;

        // Initialize the icd9/icd10 values from the DB if not done so already
        try {
            if (!isICDValuesInitialized) {
                ICD icd = new ICD(this.conn);
                hibICD9 = icd.getHibICD9();
                menBICD9 = icd.getMenBICD9();
                meningoccocalICD9 = icd.getMeningoccocalICD9();
                pcv13ICD9 = icd.getPcv13ICD9();
                ppsvICD9 = icd.getPpsvICD9();
                hpvICD9 = icd.getHpvICD9();
                hibICD10 = icd.getHibICD10();
                menBICD10 = icd.getMenBICD10();
                meningoccocalICD10 = icd.getMeningoccocalICD10();
                pcv13ICD10 = icd.getPcv13ICD10();
                ppsvICD10 = icd.getPpsvICD10();
                hpvICD10 = icd.getHpvICD10();
                isICDValuesInitialized = true;
            }
        } catch (Exception e) {
            isICDValuesInitialized = false;
        }
    }

    /**
     * Applies the NYP rules to the HepA Series
     *
     * @return
     */
    public String recommendationHepA() {
        String recommendation = null;

        Iterator<Recommendation> it = recommendations.getRecommendation().iterator();
        while (it.hasNext()) {
            Recommendation r = it.next();
            if (r.getProtocol().getSeries().equalsIgnoreCase("Hep A Vaccine Group")) {
                if (r.getProtocol().getDescription().startsWith("CONDITIONAL - HIGH_RISK")) {
                    int ageInYears = calculateAgeInYears();
                    if (ageInYears <= 18) {
                        recommendation = "RECOMMENDED - AGE";
                    } else {
                        recommendation = "CONDITIONAL - IF_HIGH_RISK";
                    }
                }
            }
        }

        return recommendation;
    }

    /**
     * Applies the NYP rules to the Hib Series
     *
     * @return
     */
    public String recommendationHib() {
        String recommendation = null;

        Iterator<Recommendation> it = recommendations.getRecommendation().iterator();
        while (it.hasNext()) {
            Recommendation r = it.next();
            if (r.getProtocol().getSeries().equalsIgnoreCase("Hib Vaccine Group")) {
                if (r.getProtocol().getDescription().startsWith("CONDITIONAL - HIGH_RISK")) {
                    if (isHibHighRisk()) {
                        recommendation = "RECOMMENDED_CMC - HIGH_RISK";
                    } else {
                        recommendation = "NOT_RECOMMENDED - NOT_HIGH_RISK";
                    }
                }
            }
        }

        return recommendation;
    }

    /**
     * Apply the NYP rules to the Meningoccocal Series
     *
     * @return
     */
    public String recommendationMeningoccocal() {
        String recommendation = null;

        Iterator<Recommendation> it = recommendations.getRecommendation().iterator();
        while (it.hasNext()) {
            Recommendation r = it.next();
            if (r.getProtocol().getSeries().equalsIgnoreCase("Meningococcal Vaccine Group")) {
                if (isMeningoccocalHighRisk()) {
                    int ageInYears = calculateAgeInYears();
                    if (ageInYears < 11) {
                        recommendation = "RECOMMENDED_CMC - HIGH_RISK";
                    } else {
                        recommendation = "RECOMMENDED_CMC_AGE - HIGH_RISK";
                    }
                } else if (r.getProtocol().getDescription().startsWith("CONDITIONAL - HIGH_RISK")) {
                    recommendation = "CONDITIONAL - NOT_HIGH_RISK";
                }
            }
        }

        return recommendation;
    }

    /**
     * Apply the NYP rules to the MenB Series
     *
     * @return
     */
    public String recommendationMenB() {
        String recommendation = null;
        boolean hasTrumenba = false;
        int ageInYears = calculateAgeInYears();
        ArrayList<Date> menBDates = new ArrayList();

        Iterator<HistoryStatusDTO> it = cds.getDBHistory().iterator();
        while (it.hasNext()) {
            HistoryStatusDTO node = it.next();
            if (node.getCvxCode() != null) {
                if ((node.getCvxCode() == 162) || (node.getCvxCode() == 163)) {
                    if (node.getCvxCode() == 162) {
                        hasTrumenba = true;
                    }
                    menBDates.add(node.getShotDate());
                }
            }
        }
        Collections.sort(menBDates);

        // Figure out the shot order
        Date firstShot = null;
        Date secondShot = null;
        for (int i = 0; i < menBDates.size(); i++) {
            //System.out.println("MENB Date: " + menBDates.get(i));
            if (firstShot == null) {
                firstShot = menBDates.get(i);
            } else //System.out.println("\tMENB Different " + calculateDateDifferenceInDays(firstShot, menBDates.get(i)));
                if (calculateDateDifferenceInDays(firstShot, menBDates.get(i)) >= 24) {
                    secondShot = menBDates.get(i);
                }
        }

        //if (firstShot != null) System.out.println("MENB: Have first shot " + firstShot);
        //if (secondShot != null) System.out.println("MENB: Have second shot " + secondShot);
        // Now apply the rules
        if (firstShot != null) {
            if (hasTrumenba) {
                recommendation = "NOT_SUPPORTED - OTHER_VACCINE";
            } else if (secondShot != null) {
                recommendation = "NOT_RECOMMENDED - COMPLETE";
            } else //System.out.println("MENB Total: " + menBDates.size());
                if (menBDates.size() > 1) {
                    recommendation = "RECOMMENDED - DUE_NOW_INVALID";   // Person has received more than 1 menB, but the second is invalid
                } else //System.out.println("MENB Audit Date Different: " + calculateDateDifferenceInDays(auditDate, firstShot));
                    if (calculateDateDifferenceInDays(firstShot, auditDate) >= 24) {
                        recommendation = "RECOMMENDED - DUE_NOW";
                    } else {
                        recommendation = "FUTURE_RECOMMENDED - DUE_IN_FUTURE";
                    }
        } else if (isMenBHighRisk()) {
            if (calculateAgeInYears() < 10) {
                recommendation = "FUTURE_RECOMMENDED - HIGH_RISK";
            } else {
                recommendation = "RECOMMENDED_CMC - HIGH_RISK";
            }
        } else if (calculateAgeInYears() < 10) {
            recommendation = "NOT_RECOMMENDED - AGE";
        } else if ((calculateAgeInYears() > 10) && (calculateAgeInYears() < 16)) {
            recommendation = "NOT_RECOMMENDED - NOT_HIGH_RISK";
        } else if ((calculateAgeInYears() > 16) && (calculateAgeInYears() < 24)) {
            recommendation = "RECOMMENDED - PERMISSIVE_REC";
        } else {
            recommendation = "NOT_RECOMMENDED - NOT_HIGH_RISK";
        }

        return recommendation;
    }

    public String recommendationMenB_V2() {
        String recommendation = null;

        Iterator<Recommendation> it = recommendations.getRecommendation().iterator();
        while (it.hasNext()) {
            Recommendation r = it.next();
            if (r.getProtocol().getSeries().equalsIgnoreCase("Meningococcal B Vaccine Group")) {
                if (r.getProtocol().getDescription().startsWith("CONDITIONAL - HIGH_RISK")
                        || r.getProtocol().getDescription().startsWith("CONDITIONAL - CLINICAL_PATIENT_DISCRETION")
                        || r.getProtocol().getDescription().startsWith("CONDITIONAL - TOO_OLD_HIGH_RISK")) {
                    if (isMenBHighRisk()) {
                        recommendation = "RECOMMENDED_CMC_AGE - HIGH_RISK";
                    }
                }
            }
        }
        return recommendation;
    }

    /**
     * Apply the NYP rules to the PCV13 Series
     *
     * @return
     */
    public String recommendationPCV13() {
        String recommendation = null;

        Iterator<Recommendation> it = recommendations.getRecommendation().iterator();
        while (it.hasNext()) {
            Recommendation r = it.next();
            if (r.getProtocol().getSeries().equalsIgnoreCase("PCV Vaccine Group")) {
                if (r.getProtocol().getDescription().startsWith("CONDITIONAL - HIGH_RISK")) {
                    if (isPCV13HighRisk()) {
                        recommendation = "RECOMMENDED_CMC - HIGH_RISK";
                    } else {
                        recommendation = "NOT_RECOMMENDED - NOT_HIGH_RISK";
                    }
                }
            }
        }

        return recommendation;
    }

    /**
     * Applies the NYP rules to the PPSV series
     *
     * @return
     */
    public String recommendationPPSV() {
        String recommendation = null;

        // List the PPSV CVX codes that we would look for 
        ArrayList<Integer> cvxPPSV = new ArrayList<Integer>();
        cvxPPSV.add(133);
        cvxPPSV.add(100);
        cvxPPSV.add(152);
        cvxPPSV.add(109);

        if (!isPPSVHighRisk()) {
            return null;    // Just go with the recommendations given
        } else {
            System.out.println("PPSV High Risk detected");
            int ageInYears = calculateAgeInYears();
            System.out.println("Age is " + ageInYears);
            if (ageInYears < 2) {
                return "NOT_RECOMMENDED - AGE";
            } else if ((ageInYears >= 19) && (ageInYears <= 64)) {
                return "CONDITIONAL - IF_HIGH_RISK";
            } else if (ageInYears >= 65) {
                return null;    // ust go with the recommendations given
            } else {  // Age is between 2 & 19 years
                // Interogate the PPSV ruleset first
                Iterator<Recommendation> it = recommendations.getRecommendation().iterator();
                while (it.hasNext()) {
                    Recommendation r = it.next();
                    //if (r.getProtocol().getSeries().equalsIgnoreCase("PPSV Vaccine Group")) { // We need to look at PCV for PPSV//pcv13, also check last date
                    System.out.println("Looping " + r.getProtocol().getSeries());
                    if (r.getProtocol().getSeries().equalsIgnoreCase("PCV Vaccine Group") || r.getProtocol().getSeries().equalsIgnoreCase("Pneumococcal Conjugate 13 valent (PCV 13")) {
                        System.out.println("\tUsing PCV result of " + r.getProtocol().getDescription());
                        // Have they received a PCV13 shot
                        Date pcvDate = null;
                        List<HistoryStatusDTO> history = cds.getHistory();
                        Iterator<HistoryStatusDTO> hit = history.iterator();
                        while (hit.hasNext()) {
                            HistoryStatusDTO h = hit.next();
                            if (h.getCvxCode() != null) {
                                if (cvxPPSV.contains(h.getCvxCode()) && h.isValid()) {
                                    if (pcvDate == null) {
                                        pcvDate = h.getShotDate();
                                    } else if (pcvDate.before(h.getShotDate())) {
                                        pcvDate = h.getShotDate();
                                    }
                                }
                            }
                        }

                        // Checking the output from the CIR rules
                        if (r.getProtocol().getDescription().startsWith("FUTURE_RECOMMENDED")) {
                            return "FUTURE_RECOMMENDED_CMC - PCV13_DUE_FUTURE";
                        } else if (r.getProtocol().getDescription().startsWith("RECOMMENDED")) {
                            return "FUTURE_RECOMMENDED_CMC - AFTER_PCV13";
                        } else if (r.getProtocol().getDescription().startsWith("CONDITIONAL - HIGH_RISK")) {
                            return "FUTURE_RECOMMENDED_CMC - MAY_NEED_PCV13";
                        } else if (r.getProtocol().getDescription().startsWith("NOT_RECOMMENDED - COMPLETE")) {
                            // Has it been 8 weeks from PCV13 shot date and audit date
                            if (pcvDate != null) {
                                int weeks = calculateDateDifferenceInWeeks(pcvDate, auditDate);
                                if (weeks >= 8) {
                                    return "RECOMMENDED_CMC - PCV_COMPLETE";
                                } else {
                                    return "RECOMMENDED_CMC - GIVE_AFTER_MINIMUM_INTERVAL";
                                }
                            } else {
                                return "RECOMMENDED_CMC - PCV_COMPLETE";
                            }
                        }
                    }
                }
            }
        }

        return recommendation;
    }

    /**
     * Apply the NYP rules to the HPV Series
     *
     * @return
     */
    public String recommendationHPV() {
        String recommendation = null;
        int ageInYears = calculateAgeInYears();

        if (ageInYears >= 9) {
            if (isHPVHighRisk()) {
                recommendation = "NEEDS_THREE_DOSES - CMC";
            }
        }
        return recommendation;
    }

    /**
     * Apply the NYP rules to the immunoglobin - applies to MMR, Varicella,
     * Rotavirus.
     *
     * @return
     */
    public String recommendationImmunoglobin() {
        String recommendation = null;

        Iterator<Recommendation> it = recommendations.getRecommendation().iterator();
        while (it.hasNext()) {
            Recommendation r = it.next();
            if (r.getProtocol().getSeries().equalsIgnoreCase("Rotavirus Vaccine Group")
                    || r.getProtocol().getSeries().equalsIgnoreCase("MMR Vaccine Group")
                    || r.getProtocol().getSeries().equalsIgnoreCase("Varicella Vaccine Group")) {
                if (r.getProtocol().getDescription().startsWith("RECOMMENDED - DUE_NOW")) {
                    // Check to see if immunoglobin was administered
                    Iterator<HistoryStatusDTO> itShots = cds.getDBHistory().iterator();
                    while (itShots.hasNext()) {
                        HistoryStatusDTO node = itShots.next();
                        if (node.getCvxCode() != null) {
                            if (node.getCvxCode() == 86) {
                                if (calculateDateDifferenceInMonths(auditDate, dateOfBirth) <= 3) {
                                    recommendation = "NOT_RECOMMENDED - IG_INTERVAL";
                                }
                            }
                        }
                    }

                }
            }
        }

        return recommendation;
    }

    /**
     * Checks the icd9/icd10 codes to see if this patient is a PPSV high risk
     *
     * @return
     */
    public boolean isPPSVHighRisk() {
        if (icd9Set != null) {
            Iterator<String> it = icd9Set.iterator();
            while (it.hasNext()) {
                String checkField = it.next();
                System.out.println("Checking ...PPSV  " + checkField);
                if (ppsvICD9.contains(checkField)) {
                    System.out.println("\tFound so returning true");
                    return true;
                }
            }
        }
        if (icd10Set != null) {
            Iterator<String> it = icd10Set.iterator();
            while (it.hasNext()) {
                String checkField = it.next();
                if (ppsvICD10.contains(checkField)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Checks the icd9/icd10 codes to see if this patient is a Hib high risk
     *
     * @return
     */
    public boolean isHibHighRisk() {
        if (icd9Set != null) {
            Iterator<String> it = icd9Set.iterator();
            while (it.hasNext()) {
                String checkField = it.next();
                if (hibICD9.contains(checkField)) {
                    return true;
                }
            }
        }
        if (icd10Set != null) {
            Iterator<String> it = icd10Set.iterator();
            while (it.hasNext()) {
                String checkField = it.next();
                if (hibICD10.contains(checkField)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Checks the icd9/icd10 codes to see if this patient is a Meningococcal
     * high risk
     *
     * @return
     */
    public boolean isMeningoccocalHighRisk() {
        if (icd9Set != null) {
            Iterator<String> it = icd9Set.iterator();
            while (it.hasNext()) {
                String checkField = it.next();
                if (meningoccocalICD9.contains(checkField)) {
                    return true;
                }
            }
        }
        if (icd10Set != null) {
            Iterator<String> it = icd10Set.iterator();
            while (it.hasNext()) {
                String checkField = it.next();
                if (meningoccocalICD10.contains(checkField)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Checks the icd9/icd10 codes to see if this patient is a PCV high risk
     *
     * @return
     */
    public boolean isPCV13HighRisk() {
        if (icd9Set != null) {
            Iterator<String> it = icd9Set.iterator();
            while (it.hasNext()) {
                String checkField = it.next();
                if (pcv13ICD9.contains(checkField)) {
                    return true;
                }
            }
        }
        if (icd10Set != null) {
            Iterator<String> it = icd10Set.iterator();
            while (it.hasNext()) {
                String checkField = it.next();
                if (pcv13ICD10.contains(checkField)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Checks the icd9/icd10 codes to see if this patient is a MenB high risk
     *
     * @return
     */
    public boolean isMenBHighRisk() {
        if (icd9Set != null) {
            Iterator<String> it = icd9Set.iterator();
            while (it.hasNext()) {
                String checkField = it.next();
                if (menBICD9.contains(checkField)) {
                    return true;
                }
            }
        }
        if (icd10Set != null) {
            Iterator<String> it = icd10Set.iterator();
            while (it.hasNext()) {
                String checkField = it.next();
                if (menBICD10.contains(checkField)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Checks the icd9/icd10 codes to see if this patient is a HPV high risk
     *
     * @return
     */
    public boolean isHPVHighRisk() {
        if (icd9Set != null) {
            Iterator<String> it = icd9Set.iterator();
            while (it.hasNext()) {
                String checkField = it.next();
                if (hpvICD9.contains(checkField)) {
                    return true;
                }
            }
        }
        if (icd10Set != null) {
            Iterator<String> it = icd10Set.iterator();
            while (it.hasNext()) {
                String checkField = it.next();
                if (hpvICD10.contains(checkField)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Calculate the age in years based on the audit date - date of birth
     *
     * @return
     */
    private int calculateAgeInYears() {
        DateTime dateTime1 = new DateTime(dateOfBirth);
        DateTime dateTime2 = new DateTime(auditDate);

        int years = Years.yearsBetween(dateTime1, dateTime2).getYears();
        return years;
    }

    private int calculateDateDifferenceInWeeks(Date d1, Date d2) {
        DateTime dateTime1 = new DateTime(d1);
        DateTime dateTime2 = new DateTime(d2);

        int weeks = Weeks.weeksBetween(dateTime1, dateTime2).getWeeks();
        return weeks;
    }

    private int calculateDateDifferenceInDays(Date d1, Date d2) {
        DateTime dateTime1 = new DateTime(d1);
        DateTime dateTime2 = new DateTime(d2);

        int days = Days.daysBetween(dateTime1, dateTime2).getDays(); //Weeks.weeksBetween(dateTime1, dateTime2).getWeeks();
        return days;
    }

    private int calculateDateDifferenceInMonths(Date d1, Date d2) {
        DateTime dateTime1 = new DateTime(d1);
        DateTime dateTime2 = new DateTime(d2);

        int months = Months.monthsBetween(dateTime1, dateTime2).getMonths();
        return months;
    }
}
