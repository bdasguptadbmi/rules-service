/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.nyp.ezvac.imsrvcs.rules;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Months;
import org.nyph.cdslibrary.CDSLibraryWrapper;
import org.nyph.cdslibrary.dto.HistoryStatusDTO;

/**
 * @author bdasgupt
 */
public class NYPFluRules {
    private CDSLibraryWrapper cds;
    private Date auditDate;
    private Date seasonStart;
    private Date seasonEnd;
    private Date cutOfDate;
    private Date futureRecommendedDate;

    /**
     * Constructor
     *
     * @param cds
     * @param auditDate
     */
    public NYPFluRules(CDSLibraryWrapper cds, Date auditDate, Date seasonStart, Date seasonEnd) {
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
        this.cds = cds;
        this.auditDate = auditDate;
        this.seasonStart = seasonStart;
        this.seasonEnd = seasonEnd;
        this.futureRecommendedDate = null;

        try {
            Calendar cal = new GregorianCalendar();
            cal.setTime(seasonStart);
            this.cutOfDate = seasonStart;
//            if (Calendar.MONTH < 6)
//                this.cutOfDate = formatter.parse("06/01/" + (cal.get(Calendar.YEAR)-1));
//            else
//                this.cutOfDate = formatter.parse("06/01/" + cal.get(Calendar.YEAR));
        } catch (Exception e) {
            this.cutOfDate = null;
        }
    }

    public String recommendationFlu() {
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
        String recommendation = null;
        int numberOfFluShotsInPreviousSeasons = 0;
        int numberOfFluShotsInCurrentSeason = 0;

        // Do an age calculation first
        DateTime startDateTime = new DateTime(cds.getDateOfBirth());
        DateTime endDateTime = new DateTime(auditDate);
        int diffMonth = Months.monthsBetween(startDateTime, endDateTime).getMonths();
        System.out.println("\tDiff in months: " + diffMonth);

        // Sort the influenza dates first
        ArrayList influenzaListToProcess = new ArrayList();
        List<HistoryStatusDTO> history = cds.getHistory(); //cds.getDBHistory();

        Iterator<HistoryStatusDTO> it = history.iterator();
        while (it.hasNext()) {
            HistoryStatusDTO item = it.next();
            System.out.println("----> Series: " + item);
            if (item.getSeries().equalsIgnoreCase("Influenza Vaccine Group"))
                influenzaListToProcess.add(item.getShotDate());
        }
        Collections.sort(influenzaListToProcess);

        // Remove any invalid shots
        ArrayList influenzaListProcessed = new ArrayList();
        Date previousShotDate = null;
        Iterator<Date> iter = influenzaListToProcess.iterator();
        System.out.println("\tCut Of Date: " + cutOfDate);
        while (iter.hasNext()) {
            Date currentShotDate = iter.next();
            System.out.println("\tCurrent Shot Date: " + currentShotDate);
            if (previousShotDate == null) {
                previousShotDate = currentShotDate;
                influenzaListProcessed.add(currentShotDate);
                if (currentShotDate.compareTo(cutOfDate) < 0) {
                    numberOfFluShotsInPreviousSeasons++;
                }
                if (currentShotDate.compareTo(cutOfDate) > 0) {
                    numberOfFluShotsInCurrentSeason++;
                }
            } else {
                long diff = currentShotDate.getTime() - previousShotDate.getTime();
                float days = (diff / (1000 * 60 * 60 * 24));
                if (days >= 24.0) { // Only add the shot if it is 24 days after the previous shot
                    previousShotDate = currentShotDate;
                    influenzaListProcessed.add(currentShotDate);
                    if (currentShotDate.compareTo(cutOfDate) < 0) {
                        numberOfFluShotsInPreviousSeasons++;
                    }
                    if (currentShotDate.compareTo(cutOfDate) > 0) {
                        numberOfFluShotsInCurrentSeason++;
                    }
                } else {
                    System.out.println("\tShot not counted " + currentShotDate + " - PS: " + previousShotDate + " diff dates: " + days);
                }
            }
        }

        System.out.println("\tAge in months: " + diffMonth);
        System.out.println("\t--> Number of Shots before flu season: " + numberOfFluShotsInPreviousSeasons);
        System.out.println("\t--> Number of Shots in current flu season: " + numberOfFluShotsInCurrentSeason);


        // Case 1: Under 6 months, not eligible
        if (diffMonth <= 6) {
            recommendation = "NA";
            //return "NOT_RECOMMENDED - BELOW_REC_AGE_SERIES";
        }

        // 6 months - < 9 years
        // 2 or more shots before July 1 of audit year need 1 more shot
        // 0 or 1 shot before July 1 of audit year - need 2 shots
        if ((diffMonth >= 6) && (diffMonth < 108)) {
            if (numberOfFluShotsInPreviousSeasons >= 2) {
                System.out.println("\tnumberOfFluShotsInPreviousSeasons >= 2");
                if (numberOfFluShotsInCurrentSeason >= 1)
                    recommendation = "Y"; //COMPLETE - VALID";
                else
                    recommendation = "D"; //INCOMPLETE - DUE_NOW";
                System.out.println("\tSetting recommendation to " + recommendation);
            }
            if (numberOfFluShotsInPreviousSeasons < 2) {
                if (numberOfFluShotsInCurrentSeason >= 2) {
                    recommendation = "Y"; //COMPLETE - VALID";
                } else {
                    recommendation = "D"; //INCOMPLETE - DUE_NOW";
                }
            }

        }

        // >= 9 years
        // 0 previous vaccines - need 1
        // 1 or more vaccines - need 0 
        if (diffMonth >= 108) {
            if (numberOfFluShotsInCurrentSeason >= 1) {
                recommendation = "Y";
            } else if ((numberOfFluShotsInPreviousSeasons >= 1) && (numberOfFluShotsInCurrentSeason >= 1)) {
                recommendation = "Y"; //COMPLETE - VALID";
            } else {
                recommendation = "D"; //INCOMPLETE - DUE_NOW";
            }
        }

        if (recommendation.equalsIgnoreCase("D")) {
            // Change it to NW if the difference between the audit date and the last shot date 
            // is less than 24 days
            if (previousShotDate != null) {
                long diff = auditDate.getTime() - previousShotDate.getTime();
                float days = (diff / (1000 * 60 * 60 * 24));
                if (days < 24) {
                    recommendation = "NW";
                    DateTime dt = new DateTime(previousShotDate);
                    dt = dt.plusDays(28);
                    this.futureRecommendedDate = dt.toDate();
                }
            }
        }
        if (recommendation.equalsIgnoreCase("NA")) {
            DateTime dt = new DateTime(cds.getDateOfBirth());
            dt = dt.plusMonths(6);
            this.futureRecommendedDate = dt.toDate();
        }

        // Remap to ICE statuses
        if (recommendation.equalsIgnoreCase("Y"))
            recommendation = "NOT_RECOMMENDED - COMPLETE";
        else if (recommendation.equalsIgnoreCase("D"))
            recommendation = "RECOMMENDED - DUE_NOW";
        else if (recommendation.equalsIgnoreCase("NW")) {
            recommendation = "FUTURE_RECOMMENDED - DUE_IN_FUTURE";
        } else
            recommendation = "FUTURE_RECOMMENDED - DUE_IN_FUTURE";

        return recommendation;
    }

    public CDSLibraryWrapper getCds() {
        return cds;
    }

    public void setCds(CDSLibraryWrapper cds) {
        this.cds = cds;
    }

    public Date getAuditDate() {
        return auditDate;
    }

    public void setAuditDate(Date auditDate) {
        this.auditDate = auditDate;
    }

    public Date getSeasonStart() {
        return seasonStart;
    }

    public void setSeasonStart(Date seasonStart) {
        this.seasonStart = seasonStart;
    }

    public Date getSeasonEnd() {
        return seasonEnd;
    }

    public void setSeasonEnd(Date seasonEnd) {
        this.seasonEnd = seasonEnd;
    }

    public Date getCutOfDate() {
        return cutOfDate;
    }

    public void setCutOfDate(Date cutOfDate) {
        this.cutOfDate = cutOfDate;
    }

    public Date getFutureRecommendedDate() {
        return futureRecommendedDate;
    }
}
