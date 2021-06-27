package org.nyp.ezvac.imsrvcs.tests;


import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.nyp.ezvac.imsrvcs.rules.NYPFluRules;
import org.nyph.cdslibrary.CDSLibraryWrapper;
import org.nyph.cdslibrary.dto.HistoryStatusDTO;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 * @author bdasgupt
 */
public class TestRules {
    private static final String FLU_TEST_FILE = "/datafiles/tests/flu-2017.csv";

    public TestRules() {
    }

    public int processFluTestFile() throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yy");
        Date seasonStart = formatter.parse("08/01/17");
        Date seasonEnd = formatter.parse("07/31/18");
        int numberOfFailedTestCases = 0;

        try {
            Connection conn = null;
            CDSLibraryWrapper cds = new CDSLibraryWrapper(conn);
            BufferedReader br = new BufferedReader(new FileReader(FLU_TEST_FILE));
            String line = br.readLine(); // Ignore the first line since this is the CSV header
            if (line != null)
                line = br.readLine();
            while (line != null) {
                List<HistoryStatusDTO> history = new ArrayList();
                String tokens[] = line.split(",");
                String desc = tokens[0];
                String ruleNo = tokens[1];
                String expectedResult = tokens[tokens.length - 1];
                cds.setGender(tokens[2]);
                cds.setDateOfBirth(formatter.parse(tokens[3]));
                cds.setEvaluationDate(formatter.parse(tokens[4]));
                //System.out.println("Tokens Length = " + tokens.length);
                for (int i = 5; i < (tokens.length - 2); i += 2) {
                    String shotType = tokens[i];
                    String shotDate = tokens[i + 1];
                    if (!((shotType.length() == 0) || (shotDate.length() == 0))) {
                        if (shotType.equalsIgnoreCase("influenza")) {
                            HistoryStatusDTO h = new HistoryStatusDTO();
                            h.setCvxCode(new Integer(88));
                            h.setSeries("Influenza");
                            h.setSeries("Influenza");
                            h.setShotDate(formatter.parse(shotDate));
                            history.add(h);
                            System.out.println("\t" + shotType + " - " + shotDate);
                        }
                    }
                }
                cds.setHistory(history);
                cds.setDbHistory(history);

                System.out.println(line);
                NYPFluRules fluRules = new NYPFluRules(cds, cds.getEvaluationDate(), seasonStart, seasonEnd);
                System.out.println("\tSTATUS: " + fluRules.recommendationFlu() + " Expected: " + expectedResult);
                if (fluRules.recommendationFlu().equalsIgnoreCase("FUTURE_RECOMMENDED - DUE_IN_FUTURE")) {
                    System.out.println("\t\tRecommende Shot Date: " + fluRules.getFutureRecommendedDate());
                }
                line = br.readLine();
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return numberOfFailedTestCases;
    }

    public static void main(String args[]) {
        TestRules tr = new TestRules();

        System.out.println("Testing Rules Engine");
        try {
            tr.processFluTestFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
