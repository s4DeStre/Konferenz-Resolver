package com.dennis.streit;

import com.opencsv.CSVReader;

import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class Resolver
{
    public static void parseCSV(Connection con)
    {
        try {
            String output = "";
            CSVReader reader = new CSVReader(new FileReader("files/evaluation.csv"));
            List<String[]> proceedings = reader.readAll();
            //durch csv Dokument laufen und index+key entfernen
            for (int i = 0; i < proceedings.size(); i++) {
                String[] tokens = proceedings.get(i)[1].split(", ");
                //Konferenztitel in Token aufteilen
                //System.out.print("["+i+1+"] ");
                output += "[" + (i + 1) + "] ";
                String[] prepTitle = tokens;
                boolean foundCountry = false;
                boolean foundPair = false;
                boolean foundCity = false;
                for (int j = 0; j < tokens.length; j++) {
                    String[] currentCountry = searchForCountryinSQL(con, tokens[j]);
                    if (currentCountry != null) {
                        foundCountry = true;
                        //noch einmal Namen durchlaufen um Stadt zu finden
                        for (int k = 0; k < tokens.length; k++) {
                            //j ist aktuelles Land und kann nicht Stadt sein
                            if (k != j) {
                                String[] pair = searchForCityWithCountry(con, tokens[j], tokens[k]);
                                if (pair != null) {
                                    foundPair = true;
                                    for (int m = 0; m < prepTitle.length; m++) {
                                        if (m == j) prepTitle[m] += " |Q" + pair[1] + "| ";
                                        if (m == k) prepTitle[m] += " |Q" + pair[3] + "| ";
                                    }

                                }
                            }
                        }
                        //Fall: nur Land gefunden und keine Stadt
                        if (!foundPair) {
                            for (int m = 0; m < prepTitle.length; m++) {
                                if (m == j) prepTitle[m] += (" |Q" + currentCountry[1] + "| ");
                            }
                        }
                    }
                }
                //falls: kein Land im String gefunden wurde
                if (!foundCountry) {
                    if (!foundPair) {
                        for (int j = 0; j < tokens.length; j++) {
                            String[] currentCity = searchJustCityInSQL(con, tokens[j]);
                            if (currentCity != null) {
                                foundCity = true;
                                for (int m = 0; m < tokens.length; m++) {
                                    if (m == j)
                                        prepTitle[m] += " |Q" + currentCity[3] + "|(" + currentCity[0] + "|Q" + currentCity[1] + "|) ";
                                }
                            }
                        }
                    }
                    //falls: kein Land und keine Stadt gefunden
                    if (!foundCity) {
                        for (int m = 0; m < tokens.length; m++) {
                        }
                    }

                }
                //System.out.print("\n");
                output += Arrays.toString(prepTitle);
                output += "\n";
            }
            FileWriter file = new FileWriter("files/evaluationAlgorithmus.csv");
            file.write(output);
            file.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String[] searchForCountryinSQL(Connection con, String länderToken)
    {
        try {
            PreparedStatement ps = con.prepareStatement("SELECT DISTINCT countryEntity, countryLabel FROM cities WHERE countryLabel = ? ");
            ps.setString(1, checkCountrySynonyms(länderToken));
            ResultSet result = ps.executeQuery();
            if (result.next()) {
                String[] data = new String[]{result.getString("countryLabel"), result.getString("countryEntity")};
                return data;
            } else return null;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

    }

    public static String[] searchForCityWithCountry(Connection con, String länderToken, String städteToken)
    {
        try {
            PreparedStatement ps = con.prepareStatement("SELECT  countryLabel, countryEntity, cityLabel, cityEntity  FROM cities WHERE countryLabel= ? and (cityLabel = ? or nativeLabel=?) ORDER BY population DESC;");
            ps.setString(1, checkCountrySynonyms(länderToken));
            ps.setString(2, städteToken);
            ps.setString(3, städteToken);
            ResultSet result = ps.executeQuery();
            if (result.next()) {
                String[] data = new String[]{result.getString("countryLabel"), result.getString("countryEntity"), result.getString("cityLabel"), result.getString("cityEntity")};
                return data;
            } else return null;
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    public static String[] searchJustCityInSQL(Connection con, String cityToken)
    {
        try {
            PreparedStatement ps = con.prepareStatement("SELECT  countryLabel, countryEntity, cityLabel, cityEntity  FROM cities WHERE cityLabel = ? or nativeLabel=?;");
            ps.setString(1, cityToken);
            ps.setString(2, cityToken);
            ResultSet result = ps.executeQuery();
            if (result.next()) {
                String[] data = new String[]{result.getString("countryLabel"), result.getString("countryEntity"), result.getString("cityLabel"), result.getString("cityEntity")};
                return data;
            } else return null;
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    public static String checkCountrySynonyms(String country)
    {
        switch (country) {
            case "USA":
                country = "United States of America";
                break;
            case "UK":
                country = "United Kingdom";
                break;
            case "China":
                country = "People's Republic of China";
                break;
            case "The Netherlands":
                country = "Netherlands";
                break;

            case "Korea":
                country = "South Korea";
                break;
            case "Republic of Korea":
                country = "South Korea";
            case "Korea (South)":
                country = "South Korea";
        }
        return country;
    }
}
