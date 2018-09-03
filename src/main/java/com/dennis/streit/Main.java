package com.dennis.streit;
import com.dennis.streit.DatabaseCreator;
import com.dennis.streit.Resolver;
import com.bordercloud.sparql.*;

import com.mysql.cj.x.protobuf.MysqlxDatatypes;
import com.opencsv.CSVReader;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.json.*;

import java.io.*;
import java.sql.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;


public class Main
{


    public static void main(String[] args)
    {
        try {
            Connection con = getConnection();
            DatabaseCreator.createCountryList();
            DatabaseCreator.resultsFromLänder();
            DatabaseCreator.createTableSQL(con);
            DatabaseCreator.insertIntoSQL(con);


            //final String WDEndpoint = "https://query.wikidata.org/sparql";
            // countries = new CountryList();
            //countries.generateList();
            //createCountryList(WDEndpoint);
            // resultsFromLänder(WDEndpoint);
            // createTableSQL();
            //insertIntoSQL();
            //Resolver.parseCSV(con);


            //searchCityInSQL("fjfgdsogb");
            //findCountryForCity();


        /*} catch (EndpointException eex) {
            System.out.println(eex);
            eex.printStackTrace();
        } catch (java.io.FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (java.io.IOException ex) {
            ex.printStackTrace(); */
        } catch (java.lang.Exception ex) {
            ex.printStackTrace();
        }


    }

  /*  public static void createCountryList(String ep) throws EndpointException
    {
        String countries =
                "SELECT DISTINCT ?country ?countryLabel WHERE {\n" +
                        "  ?city wdt:P31/wdt:P279* wd:Q515.    \n" +
                        "  ?city wdt:P1082 ?population .\n" +
                        "  ?city wdt:P17 ?country .      \n" +
                        "   FILTER(?population > 2000)\n" +
                        "   SERVICE wikibase:label {\n" +
                        "     bd:serviceParam wikibase:language \"en\" .\n" +
                        "   }\n" +
                        "}";
        JSONArray länderArray = new JSONArray();
        ResultsToJson(getQueryResults(ep, countries), länderArray, "Länder");
    }

    //schreibt Ergebnis der Query in HashMap Objekt
    public static HashMap<String, HashMap> getQueryResults(String WDendpoint, String WDquery) throws EndpointException
    {
        Endpoint endpoint = new Endpoint(WDendpoint, false);
        endpoint.setMethodHTTPRead("GET");
        HashMap<String, HashMap> queryResults = endpoint.query(WDquery);
        return queryResults;
    }

    // printed Inhalt des Query-Ergebnis HashMap Objects
    public static void printResults(HashMap<String, HashMap> queryResults, int size)
    {
        for (HashMap<String, Object> value : (ArrayList<HashMap<String, Object>>) queryResults.get("result").get("rows")) {
            for (String variable : (ArrayList<String>) queryResults.get("result").get("variables")) {
                System.out.print(String.format("%-" + size + "." + size + "s", value.get(variable)) + " | ");
            }
            System.out.print("\n");
        }

    }


    public static JSONArray ResultsToJson(HashMap<String, HashMap> queryResults, JSONArray jArray, String filename)
    {

        for (HashMap<String, Object> value : (ArrayList<HashMap<String, Object>>) queryResults.get("result").get("rows")) {
            JSONObject jObj = new JSONObject();
            for (String variable : (ArrayList<String>) queryResults.get("result").get("variables")) {
                jObj.put(variable, value.get(variable));
            }
            jArray.put(jObj);
        }
        System.out.println(jArray.toString());
        try {
            FileWriter file = new FileWriter("files/" + filename + ".json");
            file.write(jArray.toString(4));
            file.flush();


        } catch (IOException e) {
            e.printStackTrace();
        }
        return jArray;
    }

    public static void resultsFromLänder(String ep) throws EndpointException, IOException
    {
        //Array mit ALLEN Daten, Endergebnis
        JSONArray AllResultsArray = new JSONArray();
        FileReader länder = new FileReader("files/Länder.json");
        JSONTokener tokener = new JSONTokener(länder);
        //Array mit allen Ländern + deren entity Nummern
        JSONArray labelandentity = new JSONArray(tokener);
        String countryNumber;
        String countryLabel;
        for (int i = 0; i < labelandentity.length(); i++) {
            //Daten zu Land im aktuellen Schleifendurchlauf
            JSONArray currentCountry = new JSONArray();
            JSONObject landObj = labelandentity.getJSONObject(i);
            countryNumber = landObj.getString("country").replaceAll("\\D+", "");
            String queryCities =
                    "SELECT DISTINCT ?city ?native ?cityLabel ?country ?countryLabel  ?population WHERE {\n" +
                            "  ?city wdt:P31/wdt:P279* ?settlement\n" +
                            "  FILTER(?settlement = wd:Q515 || ?settlement = wd:Q15284)    \n" +
                            "  ?city wdt:P1082 ?population .\n" +
                            "  ?city wdt:P17 ?country . FILTER (?country =wd:Q" + countryNumber + ")\n" +
                            "  OPTIONAL {\n" +
                            "    ?city wdt:P1705 ?native .\n" +
                            "}\n" +
                            "      \n" +
                            "   FILTER(?population > 2000)\n" +
                            "   SERVICE wikibase:label {\n" +
                            "     bd:serviceParam wikibase:language \"en\" .\n" +
                            "   }\n" +
                            "}";
            countryLabel = landObj.getString("countryLabel");
            //currentCountry = ResultsToJson(getQueryResults(ep, queryCities), currentCountry, countryLabel);
            AllResultsArray = ResultsToJson(getQueryResults(ep, queryCities), AllResultsArray, "Allcountries");
        }

    } */

    /*

    public static void createTableSQL()
    {
        try {
            Connection con = getConnection();
            PreparedStatement create = con.prepareStatement("CREATE TABLE IF NOT EXISTS cities(cityEntity VARCHAR(100) NOT NULL , cityLabel VARCHAR(100) , nativeLabel VARCHAR (100), countryEntity INT , countryLabel VARCHAR(100) , population INT)");
            create.executeUpdate();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void insertIntoSQL()
    {
        try {
            Connection con = getConnection();
            FileReader fr = new FileReader("files/Allcountries.json");
            JSONTokener tokener = new JSONTokener(fr);
            JSONArray jArray = new JSONArray(tokener);
            int cityEntity;
            String cityLabel;
            String nativeLabel;
            int countryEntity;
            String countryLabel;
            int population;

            for (int i = 0; i < jArray.length(); i++) {
                JSONObject landObj = jArray.getJSONObject(i);
                cityEntity = Integer.valueOf(landObj.getString("city").replaceAll("\\D+", ""));
                cityLabel = landObj.getString("cityLabel");
                nativeLabel = landObj.optString("native");
                countryEntity = Integer.valueOf(landObj.getString("country").replaceAll("\\D+", ""));
                countryLabel = landObj.getString("countryLabel");
                population = Integer.valueOf(landObj.getString("population").replaceAll("\\D+", ""));
                PreparedStatement insert = con.prepareStatement("INSERT INTO cities VALUES (?, ?, ?, ? ,?, ?);");
                insert.setInt(1, cityEntity);
                insert.setString(2, cityLabel);
                insert.setString(3, nativeLabel);
                insert.setInt(4, countryEntity);
                insert.setString(5, countryLabel);
                insert.setInt(6, population);
                insert.executeUpdate();
                System.out.println(cityLabel);
            }

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    */

    //returned Array mit den Namen der Konferenzen
 /*   public static void parseCSV()
    {
        try {
            Connection con = getConnection();
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
    }//sucht nach Land in SQL DB. Wenn gefunden, returned String[2] mit [0]= Ländername und [1]= Länderidentifier

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
    }*/
    public static Connection getConnection() throws Exception
    {
        try {
            String driver = "com.mysql.cj.jdbc.Driver";
            String url = "jdbc:mysql://127.0.0.1:3306/locations?useSSL=false&useLegacyDatetimeCode=false&serverTimezone=Europe/Berlin&allowPublicKeyRetrieval=true";
            String username = "user";
            String password = "user";
            Class.forName(driver);
            Connection con = DriverManager.getConnection(url, username, password);
            //System.out.println("Connected");
            return con;


        } catch (Exception e) {
            System.out.println(e);
        }
        return null;

    }


}