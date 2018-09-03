package com.dennis.streit;

import com.bordercloud.sparql.Endpoint;
import com.bordercloud.sparql.EndpointException;
import org.json.*;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;

public class DatabaseCreator
{
    private static final String WDendpoint = "https://query.wikidata.org/sparql";

    public static void createCountryList() throws EndpointException
    {
        String countries =
                "SELECT DISTINCT ?country ?countryLabel WHERE {\n" +
                        "  ?city wdt:P31/wdt:P279* wd:Q515.    \n" +
                        "  ?city wdt:P1082 ?population .\n" +
                        "  ?city wdt:P17 ?country .      \n" +
                        "   FILTER(?population > 2000)\n" +
                        "FILTER(NOT EXISTS{?country wdt:P31 wd:Q3024240})\n"+
                        "   SERVICE wikibase:label {\n" +
                        "     bd:serviceParam wikibase:language \"en\" .\n" +
                        "   }\n" +
                        "}";
        JSONArray länderArray = new JSONArray();
        ResultsToJson(getQueryResults(countries), länderArray, "Länder");
        System.out.println("Countrylist erstellt");
    }

    private static HashMap<String, HashMap> getQueryResults( String WDquery) throws EndpointException
    {
        Endpoint endpoint = new Endpoint(WDendpoint, false);
        endpoint.setMethodHTTPRead("GET");
        HashMap<String, HashMap> queryResults = endpoint.query(WDquery);
        return queryResults;
    }

    public static void printResults(HashMap<String, HashMap> queryResults, int size)
    {
        for (HashMap<String, Object> value : (ArrayList<HashMap<String, Object>>) queryResults.get("result").get("rows")) {
            for (String variable : (ArrayList<String>) queryResults.get("result").get("variables")) {
                System.out.print(String.format("%-" + size + "." + size + "s", value.get(variable)) + " | ");
            }
            System.out.print("\n");
        }

    }

    private static JSONArray ResultsToJson(HashMap<String, HashMap> queryResults, JSONArray jArray, String filename)
    {

        for (HashMap<String, Object> value : (ArrayList<HashMap<String, Object>>) queryResults.get("result").get("rows")) {
            JSONObject jObj = new JSONObject();
            for (String variable : (ArrayList<String>) queryResults.get("result").get("variables")) {
                jObj.put(variable, value.get(variable));
            }
            jArray.put(jObj);
        }
        //Debug output
        //System.out.println(jArray.toString());
        try {
            FileWriter file = new FileWriter("files/" + filename + ".json");
            file.write(jArray.toString(4));
            file.flush();


        } catch (IOException e) {
            e.printStackTrace();
        }
        return jArray;
    }

    public static void resultsFromLänder() throws EndpointException, IOException
    {
        //Array mit ALLEN Daten, Endergebnis
        JSONArray AllResultsArray = new JSONArray();
        FileReader länder = new FileReader("files/Länder.json");
        JSONTokener tokener = new JSONTokener(länder);
        //Array mit allen Ländern + deren entity Nummern
        JSONArray labelandentity = new JSONArray(tokener);
        String countryNumber;
        //String countryLabel;
        for (int i = 0; i < labelandentity.length(); i++) {
            //Daten zu Land im aktuellen Schleifendurchlauf
            //JSONArray currentCountry = new JSONArray();
            JSONObject landObj = labelandentity.getJSONObject(i);
            countryNumber = landObj.getString("country").replaceAll("\\D+", "");
            String queryCities =
                    "SELECT DISTINCT ?city ?native ?cityLabel ?country ?countryLabel ?population WHERE {\n" +
                            "  ?city wdt:P31/wdt:P279* ?settlement\n" +
                            "  FILTER(?settlement = wd:Q515 || ?settlement = wd:Q15284)    \n" +
                            "  ?city wdt:P1082 ?population .\n" +
                            "  ?city wdt:P17 ?country . FILTER (?country =wd:Q"+countryNumber+")\n" +
                            "  OPTIONAL{?city wdt:P1705 ?native }.      \n" +
                            "   FILTER(?population > 2000)\n" +
                            "   \n" +
                            "   SERVICE wikibase:label {\n" +
                            "		bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],en\" .\n" +
                            "	}   \n" +
                            "}";
            //countryLabel = landObj.getString("countryLabel");
            //currentCountry = ResultsToJson(getQueryResults(ep, queryCities), currentCountry, countryLabel);
            AllResultsArray = ResultsToJson(getQueryResults(queryCities), AllResultsArray, "Allcountries");

        }
        System.out.println("Länderdaten erstellt");

    }

    public static void createTableSQL(Connection con)
    {
        try {
            PreparedStatement create = con.prepareStatement("CREATE TABLE IF NOT EXISTS cities(cityEntity VARCHAR(100) NOT NULL , cityLabel VARCHAR(100) , nativeLabel VARCHAR (100), countryEntity INT , countryLabel VARCHAR(100) , population INT)");
            create.executeUpdate();
        } catch (Exception e) {
            System.out.println(e);
        }
        System.out.println("SQL-Table erstellt");
    }

    public static void insertIntoSQL(Connection con)
    {
        try {
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
            }

        } catch (Exception e) {
            System.out.println(e);
        }
        System.out.print("Daten in Datenbank inserted");
    }


}
