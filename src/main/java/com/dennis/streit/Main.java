package com.dennis.streit;

import com.bordercloud.sparql.*;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.json.*;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


public class Main
{


    public static void main(String[] args){
        try {
        String WDEndpoint = "https://query.wikidata.org/sparql";
        String queryCities =
                "SELECT DISTINCT ?cityLabel ?population ?countryLabel ?cityDescription WHERE {\n" +
                        "	?city wdt:P31/wdt:P279* ?settlement\n" +
                        "    FILTER(?settlement = wd:Q515 || ?settlement = wd:Q15284)      \n" +
                        "	?city wdt:P1082 ?population .\n" +
                        "	?city wdt:P17 ?country .\n" +
                        "\n" +
                        "  FILTER(?population > 2000) \n" +
                        "  SERVICE wikibase:label {\n" +
                        "		bd:serviceParam wikibase:language \"en\" .\n" +
                        "  }\n" +
                        "} \n" +
                        "LIMIT 1000";
        String queryCitiesNative =
                "SELECT DISTINCT ?city ?native ?cityLabel ?country ?countryLabel  ?population WHERE {\n" +
                        "  ?city wdt:P31/wdt:P279* ?settlement\n" +
                        "  FILTER(?settlement = wd:Q515 || ?settlement = wd:Q15284)    \n" +
                        "  ?city wdt:P1082 ?population .\n" +
                        "  ?city wdt:P17 ?country . FILTER (?country =wd:Q30)\n" +
                        "  OPTIONAL {\n" +
                        "    ?city wdt:P1705 ?native .\n" +
                        "}\n" +
                        "      \n" +
                        "   FILTER(?population > 2000)\n" +
                        "   SERVICE wikibase:label {\n" +
                        "     bd:serviceParam wikibase:language \"en\" .\n" +
                        "   }\n" +
                        "}";


            String queryLanguages ="";



            ResultsToJson(getQueryResults(WDEndpoint, queryCitiesNative));
            //printResults(getQueryResults(WDEndpoint, queryCities), 40);


        } catch (EndpointException eex) {
            System.out.println(eex);
            eex.printStackTrace();
        }


    }

    public static HashMap<String, HashMap>  getQueryResults (String WDendpoint, String WDquery) throws EndpointException{
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


    public static void ResultsToJson(HashMap<String, HashMap> queryResults)
    {

        JSONArray jArray = new JSONArray();

        for (HashMap<String, Object> value : (ArrayList<HashMap<String, Object>>) queryResults.get("result").get("rows")) {
            JSONObject jObj = new JSONObject();
            for (String variable : (ArrayList<String>) queryResults.get("result").get("variables")) {
                jObj.put(variable, value.get(variable));
            }
            jArray.put(jObj);
        }

        System.out.println(jArray.toString());
        try {
            FileWriter file = new FileWriter("files/resultsNative.json");
            file.write(jArray.toString(4));
            file.flush();


        }
        catch(IOException e){
            e.printStackTrace();
        }
    }
}