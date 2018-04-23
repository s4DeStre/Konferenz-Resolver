package com.dennis.streit;

import java.util.ArrayList;
import java.util.HashMap;

public class Main {
    public static void main(String[] args) {
        try {
            String querySelect =
                    "SELECT ?item ?itemLabel \n" +
                    "WHERE \n" +
                    "{\n" +
                    "  ?item wdt:P31 wd:Q146.\n" +
                    "  SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],en\". }\n" +
                    "}";


            String endpoint = "https://query.wikidata.org/sparql";
            System.out.println("");
            System.out.println("Endpoint : " + endpoint);
            System.out.println("");
            System.out.println("Result : ");

            Endpoint sp2 = new Endpoint(endpoint, false);
            HashMap<String, HashMap> rs2 = sp2.query(querySelect);
            printResult(rs2, 30);

        } catch (EndpointException eex) {
            System.out.println(eex);
            eex.printStackTrace();
        }
    }

    public static void printResult(HashMap<String, HashMap> rs, int size) {

        for (String variable : (ArrayList<String>) rs.get("result").get("variables")) {
            System.out.print(String.format("%-" + size + "." + size + "s", variable) + " | ");
        }
        System.out.print("\n");
        for (HashMap<String, Object> value : (ArrayList<HashMap<String, Object>>) rs.get("result").get("rows")) {
            //System.out.print(value);
            /* for (String key : value.keySet()) {
         System.out.println(value.get(key));
         }*/
            for (String variable : (ArrayList<String>) rs.get("result").get("variables")) {
                //System.out.println(value.get(variable));
                System.out.print(String.format("%-" + size + "." + size + "s", value.get(variable)) + " | ");
            }
            System.out.print("\n");
        }
    }

}