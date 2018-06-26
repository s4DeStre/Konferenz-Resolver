package com.dennis.streit;
import java.util.Locale;

public class CountryList
{
    public CountryList(){

    }
    public static void generateList(){
    String[] locales = Locale.getISOCountries();

    for ( String countryCode :locales) {
        Locale current = new Locale("", countryCode);
        System.out.println("Countrycode = "+current.getCountry()+ ", Name = "+current.getDisplayCountry());

    }
}
}
