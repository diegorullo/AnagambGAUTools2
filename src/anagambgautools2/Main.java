/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package anagambgautools2;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import java.io.*;
import java.util.*;

import model.DBEngine;
import control.Cluster;



// dblp.Corpus;
//import dblp.Factory;
public class Main {

//	private static Corpus dblp;
//	public static Corpus getDblp() {
//		return dblp;
//	}
    public static void main(String[] argv) throws SQLException, ClassNotFoundException {
   
        DBEngine dbe;
        Connection connection;
        connection = DBEngine.getOracleConnection();
        Boolean verbose = true;
        Boolean ramLoad = false;

        Hashtable<String, Hashtable<String, String>> recordMap = new Hashtable<String, Hashtable<String, String>>();
        /* questa variabile controlla il caricamento della fonte dati:
                    se TRUE , carica tutta la tabella in memoria centrale
                    se FALSE , accede ogni volta ai dati del supporto in memoria secondaria
        */
        
        if (connection != null) {
            System.out.println("Connessione OK!");
            System.out.println("---------------------------------");
            System.out.println("1. Elenco campi tabella sorgente");
            Cluster.printSourceFields(connection, verbose);
            //Cluster.stampaCampiCluster(connection, verbose);
            System.out.println("---------------------------------");
            /* questa parte di codice richiama un metodo per l'acquisizione di tutti i dati in memoria centrale */
            System.out.println("2. Acquisizione campi-valori in mappa");
            if (ramLoad) recordMap = Cluster.populateRecordMap(connection, verbose);
            else                     Cluster.populateRowidsHashtable(connection, verbose);
                  
            //campiCluster = createCampiCluster(connection, verbose);
            System.out.println("---------------------------------");
            System.out.println("3. Ricerca similarità");
            if (ramLoad) Cluster.findSimilarity(recordMap, verbose);
            else         Cluster.findSimilarity(connection);
            
              
        }
        else {
            System.out.println("Connessione fallita!");
        }
    }
}
