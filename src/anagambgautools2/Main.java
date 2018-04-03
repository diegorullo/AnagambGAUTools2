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
        Statement statement = null;
        ArrayList<String> campiCluster = new ArrayList<String>();
        Hashtable<String, Hashtable<String, String>> recordMap = new Hashtable<String, Hashtable<String, String>>();      
        if (connection != null) {
            System.out.println("Connessione OK!");
            System.out.println("---------------------------------");
            System.out.println("1. Elenco campi tabella sorgente");
            Cluster.stampaCampiSorgente(connection, verbose);
            //Cluster.stampaCampiCluster(connection, verbose);
            System.out.println("---------------------------------");
            System.out.println("2. Acquisizione campi-valori in mappa");
            recordMap = Cluster.popolaMappaRecord(connection, verbose);
            //campiCluster = createCampiCluster(connection, verbose);
            System.out.println("---------------------------------");
            System.out.println("3. Ricerca similarità");
            Cluster.findSimilarity(recordMap);
              
        }
        else {
            System.out.println("Connessione fallita!");
        }
    }
}
