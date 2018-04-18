/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package control;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import model.DBEngine;

/**
 *
 * @author Administrator
 */
public class Cluster {
    private static Statement statement;
    private static Hashtable< Integer , String> rowidMap;
    private static Hashtable< String, Integer> rowidMapReverse;
    private static Hashtable<String, Hashtable<String, String>> recordMap;
    private static TreeMap<String, Integer> tmapClusterResults;

    private static final int targetDist = 2; // distanza cluster
    private static final Boolean ramLoad = true;
    private static final Boolean verbose = false;
    private static final String numrow = "10";
    private static final String method = "OPT"; 
                                // ottimizzazione: 
                                //      BASE      = nessuna 
                                //      OPT       = semi ottimizzato 
                                //      OPTLOCAL  = semi ottimizzato con ricerca locale del bound 
                                //      OPTRET    = ottimizzazione con ritardo del calcolo distanza
    private static final String orderTypeSQL = " ";// ORDER BY DENOMINAZIONE";         
    //private static Hashtable<String, Hashtable<String, String>> recordMap;
    private static final String selectTableSQLBase = "select " +
                        "ROWID\n" +
                        ",ID_SOGGETTO_AMBIENTALE\n" +
                        ",COD_FISCALE\n" +
                        ",DENOMINAZIONE\n" +
                        ",COD_SIRA_SOGGETTO\n" +
                        ",DATA_AGGIORNAMENTO_SOGGETTO\n" +
                        ",DATA_INIZIO_VAL_SOGGETTO\n" +
                        ",DATA_FINE_VAL_SOGGETTO\n" +
                        ",PARTITA_IVA\n" +
                        ",RAGIONE_SOCIALE\n" +
                        ",DATA_COSTITUZIONE\n" +
                        ",DATA_CESSAZIONE\n" +
                        ",DATA_INIZIO_VAL_SOGG_COMP\n" +
                        ",DATA_FINE_VAL_SOGG_COMP\n" +
                        ",ID_OGGETTO_AMBIENTALE\n" +
                        ",ID_STATO_INFO\n" +
                        ",FONTE\n" +
                        ",COD_SIRA_OGGETTO\n" +
                        ",ID_TIPO_GEOREF\n" +
                        ",COORDX\n" +
                        ",COORDY\n" +
                        ",DATA_AGGIORNAMENTO_OGGETTO\n" +
                        ",DATA_INIZIO_VAL_INDIRIZZO\n" +
                        ",DATA_FINE_VAL_INDIRIZZO\n" +
                        ",ISTAT_COMUNE_SEDE\n" +
                        ",COMUNE_SEDE\n" +
                        ",LOCALITA\n" +
                        ",INDIRIZZO\n" +
                        ",CIVICO\n" +
                        ",COD_ISTAT_PROVINCIA\n" +
                        ",PROVINCIA\n" +
                        ",DATA_INIZIO_VAL_REC\n" +
                        ",DATA_FINE_VAL_REC\n" +
                        ",REASON_CHANGE from ANAGAMB_V_GAU_ARPA ";

    private static final String selectTableSQL =  selectTableSQLBase+
                        "where rownum <= " + numrow +" " + orderTypeSQL;   
                        /*"where rownum <= " + numrow +" " +  
                        " and (rowid like 'AAASAOAAIAAAAE7AAH' or " +
                        "rowid like 'AAASAOAAIAAAAE7AAI' or " +
                        "rowid like 'AAASAOAAIAAAAE7AAJ' or " +
                        "rowid like 'AAASAOAAIAAAAE7AAK' or " +
                        "rowid like 'AAASAOAAIAAAAE7AAL' or " +
                        "rowid like 'AAASAOAAIAAAAE7AAM' or " +
                        "rowid like 'AAASAOAAIAAAAE8AAD' or " +
                        "rowid like 'AAASAOAAIAAAAE8AAE' or " +
                        "rowid like 'AAASAOAAIAAAAE8AAF' or " +
                        "rowid like 'AAASAOAAIAAAAE8AAG' ) " ;*/  
 
    public static void populateAuxiliaryStructures(Connection connection) throws SQLException {    
        if (ramLoad) Cluster.populateRecordMap(connection);
        else         Cluster.populateRowidsHashtable(connection);
    }
    
    /**
    *
    * @author diego
    * metodo di prova per la stampa di tutti i valori del campo ROWID della tabella
    */
    public static void printSourceFields(Connection connection, Boolean verbose) throws SQLException {        
        boolean verboseLocal = false;                     
        try {
            Statement statement = connection.createStatement();
            if(verbose&&verboseLocal) System.out.println("[printSourceFields]"+selectTableSQL);
            
            // esegue lo statement SQL 
            ResultSet rs = statement.executeQuery(selectTableSQL);
            
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            // L'indice della prima colonna è 1
            for (int i = 1; i <= columnCount; i++ ) {
                String name = rsmd.getColumnName(i);
                if(verbose&&verboseLocal) System.out.println("[printSourceFields]field n.: " + i + ", name: "+name);
            }
              
        } catch (SQLException e) {
            System.out.println(e.getMessage());                  
        } finally {      
            if (statement != null) {
                statement.close();
            }      
        }         
    }
    
    /*
    popola le seguenti strutture dati:
        recordMap       - mappa tutti i record della tabella in memoria centrale
        rowidMap        - mappa un id intero 1-n con un rowid 
        rowidMapReverse - mappa un rowid con un intero 1-n        
    */
    public static void populateRecordMap(Connection connection) throws SQLException {
        boolean verboseLocal = false;
        Enumeration names;
        Enumeration rowids;
        String key;  
        String rid = null;        
        recordMap = new Hashtable<String, Hashtable<String, String>>();      
        rowidMap = new Hashtable< Integer , String>();
        rowidMapReverse = new Hashtable< String, Integer>();            
        try {
            Statement statement = connection.createStatement();
            if(verbose&&verboseLocal) System.out.println(selectTableSQL);            
            // esegue lo statement SQL 
            ResultSet rs = statement.executeQuery(selectTableSQL);            
            // recupera i metadati
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            if(verbose&&verboseLocal) System.out.println("[populateRecordMap]Total column n. :"+columnCount);                 
            Integer counter=0;     
            while (rs.next()) {                
                Hashtable<String, String> hashtable = new Hashtable<String, String>();
                if (verbose){  
                    if(verbose&&verboseLocal) System.out.println("[populateRecordMap]---------------------------------");
                    if(verbose&&verboseLocal) System.out.println("[populateRecordMap] storing record in hashtable");
                } 
                for (int i = 2; i <= columnCount; i++ ) {
                    String name = rsmd.getColumnName(i);
                    if (verbose){                        
                        if(verbose&&verboseLocal) System.out.println("[populateRecordMap] storing field n. : " + i + ", name: "+name);
                    }                  
                    String value = rs.getString(name);
                    if (value!=null)
                        hashtable.put(name,value);
                    else 
                        hashtable.put(name,"NULL_VALUE");                                                                   
                }
                rid = rs.getString("ROWID");
                counter++;
                if(verbose&&verboseLocal) System.out.println("[populateRecordMap] counter: "+counter+" - rid: "+rid);
                rowidMap.put(counter, rid);
                rowidMapReverse.put(rid, counter);                
                names = hashtable.keys();
                if (verbose){  
                    if(verbose&&verboseLocal) System.out.println("[populateRecordMap]---------------------------------");
                    if(verbose&&verboseLocal) System.out.println("[populateRecordMap] printing record stored in hashtable");
                } 
                while(names.hasMoreElements()) {
                    key = (String) names.nextElement();
                    if (verbose){  
                        if(verbose&&verboseLocal) System.out.println("[populateRecordMap]key: " +key+ "  value: " + hashtable.get(key));
                    }                    
                }                                
                if (verbose){                        
                    if(verbose&&verboseLocal) System.out.println("[populateRecordMap]atoring record in hashtableMatrix "+rid);
                }                  
                if (rid!=null) {
                    recordMap.put(rid,hashtable);
                }
                else {
                    if(verbose&&verboseLocal) System.out.println("[populateRecordMap] Error null rowid !");
                }                                                                          
            }
            rowids = recordMap.keys();
            if (verbose){  
                if(verbose&&verboseLocal) System.out.println("[populateRecordMap]---------------------------------");
                if(verbose&&verboseLocal) System.out.println("[populateRecordMap] printing hashtableMatrix");
            } 
            while(rowids.hasMoreElements()) {
                //rowid = (RowId) rowids.nextElement();
                rid = (String) rowids.nextElement();
                if (verbose){  
                    //System.out.println("chiave: " +rowid.toString()+ "  valore: " + hashtableMatrix.get(rowid).toString());
                    if(verbose&&verboseLocal) System.out.println("[populateRecordMap] key: " +rid.toString()+ "  value: " + recordMap.get(rid).toString());
                } 
            }            
            Enumeration generic = rowidMap.keys();
            Integer genericInteger;
            if (verbose){  
                if(verbose&&verboseLocal) System.out.println("[populateRecordMap]---------------------------------");
                if(verbose&&verboseLocal) System.out.println("[populateRecordMap] 1.4 printing mapRowid");
            } 
            while(generic.hasMoreElements()) {
                //rowid = (RowId) rowids.nextElement();
                genericInteger = (Integer) generic.nextElement();
                if (verbose){  
                    //System.out.println("chiave: " +rowid.toString()+ "  valore: " + hashtableMatrix.get(rowid).toString());
                    if(verbose&&verboseLocal) System.out.println("[populateRecordMap] key: " +genericInteger.toString()+ "  value: " + rowidMap.get(genericInteger).toString());
                } 
            }                        
            generic = rowidMapReverse.keys();
            String genericString;
            if (verbose){  
                if(verbose&&verboseLocal) System.out.println("[populateRecordMap]---------------------------------");
                if(verbose&&verboseLocal) System.out.println("[populateRecordMap] 1.5 printing mapRowidReverse");
            } 
            while(generic.hasMoreElements()) {
                //rowid = (RowId) rowids.nextElement();
                genericString = (String) generic.nextElement();
                if (verbose){                      
                    if(verbose&&verboseLocal) System.out.println("[populateRecordMap] key: " +genericString+ "  value: " + rowidMapReverse.get(genericString).toString());
                } 
            }                    
        } catch (SQLException e) {
            System.out.println(e.getMessage());                  
        } finally {      
            if (statement != null) {
                statement.close();
            }      
        }               
        
    }
  
    /*
        popola le seguenti strutture dati:            
            rowidMap        - mappa un id intero 1-n con un rowid 
            rowidMapReverse - mappa un rowid con un intero 1-n

    */
    public static void populateRowidsHashtable(Connection connection) throws SQLException {
        boolean verboseLocal = false;
        
        Enumeration names;
        Enumeration rowids;
        String key;  
        String rid = null;        
        
        rowidMap = new Hashtable< Integer , String>();
        rowidMapReverse = new Hashtable< String, Integer>();            
        try {
            Statement statement = connection.createStatement();
            if(verbose&&verboseLocal) System.out.println("[populateRowidsHashtable]"+selectTableSQL);            
            // esegue lo statement SQL 
            ResultSet rs = statement.executeQuery(selectTableSQL);            
            // recupera i metadati
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            if(verbose&&verboseLocal) System.out.println("[populateRowidsHashtable]Total column n.:"+columnCount);                 
            Integer counter=0;     
            while (rs.next()) {                                
                if (verbose){  
                    if(verbose&&verboseLocal) System.out.println("[populateRowidsHashtable]--------------------------------");
                    if(verbose&&verboseLocal) System.out.println("[populateRowidsHashtable]storing rowids in hashtable");
                }                 
                rid = rs.getString("ROWID");
                counter++;
                if(verbose&&verboseLocal) System.out.println("[populateRowidsHashtable]counter: "+counter+" - rid: "+rid);
                rowidMap.put(counter, rid);
                rowidMapReverse.put(rid, counter);                                                                                                    
            }            
            Enumeration generic = rowidMap.keys();
            Integer genericInteger;
            if (verbose){  
                if(verbose&&verboseLocal) System.out.println("[populateRowidsHashtable]---------------------------------");
                if(verbose&&verboseLocal) System.out.println("[populateRowidsHashtable]mapRowid");
            } 
            while(generic.hasMoreElements()) {
                //rowid = (RowId) rowids.nextElement();
                genericInteger = (Integer) generic.nextElement();
                if (verbose){  
                    //System.out.println("chiave: " +rowid.toString()+ "  valore: " + hashtableMatrix.get(rowid).toString());
                    if(verbose&&verboseLocal) System.out.println("[populateRowidsHashtable]key: " +genericInteger.toString()+ " value: " + rowidMap.get(genericInteger).toString());
                } 
            }                        
            generic = rowidMapReverse.keys();
            String genericString;
            if (verbose){  
                if(verbose&&verboseLocal) System.out.println("[populateRowidsHashtable]---------------------------------");
                if(verbose&&verboseLocal) System.out.println("[populateRowidsHashtable] mapRowidReverse");
            } 
            while(generic.hasMoreElements()) {
                //rowid = (RowId) rowids.nextElement();
                genericString = (String) generic.nextElement();
                if (verbose){                      
                    if(verbose&&verboseLocal) System.out.println("[populateRowidsHashtable] key: " +genericString+ "  value: " + rowidMapReverse.get(genericString).toString());
                } 
            }                    
        } catch (SQLException e) {
            System.out.println(e.getMessage());                  
        } finally {      
            if (statement != null) {
                statement.close();
            }      
        }                       
    }
           
    public static Hashtable<String, String> getRecordByRowid(Connection connection, String rowid) throws SQLException {
        boolean verboseLocal = false;
        String selectTableSQL = selectTableSQLBase +
                        "where ROWID like '" + rowid + "'";  

        Enumeration names;
        Enumeration keys;
        String key;  
        String rid = null;   
        Hashtable<String, String> hashtable = new Hashtable<String, String>();
        //Hashtable<String, Hashtable<String, String>> recordMap = new Hashtable<String, Hashtable<String, String>>();      
        //rowidMap = new Hashtable< Integer , String>();
        //rowidMapReverse = new Hashtable< String, Integer>();  
        if(verbose&&verboseLocal) System.out.println("[getRecordByRowid] INIZIO ESECUZIONE METODO");        
        Statement statement = connection.createStatement();
        try {           
            if(verbose&&verboseLocal) System.out.println("[getRecordByRowid]"+selectTableSQL);            
            // esegue lo statement SQL 
            ResultSet rs = statement.executeQuery(selectTableSQL);   
            try {
                // recupera i metadati
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();
                if(verbose&&verboseLocal) System.out.println("[getRecordByRowid] Numero colonne:"+columnCount);                 
                Integer counter=0; 
                // FIXME: andrebbe gestito il caso in cui dovessero esistere due record con stesso rowid
                // oppure il caso in cui venisse passato un rowid inesistente
                // i due casi in teoria non dovrebbero verificarsi mai

                while (rs.next()) {                

                    if (verbose){  
                        if(verbose&&verboseLocal) System.out.println("[getRecordByRowid] ---------------------------------");
                        if(verbose&&verboseLocal) System.out.println("[getRecordByRowid] 1.1 memorizzazione record in hashtable");
                    } 
                    for (int i = 2; i <= columnCount; i++ ) {
                        String name = rsmd.getColumnName(i);
                        if (verbose){                        
                            if(verbose&&verboseLocal) System.out.println("[getRecordByRowid] memorizzo in hashtable campo : " + i + ", nome: "+name);
                        }                  
                        String value = rs.getString(name);
                        if (value!=null)
                            hashtable.put(name,value);
                        else 
                            hashtable.put(name,"NULL_VALUE");                                                                   
                    }

                }
                keys = hashtable.keys();
                if (verbose){  
                    if(verbose&&verboseLocal) System.out.println("[getRecordByRowid] ---------------------------------");
                    if(verbose&&verboseLocal) System.out.println("[getRecordByRowid] 1.3 stampa contenuto hashtable");
                } 
                while(keys.hasMoreElements()) {
                    //rowid = (RowId) rowids.nextElement();
                    key = (String) keys.nextElement();
                    if (verbose){  
                        //System.out.println("chiave: " +rowid.toString()+ "  valore: " + hashtableMatrix.get(rowid).toString());
                        if(verbose&&verboseLocal) System.out.println("[getRecordByRowid] chiave: " +key+ "  valore: " + hashtable.get(key).toString());
                    } 
                } 
            } catch (SQLException e) {
                System.out.println(e.getMessage()); 
            }
            finally {
                try { rs.close(); } catch (Exception ignore) { }
            }
                         
        } catch (SQLException e) {
            System.out.println(e.getMessage());                  
        } finally {      
            try { statement.close(); } catch (Exception ignore) { }
        } 
        if(verbose&&verboseLocal) System.out.println("[getRecordByRowid] FINE ESECUZIONE METODO");        
        return hashtable;
    }

    public static void printMatrix(int matrix[][], int maxElem, String title){        
        int i,j;
        System.out.println("=================================");
        System.out.println("contenuto matrice: "+title);
        System.out.println("---------------------------------");
        for (i = 1; i <= maxElem; i++ ) {                      
            for (j = 1; j <= maxElem; j++ ) {   
                
                System.out.print("[");
                if (matrix[i][j]<=9) System.out.print(" "); 
                System.out.print(matrix[i][j]+"]");                                 
            }
            System.out.println();
        }
        System.out.println("=================================");
    }

    public static void tmapClusterResultsPut(int i, int j){ 
        boolean verboseLocal = false;     
        if (tmapClusterResults.containsKey(rowidMap.get(i))){
            if(verbose&&verboseLocal)System.out.println("[findSimilarity] Contains i =YES : "+rowidMap.get(i));
            int oldValue = tmapClusterResults.get(rowidMap.get(i));
            int newValue = oldValue++;
            if(verbose&&verboseLocal)System.out.println("[findSimilarity] oldValue, newValue "+oldValue+", "+newValue);
            tmapClusterResults.replace(rowidMap.get(i), newValue, oldValue);
        }
        else tmapClusterResults.put(rowidMap.get(i), 1);

        if (tmapClusterResults.containsKey(rowidMap.get(j))){
            if(verbose&&verboseLocal)System.out.println("[findSimilarity] Contains j =YES : "+rowidMap.get(j));
            int oldValue = tmapClusterResults.get(rowidMap.get(j));
            int newValue = oldValue++;
            if(verbose&&verboseLocal)System.out.println("[findSimilarity] oldValue, newValue "+oldValue+", "+newValue);
            tmapClusterResults.replace(rowidMap.get(j), newValue, oldValue);
        }
        else tmapClusterResults.put(rowidMap.get(j), 1);
    }
    
    
    public static int [][] findSimilarity(Connection connection) throws SQLException {
        boolean verboseLocal = true;
        Enumeration names;
        Enumeration rowids;
        String key;  
        String rid = null; 
        tmapClusterResults = new TreeMap<String, Integer>();           
        
        //rowidMap 
        //rowidMapReverse       
        // devo popolare una matrice i,j i cui elementi sono il risultato del controllo di similarità tra i record
        // aventi rowid - > i,j  corrispondenti
        // dichiaro la matrice
        
        int i,j,k;
        int maxElem = rowidMap.size();
        /* FIXME: targetDist è la distanza target su cui classificare, elevarla a variabile perlomeno static #####*/
        int targetDist = 2;
        int[][] similarityDistances = new int[maxElem+1][maxElem+1];
        int[][] similarityCluster    = new int[maxElem+1][maxElem+1];
        boolean[][] similarityApproximate    = new boolean[maxElem+1][maxElem+1];

        int [][] similarityLB = new int[maxElem+1][maxElem+1];
        int [][] similarityUB = new int[maxElem+1][maxElem+1];
        
        for (i = 1; i <= maxElem; i++ ) {            
            for (j = 1 + i; j <= maxElem; j++ ) {
                similarityApproximate [i][j] = true;
            }
        }
        
        for (i = 1; i <= maxElem; i++ ) {            
            for (j = 1 + i; j <= maxElem; j++ ) {
                similarityLB [i][j] = -9;
            }
        }
        for (i = 1; i <= maxElem; i++ ) {            
            for (j = 1 + i; j <= maxElem; j++ ) {
                similarityUB [i][j] = 99;
            }
        }
         
        int counter;
        String rowidA, rowidB;
        Enumeration fieldsA;
        Enumeration fieldsB;
        int dist = 0;
        int maxDist = 0;
        int LBDist = 0;
        int LBDist1 = 0;
        int LBDist2 = 0;
        int UBDist = 0;
        int LBMax = 0;
        boolean calculateDist = true;
        int numeroCalcoli = 0; 
        int numberOfSimilarity = 0;
        Hashtable<String, String> hashtableA;                
        Hashtable<String, String> hashtableB;
        String switchCondition = null ;
        if (method.compareTo("OPTRET")==0)  switchCondition = "OPTRET";
        if ((method.compareTo("OPTLOCAL")==0)||(method.compareTo("BASE")==0))  switchCondition = "OPTLOCAL_or_BASE";
        if ((method.compareTo("OPT")==0))  switchCondition = "OPT";
        switch(switchCondition)  {
            case "OPTRET":  
                // calcolo le distanze per la riga 1 della matrice
                i = 1;
                rowidB = rowidMap.get(i);
                for (j = 1; j <=maxElem; j++ ) {
                    rowidA = rowidMap.get(j);
                    counter = 0;   
                    if (ramLoad) {
                        hashtableA = recordMap.get(rowidA);                
                        hashtableB = recordMap.get(rowidB);
                    }
                    else {
                        hashtableA = Cluster.getRecordByRowid(connection,  rowidA);                
                        hashtableB = Cluster.getRecordByRowid(connection,  rowidB);
                    }
                    maxDist = hashtableA.size();
                    fieldsA = hashtableA.keys();
                    while(fieldsA.hasMoreElements()) {
                        key = (String) fieldsA.nextElement();                       
                        Object elemA = hashtableA.get(key);
                        Object elemB = hashtableB.get(key);
                        if (elemA.equals(elemB)){
                            counter++;
                        }
                    }
                    dist = maxDist-counter;                       
                    similarityDistances[i][j] = dist;                                                      
                    similarityApproximate[i][j] = false;          
                    similarityUB[i][j] = dist;
                    similarityLB[i][j] = dist;         
                    if ((targetDist - dist)>=0){                                                        
                        similarityCluster[i][j] = 1;                                                         
                    }
                    numeroCalcoli++;    
                }


                for (i = 1; i <= maxElem; i++ ) {
                    if (i>1){
                        rowidA = rowidMap.get(i);                                            
                        int h=0;
                        int w=-1;
                        int y=0;
                        for (j = 1 + i; j<=maxElem; j++){ 

                            //for (y = i+w; y<=maxElem; y++)
                            //    {

                                h++;
                                int maxElemRiga = maxElem-j;                        
                                for (k = j-h; k<=maxElem; k++){
                                    if (k!=j-1){
                                        if(verbose&&verboseLocal) System.out.println("[findSimilarity] ELEMENTI-->("+(i-1)+", "+(k)+") - ("+(i-1)+", "+(j-1)+"), ("+i+"), ("+j+")");                               
                                        rowidB = rowidMap.get(j);                         
                                        calculateDist = false;

                                        if (similarityApproximate[i-1][k]){
                                            LBDist1 = similarityLB[i-1][k]-similarityDistances[i-1][j-1];    
                                        }
                                        else {
                                            if (similarityApproximate[i-1][j-1]){
                                                LBDist1 = similarityDistances[i-1][k]-similarityUB[i-1][j-1];    
                                            }
                                            else LBDist1 = similarityDistances[i-1][k]-similarityDistances[i-1][j-1];  
                                        }

                                        if (similarityApproximate[i-1][j-1]){
                                            LBDist2 = similarityLB[i-1][j-1]-similarityDistances[i-1][k];
                                        }
                                        else {
                                            if (similarityApproximate[i-1][k]){
                                                LBDist2 = similarityDistances[i-1][j-1]-similarityUB[i-1][k];
                                            }
                                            else LBDist2 = similarityDistances[i-1][j-1]-similarityDistances[i-1][k]; 
                                        }
                                        if (LBDist1>=LBDist2) {
                                            LBMax = LBDist1;
                                        }
                                        else {
                                            LBMax = LBDist2;
                                        }

                                        // per d13+d12: se approssimata uno dei due addendi, considero l'UB...                                                                  
                                        if (similarityApproximate[i-1][k]||similarityApproximate[i-1][j-1]){
                                            UBDist = similarityUB[i-1][j-1]+similarityUB[i-1][k];
                                        }
                                        else UBDist = similarityDistances[i-1][j-1]+similarityDistances[i-1][k];

                                        //Se LB' è maggiore...aggiorno matrice LB
                                        if (LBMax > similarityLB[i][j]){
                                            similarityLB[i][j] = LBMax;                                
                                        }                                                                
                                        if (UBDist < similarityUB[i][j] ) {
                                            similarityUB[i][j] = UBDist;
                                        }
                                        if (similarityUB[i][j] == 0) {
                                            similarityUB[i][j] = UBDist;
                                        }
                                        if(verbose&&verboseLocal) System.out.println("[findSimilarity] LBDist1 = "+ LBDist1 + ", LBDist2 = "+ LBDist2+ ", LBMax = "+ LBMax + ", UBDist = "+UBDist+ ",LB matrix = "+similarityLB[i][j]+", UB matrix = "+similarityUB[i][j]); 

                                    } 
                                }
                            //}
                        }
                        for (j = 1 + i; j<=maxElem; j++){ 
                            rowidB = rowidMap.get(j);  
                            if (similarityLB[i][j]==similarityUB[i][j]) {
                                similarityDistances[i][j] = similarityLB[i][j];
                                similarityApproximate[i][j] = true;
                                calculateDist = false;
                            } 
                            else {
                                if (similarityLB[i][j]<similarityUB[i][j]){                        
                                    //System.out.println("confronto campi :  elemA["+i+"]["+rowidA+"], elemB["+j+"]["+rowidB+"]  similarity: "+similarityBase[i][j]);
                                    counter = 0; 
                                    if (ramLoad) {
                                        hashtableA = recordMap.get(rowidA);                
                                        hashtableB = recordMap.get(rowidB);
                                    }
                                    else {
                                        hashtableA = Cluster.getRecordByRowid(connection, rowidA);                
                                        hashtableB = Cluster.getRecordByRowid(connection, rowidB);
                                    }
                                    maxDist = hashtableA.size();
                                    fieldsA = hashtableA.keys();
                                    while(fieldsA.hasMoreElements()) {
                                        key = (String) fieldsA.nextElement();                       
                                        Object elemA = hashtableA.get(key);
                                        Object elemB = hashtableB.get(key);
                                        if (elemA.equals(elemB)){
                                            counter++;
                                        }
                                    }
                                    dist = maxDist-counter;                            
                                    similarityDistances[i][j] = dist;                                        
                                    similarityApproximate[i][j] = false;

                                    //similarityUB[i][j] = dist;
                                    //similarityLB[i][j] = dist; 

                                    if ((targetDist - dist)>=0){                                                        
                                        similarityCluster[i][j] = 1;                                                                                                                                                                                                                                                                                                                               
                                        tmapClusterResultsPut(i,j);                                          
                                        numberOfSimilarity++;
                                    }
                                    numeroCalcoli++;
                                    if(verbose&&verboseLocal) {
                                        System.out.println("[findSimilarity] CALC=YES");
                                    }

                                }
                            }
                        }
                    }
                }                                                                                                                    
                break;
            
                case  "OPT":                                                                                
                    for (i = 1; i <= maxElem; i++ ) {                    
                        rowidA = rowidMap.get(i);                                                                    
                        int w=-1;
                        int y=0;
                        for (j = 1 + i; j<=maxElem; j++){ 
                            rowidB = rowidMap.get(j); 
                            if (i>1){
                                int h=-1;
                                for (y = 1; y<i; y++){ 
                                    h++;
                                    int q;
                                    int maxElemRigaP = maxElem-h+1;  
                                    int p;
                                    int z=-1;
                                    z++;                                 
                                    for (p = 2+z; p<=maxElemRigaP; p++){
                                        int maxElemRigaQ = maxElem-z; 
                                        for (q = p+1; q<=maxElemRigaQ; q++){                                            
                                            if(verbose&&verboseLocal) System.out.println("[findSimilarity] ELEMENTI-->[y,p]-[y,q]-[i,j]-->("+(y)+", "+(p)+") - ("+(y)+", "+(q)+"), ("+i+"), ("+j+")");                                
                                            calculateDist = false;
                                            //ottimizzazione (se le distanze della riga 1 sono già state calcolate...
                                                                     
                                            // caso 1: verifico che d12 non sia approssimato...   

                                            if (!similarityApproximate[i-1][j-1]){

                                                LBDist = similarityDistances[i-1][j]-similarityDistances[i-1][j-1];                            
                                                // verifico se  d23 >= d13-d12>soglia
                                                if (LBDist > targetDist){                                
                                                    similarityDistances[i][j] = LBDist;
                                                    //similarityOptimized[j][i] = LBDist; 
                                                    similarityApproximate [i][j] = true;
                                                    if ((targetDist - LBDist)>=0){                                
                                                        similarityCluster[i][j] = 1;
                                                        tmapClusterResultsPut(i,j);                                                                             
                                                        numberOfSimilarity++;
                                                    }
                                                }
                                                else {                                
                                                    LBDist = similarityDistances[i-1][j-1]-similarityDistances[i-1][j];
                                                    if (LBDist > targetDist && !similarityApproximate [i-1][j]){
                                                        similarityDistances[i][j] = LBDist; 
                                                        //similarityOptimized[j][i] = LBDist; 
                                                        similarityApproximate [i][j] = true;
                                                        if ((targetDist - LBDist)>=0){                                
                                                            similarityCluster[i][j] = 1;
                                                            tmapClusterResultsPut(i,j);
                                                            numberOfSimilarity++;
                                                        }
                                                    }                                
                                                    else {
                                                        calculateDist = true;                          
                                                    }                                
                                                }

                                            }
                                            else {
                                                if (!similarityApproximate[i-1][j]){
                                                    LBDist = similarityDistances[i-1][j-1]-similarityDistances[i-1][j];
                                                    if (LBDist > targetDist){
                                                        similarityDistances[i][j] = LBDist; 
                                                        //similarityOptimized[j][i] = LBDist; 
                                                        similarityApproximate [i][j] = true;
                                                        if ((targetDist - LBDist)>=0){                                
                                                            similarityCluster[i][j] = 1;
                                                            tmapClusterResultsPut(i,j);;
                                                            numberOfSimilarity++;
                                                        }
                                                    }
                                                    else {
                                                        calculateDist = true;
                                                    }
                                                }
                                                else calculateDist = true;
                                            }
                                            
                                        }
                                    }


                                }
                                
                                if (calculateDist){
                                    //System.out.println("confronto campi :  elemA["+i+"]["+rowidA+"], elemB["+j+"]["+rowidB+"]  similarity: "+similarityBase[i][j]);
                                    counter = 0;  
                                    if (ramLoad) {
                                        hashtableA = recordMap.get(rowidA);                
                                        hashtableB = recordMap.get(rowidB);
                                    }
                                    else {
                                        hashtableA = Cluster.getRecordByRowid(connection, rowidA);                
                                        hashtableB = Cluster.getRecordByRowid(connection, rowidB);
                                    }

                                    maxDist = hashtableA.size();
                                    fieldsA = hashtableA.keys();
                                    while(fieldsA.hasMoreElements()) {
                                        key = (String) fieldsA.nextElement();                       
                                        Object elemA = hashtableA.get(key);
                                        Object elemB = hashtableB.get(key);
                                        if (elemA.equals(elemB)){
                                            counter++;
                                        }
                                    }
                                    dist = maxDist-counter;                            

                                    similarityDistances[i][j] = dist;                                        
                                    //similarityOptimized[j][i] = dist; 
                                    similarityApproximate[i][j] = false;
                                    /*
                                    if (method.compareTo("OPTRET")==0&&i==1) {
                                        similarityUB[i][j] = dist;
                                        similarityLB[i][j] = dist;
                                    }*/


                                    if ((targetDist - dist)>=0){                                                        
                                        similarityCluster[i][j] = 1; 
                                        tmapClusterResultsPut(i,j);
                                        numberOfSimilarity++;
                                    }
                                    numeroCalcoli++;
                                    if(verbose&&verboseLocal) System.out.println("[findSimilarity] CALC=YES");
                                }
                                else  {
                                    if(verbose&&verboseLocal) System.out.println("[findSimilarity] CALC=NO");
                                }
                            }
                            else {
                                                                    //System.out.println("confronto campi :  elemA["+i+"]["+rowidA+"], elemB["+j+"]["+rowidB+"]  similarity: "+similarityBase[i][j]);
                                counter = 0;  
                                if (ramLoad) {
                                    hashtableA = recordMap.get(rowidA);                
                                    hashtableB = recordMap.get(rowidB);
                                }
                                else {
                                    hashtableA = Cluster.getRecordByRowid(connection, rowidA);                
                                    hashtableB = Cluster.getRecordByRowid(connection, rowidB);
                                }

                                maxDist = hashtableA.size();
                                fieldsA = hashtableA.keys();
                                while(fieldsA.hasMoreElements()) {
                                    key = (String) fieldsA.nextElement();                       
                                    Object elemA = hashtableA.get(key);
                                    Object elemB = hashtableB.get(key);
                                    if (elemA.equals(elemB)){
                                        counter++;
                                    }
                                }
                                dist = maxDist-counter;                            

                                similarityDistances[i][j] = dist;                                        
                                //similarityOptimized[j][i] = dist; 
                                similarityApproximate[i][j] = false;
                                /*
                                if (method.compareTo("OPTRET")==0&&i==1) {
                                    similarityUB[i][j] = dist;
                                    similarityLB[i][j] = dist;
                                }*/


                                if ((targetDist - dist)>=0){                                                        
                                    similarityCluster[i][j] = 1; 
                                    tmapClusterResultsPut(i,j);
                                    numberOfSimilarity++;
                                }
                                numeroCalcoli++;
                                if(verbose&&verboseLocal) System.out.println("[findSimilarity] CALC=YES");
                                
                            }
                            
                            
                        }
                    
                    }
                    
                break;
                               
            case  "OPTLOCAL_or_BASE":
                for (i = 1; i <= maxElem; i++ ) {
                    rowidA = rowidMap.get(i);            
                    for (j = 1 + i; j <= maxElem; j++ ) {
                        rowidB = rowidMap.get(j);                         
                        if(verbose&&verboseLocal) System.out.println("[findSimilarity]-->"+i+"  "+j);                
                        if (method.compareTo("OPTLOCAL")==0){
                            calculateDist = false;
                            //ottimizzazione (se le distanze della riga 1 sono già state calcolate...
                            if (i>1){                         
                                // caso 1: verifico che d12 non sia approssimato...   

                                if (!similarityApproximate[i-1][j-1]){

                                    LBDist = similarityDistances[i-1][j]-similarityDistances[i-1][j-1];                            
                                    // verifico se  d23 >= d13-d12>soglia
                                    if (LBDist > targetDist){                                
                                        similarityDistances[i][j] = LBDist;
                                        //similarityOptimized[j][i] = LBDist; 
                                        similarityApproximate [i][j] = true;
                                        if ((targetDist - LBDist)>=0){                                
                                            similarityCluster[i][j] = 1;
                                            tmapClusterResultsPut(i,j);                                                                             
                                            numberOfSimilarity++;
                                        }
                                    }
                                    else {                                
                                        LBDist = similarityDistances[i-1][j-1]-similarityDistances[i-1][j];
                                        if (LBDist > targetDist && !similarityApproximate [i-1][j]){
                                            similarityDistances[i][j] = LBDist; 
                                            //similarityOptimized[j][i] = LBDist; 
                                            similarityApproximate [i][j] = true;
                                            if ((targetDist - LBDist)>=0){                                
                                                similarityCluster[i][j] = 1;
                                                tmapClusterResultsPut(i,j);
                                                numberOfSimilarity++;
                                            }
                                        }                                
                                        else {
                                            calculateDist = true;                          
                                        }                                
                                    }

                                }
                                else {
                                    if (!similarityApproximate[i-1][j]){
                                        LBDist = similarityDistances[i-1][j-1]-similarityDistances[i-1][j];
                                        if (LBDist > targetDist){
                                            similarityDistances[i][j] = LBDist; 
                                            //similarityOptimized[j][i] = LBDist; 
                                            similarityApproximate [i][j] = true;
                                            if ((targetDist - LBDist)>=0){                                
                                                similarityCluster[i][j] = 1;
                                                tmapClusterResultsPut(i,j);;
                                                numberOfSimilarity++;
                                            }
                                        }
                                        else {
                                            calculateDist = true;
                                        }
                                    }
                                    else calculateDist = true;
                                }
                            }
                            else {
                                calculateDist = true;
                            }                
                        }

                        if (calculateDist){
                            //System.out.println("confronto campi :  elemA["+i+"]["+rowidA+"], elemB["+j+"]["+rowidB+"]  similarity: "+similarityBase[i][j]);
                            counter = 0;  
                            if (ramLoad) {
                                hashtableA = recordMap.get(rowidA);                
                                hashtableB = recordMap.get(rowidB);
                            }
                            else {
                                hashtableA = Cluster.getRecordByRowid(connection, rowidA);                
                                hashtableB = Cluster.getRecordByRowid(connection, rowidB);
                            }

                            maxDist = hashtableA.size();
                            fieldsA = hashtableA.keys();
                            while(fieldsA.hasMoreElements()) {
                                key = (String) fieldsA.nextElement();                       
                                Object elemA = hashtableA.get(key);
                                Object elemB = hashtableB.get(key);
                                if (elemA.equals(elemB)){
                                    counter++;
                                }
                            }
                            dist = maxDist-counter;                            

                            similarityDistances[i][j] = dist;                                        
                            //similarityOptimized[j][i] = dist; 
                            similarityApproximate[i][j] = false;
                            /*
                            if (method.compareTo("OPTRET")==0&&i==1) {
                                similarityUB[i][j] = dist;
                                similarityLB[i][j] = dist;
                            }*/


                            if ((targetDist - dist)>=0){                                                        
                                similarityCluster[i][j] = 1; 
                                tmapClusterResultsPut(i,j);
                                numberOfSimilarity++;
                            }
                            numeroCalcoli++;
                            if(verbose&&verboseLocal) System.out.println("[findSimilarity] CALC=YES");
                        }
                        else  {
                            if(verbose&&verboseLocal) System.out.println("[findSimilarity] CALC=NO");
                        }
                    }            
                }
                break;
                
                default:
                    if(verbose&&verboseLocal) System.out.println("[findSimilarity] Method non found");
            }

        

        if (method.compareTo("BASE")==0)         printResults(connection, "BASE, NO OPTIMIZATION",  similarityDistances,   similarityCluster ,  maxElem,  numeroCalcoli, numberOfSimilarity );
        if (method.compareTo("OPT")==0)   {        
                printMatrix(similarityLB, maxElem, "LATE CALC METHOD - LB VALUE MATRIX");
                printResults(connection, "PARTIAL (UPPER BOUND ONLY)",  similarityDistances,   similarityCluster ,  maxElem,  numeroCalcoli, numberOfSimilarity );
        }
        if (method.compareTo("OPTLOCAL")==0)     printResults(connection, "PARTIAL with LOCAL SEARCH(UPPER BOUND ONLY)",  similarityDistances,   similarityCluster ,  maxElem,  numeroCalcoli, numberOfSimilarity );
        if (method.compareTo("OPTRET")==0) {
            //printMatrix(similarityApproximate, maxElem, "LATE CALC METHOD (LB+UB)");
            printMatrix(similarityLB, maxElem, "LATE CALC METHOD - LB VALUE MATRIX");
            printMatrix(similarityUB, maxElem, "LATE CALC METHOD - UB VALUE MATRIX");        
            printResults(connection, "LATE CALC OPTIMIZATION - MATRIX DISTANCE",    similarityDistances,   similarityCluster ,  maxElem,  numeroCalcoli, numberOfSimilarity );
        }
        
        
        return similarityDistances;
    }
       

        
    
    // printResults : stampa risultati, caso ramLoad = TRUE;   

    public static void printResults(Connection connection, String methodTitle,  int similarityDistances[][], int similarityCluster [][], int maxElem, int numeroCalcoli, int numberOfSimilarity ) throws SQLException {    
        
        int i, j;

        printMatrix(similarityDistances, maxElem, methodTitle);
        printMatrix(similarityCluster, maxElem, methodTitle);

    

        System.out.println("[findSimilarity] ---------------------------------");
        System.out.println("[findSimilarity] Cluster :");
        System.out.println("[findSimilarity] ---------------------------------");
        for (i = 1; i <= maxElem; i++ ) {                      
            for (j = 1 + i; j <= maxElem; j++ ) {     
                if (similarityCluster[i][j] == 1){
                    if (ramLoad){
                        System.out.println("[findSimilarity] i,j: "+i+","+j+"rowid: "+rowidMap.get(i)+ " val: "+recordMap.get(rowidMap.get(i)).toString());
                        System.out.println("[findSimilarity] i,j: "+i+","+j+"rowid: "+rowidMap.get(j)+ " val: "+recordMap.get(rowidMap.get(j)).toString());
                    }
                    else {
                        System.out.println("[findSimilarity] i,j: "+i+","+j+"rowid: "+rowidMap.get(i)+ " val: "+Cluster.getRecordByRowid(connection, rowidMap.get(i)));
                        System.out.println("[findSimilarity] i,j: "+i+","+j+"rowid: "+rowidMap.get(j)+ " val: "+Cluster.getRecordByRowid(connection, rowidMap.get(i)));
                        
                    }
                    //System.out.println(recordMap.get(rowidMap.get(i)).toString());
                    //System.out.println(recordMap.get(rowidMap.get(j)).toString());
                }
            }
            //System.out.println();
        }
        System.out.println("[findSimilarity] ---------------------------------");
        System.out.println("[findSimilarity] Cluster (unique)");
        System.out.println("[findSimilarity] ---------------------------------");
        Set set = tmapClusterResults.entrySet();
        Iterator iterator = set.iterator();
        while(iterator.hasNext()) {
           Map.Entry mentry = (Map.Entry)iterator.next();
           System.out.print("[findSimilarity] rowid is: "+ mentry.getKey() + " number of occurrences: ");
           System.out.println(mentry.getValue());
        }
        System.out.println("[findSimilarity] ---------------------------------");
        System.out.println("[findSimilarity] ---------------------------------");
        System.out.println("[findSimilarity] N. of calc.: "+numeroCalcoli);
        System.out.println("[findSimilarity] ---------------------------------");
        System.out.println("[findSimilarity] ---------------------------------");
        System.out.println("[findSimilarity] N. of sim. couple : "+numberOfSimilarity);
        System.out.println("[findSimilarity] ---------------------------------");
        System.out.println("[findSimilarity] N. of unique elem.: "+tmapClusterResults.size());
        System.out.println("[findSimilarity] ====================================");
        System.out.println("[findSimilarity] *** A P P    P A R A M E T E R S ***");
        System.out.println("[findSimilarity] ->  targetDist: "+targetDist);
        System.out.println("[findSimilarity] ->  ramLoad   : "+ramLoad);
        System.out.println("[findSimilarity] ->  verbose   : "+verbose);
        System.out.println("[findSimilarity] ->  numrow    : "+numrow);
        System.out.println("[findSimilarity] ->  method    : "+method);
        System.out.println("[findSimilarity] =====================================");
        
    }
    



    
/*========================================================================================*/
/*========================================================================================*/
    
    private static void updateDbStructure(TreeMap<String, Integer> dict, Connection connection, String destTable, String source) throws SQLException {               
        
        try {
            
            Statement statement = connection.createStatement();
            System.out.println(destTable);
            String strSQL_part1 = "INSERT INTO "+destTable+ "(LEMMA, N_OCC_LEXSRC, N_OCC_LEXSRC_PERC, N_LEXSRC, N_OCC_TWEETS_ANGER, N_OCC_TWEETS_ANGER_PERC, SRC) "; 
            String strSQL;
            String token;
            for (Map.Entry<String, Integer> k : dict.entrySet()) {
                token = (String)k.getKey();
                token = token.replaceAll("'","");
                if (source.equals("CORPUS")){
                    strSQL  = strSQL_part1 + " values ('"+token+"', 0 , 0.0, 0.0, "+k.getValue()+", 0.0, '"+source+"')";                              
                    System.out.println(strSQL);
                }
                else {
                    strSQL  = strSQL_part1 + " values ('"+token+"', "+k.getValue()+" , 0.0, 0.0, 0.0, 0.0, '"+source+"')";        
                    
                }
                //System.out.println("chiave: "+token+" - valore: "+k.getValue());
                // esegue lo statement SQL 
                
                statement.executeUpdate(strSQL);               
            }
    
        } catch (SQLException e) {
            System.out.println(e.getMessage());           
        } finally {      
            if (statement != null) {
                statement.close();
            }      
        }  
        //return true;
    }
            
        

   
    
    
    
    



    

}
