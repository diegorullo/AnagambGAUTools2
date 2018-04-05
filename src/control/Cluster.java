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
    private static final String numrow = "40";
    private static final String method = "OPT"; 
                                // ottimizzazione: 
                                //BASE=nessuna 
                                //OPT=semi ottimizzato 
                                //OPTRET =calcolo distanza ritardato
         
    //private static Hashtable<String, Hashtable<String, String>> recordMap;
    
    /**
    *
    * @author diego
    * metodo di prova per la stampa di tutti i valori del campo ROWID della tabella
    */
    public static void printSourceFields(Connection connection, Boolean verbose) throws SQLException {        
        boolean verboseLocal = false;
        
        String selectTableSQL = "select * from ANAGAMB_V_GAU_ARPA";         
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
        
    restituisce recordMap
    */
    public static Hashtable<String, Hashtable<String, String>> populateRecordMap(Connection connection, Boolean verbose) throws SQLException {
        boolean verboseLocal = false;
        String selectTableSQL = "select " +
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
                        ",REASON_CHANGE from ANAGAMB_V_GAU_ARPA " +
                        "where rownum <= " + numrow +" " +  
                        " and (rowid like 'AAASAOAAIAAAAE7AAH' or " +
                        "rowid like 'AAASAOAAIAAAAE7AAI' or " +
                        "rowid like 'AAASAOAAIAAAAE7AAJ' or " +
                        "rowid like 'AAASAOAAIAAAAE7AAK' or " +
                        "rowid like 'AAASAOAAIAAAAE7AAL' or " +
                        "rowid like 'AAASAOAAIAAAAE7AAM' or " +
                        "rowid like 'AAASAOAAIAAAAE8AAD' or " +
                        "rowid like 'AAASAOAAIAAAAE8AAE' or " +
                        "rowid like 'AAASAOAAIAAAAE8AAF' or " +
                        "rowid like 'AAASAOAAIAAAAE8AAG' ) " ;

        Enumeration names;
        Enumeration rowids;
        String key;  
        String rid = null;        
        Hashtable<String, Hashtable<String, String>> recordMap = new Hashtable<String, Hashtable<String, String>>();      
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
        return recordMap;
    }
  
    
    public static void populateRowidsHashtable(Connection connection, Boolean verbose) throws SQLException {
        boolean verboseLocal = false;
        String selectTableSQL = "select " +
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
                        ",REASON_CHANGE from ANAGAMB_V_GAU_ARPA " +
                        //"where rownum <= " + numrow +" ";   
                "where rownum <= " + numrow +" " +  
                        " and (rowid like 'AAASAOAAIAAAAE7AAH' or " +
                        "rowid like 'AAASAOAAIAAAAE7AAI' or " +
                        "rowid like 'AAASAOAAIAAAAE7AAJ' or " +
                        "rowid like 'AAASAOAAIAAAAE7AAK' or " +
                        "rowid like 'AAASAOAAIAAAAE7AAL' or " +
                        "rowid like 'AAASAOAAIAAAAE7AAM' or " +
                        "rowid like 'AAASAOAAIAAAAE8AAD' or " +
                        "rowid like 'AAASAOAAIAAAAE8AAE' or " +
                        "rowid like 'AAASAOAAIAAAAE8AAF' or " +
                        "rowid like 'AAASAOAAIAAAAE8AAG' ) " ;

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
      
/*
    riceve in ingresso recordMap e misura la similarità tra gli elemenenti in essa contenuti
 */  
        public static int [][] findSimilarity(Hashtable<String, Hashtable<String, String>> recordMap, boolean verbose) throws SQLException {
  
        boolean verboseLocal = false;    
        Enumeration names;
        Enumeration rowids;
        String key;  
        String rid = null;        
       
        //rowidMap 
        //rowidMapReverse

        
        // devo popolare una matrice i,j i cui elementi sono il risultato del controllo di similarità tra i record
        // aventi rowid - > i,j  corrispondenti
        // dichiaro la matrice
        
        int i,j;
        int maxElem = rowidMap.size();
        /* FIXME: targetDist è la distanza target su cui classificare, elevarla a variabile perlomeno static #####*/
        int targetDist = 2;
        int similarityBase [][] = new int[maxElem+1][maxElem+1];
        int similarityCluster [][] = new int[maxElem+1][maxElem+1];
        int similarityClusterOpt [][] = new int[maxElem+1][maxElem+1];
        int similarityOptimized [][] = new int[maxElem+1][maxElem+1];
        boolean[][] similarityOptLBCheck = new boolean[maxElem+1][maxElem+1];
        boolean[][] similarityOptUBCheck = new boolean[maxElem+1][maxElem+1];
        for (i = 1; i <= maxElem; i++ ) {            
            for (j = 1 + i; j <= maxElem; j++ ) {
                similarityOptLBCheck [i][j] = false;
            }
        }
        for (i = 1; i <= maxElem; i++ ) {            
            for (j = 1 + i; j <= maxElem; j++ ) {
                similarityOptUBCheck [i][j] = false;
            }
        }
        int counter;
        String rowidA, rowidB;
        Enumeration fieldsA;
        Enumeration fieldsB;
        int dist = 0;
        int maxDist = 0;
        int LBDist = 0;        
        boolean calculateDist = true;
        int numeroCalcoli = 0;
        for (i = 1; i <= maxElem; i++ ) {
            rowidA = rowidMap.get(i);            
            for (j = 1 + i; j <= maxElem; j++ ) {
                rowidB = rowidMap.get(j);
                Hashtable<String, String> hashtableA = recordMap.get(rowidA);                
                Hashtable<String, String> hashtableB = recordMap.get(rowidB);
                maxDist = hashtableA.size();
                fieldsA = hashtableA.keys();         
                if(verbose&&verboseLocal) System.out.println("[findSimilarity]-->"+i+"  "+j);                
                if (method.compareTo("OPT")==0){
                    calculateDist = false;
                    //System.out.println("[findSimilarity] Calcolo distanza D");
                    //ottimizzazione (se le distanze della riga 1 sono già state calcolate...
                    if (i>1){ 
                        
                        // caso 1: verifico che d12 non sia approssimato...           
                        if (!similarityOptLBCheck[i-1][j-1]){
                            LBDist = similarityOptimized[i-1][j]-similarityOptimized[i-1][j-1];
                            // verifico se  d23 >= d13-d12>soglia
                            if (LBDist > targetDist && !similarityOptLBCheck [i-1][j-1]){                                
                                similarityOptimized[i][j] = LBDist;
                                similarityOptimized[j][i] = LBDist; 
                                similarityOptLBCheck [i][j] = true;
                                if ((targetDist - LBDist)>=0){                                
                                    similarityClusterOpt[i][j] = 1;
                                }
                            }
                            else {                                
                                LBDist = similarityOptimized[i-1][j-1]-similarityOptimized[i-1][j];
                                if (LBDist > targetDist && !similarityOptLBCheck [i-1][j]){
                                    similarityOptimized[i][j] = LBDist; 
                                    similarityOptimized[j][i] = LBDist; 
                                    similarityOptLBCheck [i][j] = true;
                                    if ((targetDist - LBDist)>=0){                                
                                        similarityClusterOpt[i][j] = 1;
                                    }
                                }                                
                                else {
                                    calculateDist = true;                          
                                }                                
                            }
                        }
                        else {
                            if (!similarityOptLBCheck[i-1][j]){
                                LBDist = similarityOptimized[i-1][j-1]-similarityOptimized[i-1][j];
                                if (LBDist > targetDist){
                                    similarityOptimized[i][j] = LBDist; 
                                    similarityOptimized[j][i] = LBDist; 
                                    similarityOptLBCheck [i][j] = true;
                                    if ((targetDist - LBDist)>=0){                                
                                        similarityClusterOpt[i][j] = 1;
                                    }
                                }
                                else {
                                    calculateDist = true;
                                }
                            }
                        }
                    }
                    else {
                        calculateDist = true;
                    }                
                }                
                if (calculateDist){
                    //System.out.println("confronto campi :  elemA["+i+"]["+rowidA+"], elemB["+j+"]["+rowidB+"]  similarity: "+similarityBase[i][j]);
                    counter = 1;                    
                    while(fieldsA.hasMoreElements()) {
                        key = (String) fieldsA.nextElement();
                        //System.out.println("confronto: chiave(A): " +key+ "  valore(A): " + hashtableA.get(key)+ "  con chiave(B): " +key+ "  valore(B): " + hashtableB.get(key));
                        Object elemA = hashtableA.get(key);
                        Object elemB = hashtableB.get(key);
                        if (elemA.equals(elemB)){
                            dist = maxDist-counter;                            
                            if (method.compareTo("OPT")==0){
                                if (calculateDist){
                                    similarityOptimized[i][j] = dist;
                                    similarityOptimized[j][i] = dist; 
                                }
                            }
                            else {
                                similarityBase[i][j] = dist;
                                similarityBase[j][i] = dist;    
                            }                                
                            counter++;
                            if ((targetDist - dist)>=0){                                
                                if (method.compareTo("OPT")==0){
                                    similarityClusterOpt[i][j] = 1;
                                }
                                else {
                                    similarityCluster[i][j] = 1;
                                }
                                    
                            }
                        }
                    }
                    numeroCalcoli++;
                }
            }            
        } 
        printResults( recordMap, verbose,  similarityBase, similarityOptimized ,  similarityCluster ,  similarityClusterOpt ,  maxElem,  numeroCalcoli );
        return similarityBase;
    }
        
       
    public static void printResults(Hashtable<String, Hashtable<String, String>> recordMap, boolean verbose, int similarityBase[][], int similarityOptimized [][], int similarityCluster [][], int similarityClusterOpt [][], int maxElem, int numeroCalcoli ) throws SQLException {    
        
        int i, j;
        if (method.compareTo("BASE")==0)printMatrix(similarityBase, maxElem, "BASE, NO OPTIMIZATION");
        if (method.compareTo("OPT")==0) printMatrix(similarityOptimized, maxElem, "OPTIMIZED");
        if (method.compareTo("BASE")==0)printMatrix(similarityCluster, maxElem, "BASE CLUSTER");
        if (method.compareTo("OPT")==0) printMatrix(similarityClusterOpt, maxElem, "OPTIMIZED CLUSTER");
        if (method.compareTo("BASE")==0) {
            System.out.println("[findSimilarity] ---------------------------------");
            System.out.println("[findSimilarity] Cluster :");
            System.out.println("[findSimilarity] ---------------------------------");
            for (i = 1; i <= maxElem; i++ ) {                      
                for (j = 1 + i; j <= maxElem; j++ ) {     
                    if (similarityCluster[i][j] == 1){
                        System.out.println("[findSimilarity] i,j: "+i+","+j+"rowid: "+rowidMap.get(i)+ " val: "+recordMap.get(rowidMap.get(i)).toString());
                        System.out.println("[findSimilarity] i,j: "+i+","+j+"rowid: "+rowidMap.get(j)+ " val: "+recordMap.get(rowidMap.get(j)).toString());
                        //System.out.println(recordMap.get(rowidMap.get(i)).toString());
                        //System.out.println(recordMap.get(rowidMap.get(j)).toString());
                    }
                }
                //System.out.println();
            }
        }
        if (method.compareTo("OPT")==0) {
            System.out.println("[findSimilarity] ---------------------------------");
            System.out.println("[findSimilarity] Optimized cluster :");
            System.out.println("[findSimilarity] ---------------------------------");
            for (i = 1; i <= maxElem; i++ ) {                      
                for (j = 1 + i; j <= maxElem; j++ ) {     
                    if (similarityClusterOpt[i][j] == 1){
                        System.out.println("[findSimilarity] i,j: "+i+","+j+"rowid: "+rowidMap.get(i)+ " val: "+recordMap.get(rowidMap.get(i)).toString());
                        System.out.println("[findSimilarity] i,j: "+i+","+j+"rowid: "+rowidMap.get(j)+ " val: "+recordMap.get(rowidMap.get(j)).toString());
                        //System.out.println(recordMap.get(rowidMap.get(i)).toString());
                        //System.out.println(recordMap.get(rowidMap.get(j)).toString());
                    }
                }
                //System.out.println();
            }
        }
        System.out.println("[findSimilarity] ---------------------------------");
        System.out.println("[findSimilarity] N. of calc.: "+numeroCalcoli);
        System.out.println("[findSimilarity] ---------------------------------");
        
    }
    public static void printResults(Connection connection, boolean verbose, int similarityBase[][], int similarityOptimized [][], int similarityCluster [][], int similarityClusterOpt [][], int maxElem, int numeroCalcoli ) throws SQLException {    
        
        int i, j;
        if (method.compareTo("BASE")==0)printMatrix(similarityBase, maxElem, "BASE, NO OPTIMIZATION");
        if (method.compareTo("OPT")==0) printMatrix(similarityOptimized, maxElem, "OPTIMIZED");
        if (method.compareTo("BASE")==0)printMatrix(similarityCluster, maxElem, "BASE CLUSTER");
        if (method.compareTo("OPT")==0) printMatrix(similarityClusterOpt, maxElem, "OPTIMIZED CLUSTER");
        if (method.compareTo("BASE")==0){
            System.out.println("[findSimilarity] ---------------------------------");
            System.out.println("[findSimilarity] Cluster :");
            System.out.println("[findSimilarity] ---------------------------------");
            for (i = 1; i <= maxElem; i++ ) {                      
                for (j = 1 + i; j <= maxElem; j++ ) {     
                    if (similarityCluster[i][j] == 1){
                        System.out.println("[findSimilarity] i,j: "+i+","+j+"rowid: "+rowidMap.get(i)+ " val: "+getRecordByRowid(connection, verbose, rowidMap.get(i)).toString());
                        System.out.println("[findSimilarity] i,j: "+i+","+j+"rowid: "+rowidMap.get(j)+ " val: "+getRecordByRowid(connection, verbose, rowidMap.get(j)).toString());
                        //System.out.println(recordMap.get(rowidMap.get(i)).toString());
                        //System.out.println(recordMap.get(rowidMap.get(j)).toString());
                    }
                }
                //System.out.println();
            }
        }
        
        if (method.compareTo("OPT")==0) {
            System.out.println("[findSimilarity] ---------------------------------");
            System.out.println("[findSimilarity] Optimized cluster :");
            System.out.println("[findSimilarity] ---------------------------------");
            for (i = 1; i <= maxElem; i++ ) {                      
                for (j = 1 + i; j <= maxElem; j++ ) {     
                    if (similarityClusterOpt[i][j] == 1){
                        System.out.println("[findSimilarity] i,j: "+i+","+j+"rowid: "+rowidMap.get(i)+ " val: "+getRecordByRowid(connection, verbose, rowidMap.get(i)).toString());
                        System.out.println("[findSimilarity] i,j: "+i+","+j+"rowid: "+rowidMap.get(j)+ " val: "+getRecordByRowid(connection, verbose, rowidMap.get(j)).toString());
                        //System.out.println(recordMap.get(rowidMap.get(i)).toString());
                        //System.out.println(recordMap.get(rowidMap.get(j)).toString());
                    }
                }
                //System.out.println();
            }
        }
        System.out.println("[findSimilarity] ---------------------------------");
        System.out.println("[findSimilarity] N. of calc.: "+numeroCalcoli);
        System.out.println("[findSimilarity] ---------------------------------");
        
    }
        
        
        
    public static int [][] findSimilarity(Connection connection) throws SQLException {
  
        boolean verboseLocal = false;
        Enumeration names;
        Enumeration rowids;
        String key;  
        String rid = null; 
        
       
        //rowidMap 
        //rowidMapReverse

        
        // devo popolare una matrice i,j i cui elementi sono il risultato del controllo di similarità tra i record
        // aventi rowid - > i,j  corrispondenti
        // dichiaro la matrice
        
        int i,j;
        int maxElem = rowidMap.size();
        /* FIXME: targetDist è la distanza target su cui classificare, elevarla a variabile perlomeno static #####*/
        int targetDist = 2;
        int similarityBase [][] = new int[maxElem+1][maxElem+1];
        int similarityCluster [][] = new int[maxElem+1][maxElem+1];
        int similarityClusterOpt [][] = new int[maxElem+1][maxElem+1];
        int similarityOptimized [][] = new int[maxElem+1][maxElem+1];
        boolean[][] similarityOptLBCheck = new boolean[maxElem+1][maxElem+1];
        boolean[][] similarityOptUBCheck = new boolean[maxElem+1][maxElem+1];
        for (i = 1; i <= maxElem; i++ ) {            
            for (j = 1 + i; j <= maxElem; j++ ) {
                similarityOptLBCheck [i][j] = false;
            }
        }
        for (i = 1; i <= maxElem; i++ ) {            
            for (j = 1 + i; j <= maxElem; j++ ) {
                similarityOptUBCheck [i][j] = false;
            }
        }
        int counter;
        String rowidA, rowidB;
        Enumeration fieldsA;
        Enumeration fieldsB;
        int dist = 0;
        int maxDist = 0;
        int LBDist = 0;
        boolean calculateDist = true;
        int numeroCalcoli = 0;        
        Boolean verbose = true;
        for (i = 1; i <= maxElem; i++ ) {
            rowidA = rowidMap.get(i);            
            for (j = 1 + i; j <= maxElem; j++ ) {
                rowidB = rowidMap.get(j);
                         
                if(verbose&&verboseLocal) System.out.println("[findSimilarity]-->"+i+"  "+j);                
                if (method.compareTo("OPT")==0){
                    calculateDist = false;
                    //System.out.println("[findSimilarity] Calcolo distanza D");
                    //ottimizzazione (se le distanze della riga 1 sono già state calcolate...
                    if (i>1){ 
                        
                        // caso 1: verifico che d12 non sia approssimato...           
                        if (!similarityOptLBCheck[i-1][j-1]){
                            LBDist = similarityOptimized[i-1][j]-similarityOptimized[i-1][j-1];
                            // verifico se  d23 >= d13-d12>soglia
                            if (LBDist > targetDist && !similarityOptLBCheck [i-1][j-1]){                                
                                similarityOptimized[i][j] = LBDist;
                                //similarityOptimized[j][i] = LBDist; 
                                similarityOptLBCheck [i][j] = true;
                                if ((targetDist - LBDist)>=0){                                
                                    similarityClusterOpt[i][j] = 1;
                                }
                            }
                            else {                                
                                LBDist = similarityOptimized[i-1][j-1]-similarityOptimized[i-1][j];
                                if (LBDist > targetDist && !similarityOptLBCheck [i-1][j]){
                                    similarityOptimized[i][j] = LBDist; 
                                    //similarityOptimized[j][i] = LBDist; 
                                    similarityOptLBCheck [i][j] = true;
                                    if ((targetDist - LBDist)>=0){                                
                                        similarityClusterOpt[i][j] = 1;
                                    }
                                }                                
                                else {
                                    calculateDist = true;                          
                                }                                
                            }
                        }
                        else {
                            if (!similarityOptLBCheck[i-1][j]){
                                LBDist = similarityOptimized[i-1][j-1]-similarityOptimized[i-1][j];
                                if (LBDist > targetDist){
                                    similarityOptimized[i][j] = LBDist; 
                                    //similarityOptimized[j][i] = LBDist; 
                                    similarityOptLBCheck [i][j] = true;
                                    if ((targetDist - LBDist)>=0){                                
                                        similarityClusterOpt[i][j] = 1;
                                    }
                                }
                                else {
                                    calculateDist = true;
                                }
                            }
                        }
                    }
                    else {
                        calculateDist = true;
                    }                
                }                
                if (calculateDist){
                    //System.out.println("confronto campi :  elemA["+i+"]["+rowidA+"], elemB["+j+"]["+rowidB+"]  similarity: "+similarityBase[i][j]);
                    counter = 1;   
                    
                    Hashtable<String, String> hashtableA = Cluster.getRecordByRowid(connection, verbose, rowidA);                
                    Hashtable<String, String> hashtableB = Cluster.getRecordByRowid(connection, verbose, rowidB);
                    maxDist = hashtableA.size();
                    fieldsA = hashtableA.keys();
                    while(fieldsA.hasMoreElements()) {
                        key = (String) fieldsA.nextElement();
                       
                        Object elemA = hashtableA.get(key);
                        Object elemB = hashtableB.get(key);
                        if (elemA.equals(elemB)){
                            dist = maxDist-counter;                            
                            if (method.compareTo("OPT")==0){
                                if (calculateDist){
                                    similarityOptimized[i][j] = dist;
                                    //similarityOptimized[j][i] = dist; 
                                }
                            }
                            else {
                                similarityBase[i][j] = dist;
                                //similarityBase[j][i] = dist;    
                            }                                
                            counter++;
                            if ((targetDist - dist)>=0){                                
                                if (method.compareTo("OPT")==0){
                                    similarityClusterOpt[i][j] = 1;
                                }
                                else {
                                    similarityCluster[i][j] = 1;
                                }
                                    
                            }
                        }
                    }
                    
                    numeroCalcoli++;
                    if(verbose&&verboseLocal) System.out.println("[findSimilarity] CALC=YES");
                }
                else  {
                    if(verbose&&verboseLocal) System.out.println("[findSimilarity] CALC=NO");
                }
            }            
        }   
        
;
        printResults(connection, verbose,  similarityBase, similarityOptimized ,  similarityCluster ,  similarityClusterOpt ,  maxElem,  numeroCalcoli );
        return similarityBase;
    }

    
    public static Hashtable<String, String> getRecordByRowid(Connection connection, Boolean verbose, String rowid) throws SQLException {
        boolean verboseLocal = false;
        String selectTableSQL = "select " +
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
                        ",REASON_CHANGE from ANAGAMB_V_GAU_ARPA " +
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
                System.out.print("["+matrix[i][j]+"]");                                 
            }
            System.out.println();
        }
        System.out.println("=================================");
    }
    

    
        
    /**
    *
    * @author diego
    * metodo di prova per la stampa di tutti i valori del campo ROWID della tabella
    */
    public static void stampaCampiCluster(Connection connection, Boolean verbose) throws SQLException {
        // popolamento indice (hasmmap locale)
        boolean verboseLocal = false;
        ArrayList<String> campiCluster = new ArrayList<String>();
        String selectTableSQL = "select rowid, DENOMINAZIONE from ANAGAMB_V_GAU_ARPA_TEMP";          
        try {
            Statement statement = connection.createStatement();
            if(verbose&&verboseLocal) System.out.println(selectTableSQL);
            // esegue lo statement SQL 
            ResultSet rs = statement.executeQuery(selectTableSQL);
            //inizializzo strutture per indice keyword sentimento (dizionario)...
            //TreeMap<String, Integer> dictKeywordSetWithOccurrences = new TreeMap<String, Integer>();
            while (rs.next()) {
                //String id = rs.getString("ID");
                String rowid = rs.getString("ROWID");
                if (verbose){
                    //System.out.println("id : " + id);
                    if(verbose&&verboseLocal) System.out.println("rowid : " + rowid);
                }                                      
                /*
                implementazione stemmer, non utilizzato:
                Stemmer s = new Stemmer();   
                String stemma = s.stem(word);
                if (verbose){
                    System.out.println("Lemma stemmizzato: "+stemma);            
                    System.out.println("-----------------------------------------");
                    System.out.println("[addLexiconSourcesToDictKeywordSetWithOccurrences]:");
                }*/
                //campiCluster.add(rowid);                 
            }
            //System.out.println("Indice anger da dizionario: " + dict.toString());        
        } catch (SQLException e) {
            System.out.println(e.getMessage());                  
        } finally {      
            if (statement != null) {
                statement.close();
            }
        }         
    }        
        
        
    
    
    
    private static TreeMap<String, Integer> addLexiconSourcesToDictKeywordSetWithOccurrences(TreeMap<String, Integer> dict, Connection connection, String selectTableSQL, Boolean verbose) throws SQLException {               
        boolean verboseLocal = false;
        try {
            Statement statement = connection.createStatement();
            if(verbose&&verboseLocal) System.out.println(selectTableSQL);
            // esegue lo statement SQL 
            ResultSet rs = statement.executeQuery(selectTableSQL);

            //inizializzo strutture per indice keyword sentimento (dizionario)...
            //TreeMap<String, Integer> dictKeywordSetWithOccurrences = new TreeMap<String, Integer>();
            while (rs.next()) {

                String id = rs.getString("ID");
                String word = rs.getString("WORD").toLowerCase();
                if (verbose){
                    if(verbose&&verboseLocal) System.out.println("id : " + id);
                    if(verbose&&verboseLocal) System.out.println("word : " + word);
                }                                      
                /*
                implementazione stemmer, non utilizzato:
                Stemmer s = new Stemmer();   
                String stemma = s.stem(word);
                if (verbose){
                    System.out.println("Lemma stemmizzato: "+stemma);            
                    System.out.println("-----------------------------------------");
                    System.out.println("[addLexiconSourcesToDictKeywordSetWithOccurrences]:");
                }*/
                if (!dict.containsKey(word)) {
                    dict.put(word, 1);
                } 
                else {
                    dict.put(word, dict.get(word) + 1);
                }
            }
            //System.out.println("Indice anger da dizionario: " + dict.toString());        
        } catch (SQLException e) {
            System.out.println(e.getMessage());                  
        } finally {      
            if (statement != null) {
                statement.close();
            }      
        }  
        return dict;
    }
    
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
            
        

    public static ArrayList<String> createCampiCluster(Connection connection, Boolean verbose) throws SQLException {
        // popolamento indice (hasmmap locale)

        ArrayList<String> campiCluster = new ArrayList<String>();
        String selectTableSQL = "select rowid, DENOMINAZIONE from ANAGAMB_V_GAU_ARPA";
        campiCluster = addCampiCluster(campiCluster, connection, selectTableSQL, verbose);               
        
        return campiCluster;
        

    }
    
    
    
    private static ArrayList<String> addCampiCluster(ArrayList<String> campiCluster, Connection connection, String selectTableSQL, Boolean verbose) throws SQLException {               
        try {
            Statement statement = connection.createStatement();
            System.out.println(selectTableSQL);
            // esegue lo statement SQL 
            ResultSet rs = statement.executeQuery(selectTableSQL);
            //inizializzo strutture per indice keyword sentimento (dizionario)...
            //TreeMap<String, Integer> dictKeywordSetWithOccurrences = new TreeMap<String, Integer>();
            while (rs.next()) {
                //String id = rs.getString("ID");
                String rowid = rs.getString("ROWID");
                if (verbose){
                    //System.out.println("id : " + id);
                    System.out.println("rowid : " + rowid);
                }                                      
                /*
                implementazione stemmer, non utilizzato:
                Stemmer s = new Stemmer();   
                String stemma = s.stem(word);
                if (verbose){
                    System.out.println("Lemma stemmizzato: "+stemma);            
                    System.out.println("-----------------------------------------");
                    System.out.println("[addLexiconSourcesToDictKeywordSetWithOccurrences]:");
                }*/
                campiCluster.add(rowid);                 
            }
            //System.out.println("Indice anger da dizionario: " + dict.toString());        
        } catch (SQLException e) {
            System.out.println(e.getMessage());                  
        } finally {      
            if (statement != null) {
                statement.close();
            }      
        }  
        return campiCluster;
    }



    

}
