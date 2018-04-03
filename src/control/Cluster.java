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

    
    

    
        /**
    *
    * @author diego
    * metodo di prova per la stampa di tutti i valori del campo ROWID della tabella
    */
    public static void stampaCampiSorgente(Connection connection, Boolean verbose) throws SQLException {        
        
        String selectTableSQL = "select * from ANAGAMB_V_GAU_ARPA_TEMP";         
        try {
            Statement statement = connection.createStatement();
            System.out.println(selectTableSQL);
            
            // esegue lo statement SQL 
            ResultSet rs = statement.executeQuery(selectTableSQL);
            
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            // L'indice della prima colonna è 1
            for (int i = 1; i <= columnCount; i++ ) {
                String name = rsmd.getColumnName(i);
                System.out.println("campo : " + i + ", nome: "+name);
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
    public static Hashtable<String, Hashtable<String, String>> popolaMappaRecord(Connection connection, Boolean verbose) throws SQLException {
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
                        ",REASON_CHANGE from ANAGAMB_V_GAU_ARPA_TEMP " +
                        "where rownum <= 1000                              ";  

        Enumeration names;
        Enumeration rowids;
        String key;  
        String rid = null;        
        Hashtable<String, Hashtable<String, String>> recordMap = new Hashtable<String, Hashtable<String, String>>();      
        rowidMap = new Hashtable< Integer , String>();
        rowidMapReverse = new Hashtable< String, Integer>();            
        try {
            Statement statement = connection.createStatement();
            System.out.println(selectTableSQL);            
            // esegue lo statement SQL 
            ResultSet rs = statement.executeQuery(selectTableSQL);            
            // recupera i metadati
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            System.out.println("Numero colonne:"+columnCount);                 
            Integer counter=0;     
            while (rs.next()) {                
                Hashtable<String, String> hashtable = new Hashtable<String, String>();
                if (verbose){  
                    System.out.println("---------------------------------");
                    System.out.println("1.1 memorizzazione record in hashtable");
                } 
                for (int i = 2; i <= columnCount; i++ ) {
                    String name = rsmd.getColumnName(i);
                    if (verbose){                        
                        System.out.println("memorizzo in hashtable campo : " + i + ", nome: "+name);
                    }                  
                    String value = rs.getString(name);
                    if (value!=null)
                        hashtable.put(name,value);
                    else 
                        hashtable.put(name,"NULL_VALUE");                                                                   
                }
                rid = rs.getString("ROWID");
                counter++;
                System.out.println("counter: "+counter+" - rid: "+rid);
                rowidMap.put(counter, rid);
                rowidMapReverse.put(rid, counter);                
                names = hashtable.keys();
                if (verbose){  
                    System.out.println("---------------------------------");
                    System.out.println("1.2 stampa record memorizzato in hashtable");
                } 
                while(names.hasMoreElements()) {
                    key = (String) names.nextElement();
                    if (verbose){  
                        System.out.println("chiave: " +key+ "  valore: " + hashtable.get(key));
                    }                    
                }                                
                if (verbose){                        
                    System.out.println("memorizzo in hashtableMatrix record "+rid);
                }                  
                if (rid!=null) {
                    recordMap.put(rid,hashtable);
                }
                else {
                    System.out.println("[Cluster] Errore: rowid nullo!");
                }                                                                          
            }
            rowids = recordMap.keys();
            if (verbose){  
                System.out.println("---------------------------------");
                System.out.println("1.3 stampa contenuto hashtableMatrix");
            } 
            while(rowids.hasMoreElements()) {
                //rowid = (RowId) rowids.nextElement();
                rid = (String) rowids.nextElement();
                if (verbose){  
                    //System.out.println("chiave: " +rowid.toString()+ "  valore: " + hashtableMatrix.get(rowid).toString());
                    System.out.println("chiave: " +rid.toString()+ "  valore: " + recordMap.get(rid).toString());
                } 
            }            
            Enumeration generic = rowidMap.keys();
            Integer genericInteger;
            if (verbose){  
                System.out.println("---------------------------------");
                System.out.println("1.4 stampa contenuto mapRowid");
            } 
            while(generic.hasMoreElements()) {
                //rowid = (RowId) rowids.nextElement();
                genericInteger = (Integer) generic.nextElement();
                if (verbose){  
                    //System.out.println("chiave: " +rowid.toString()+ "  valore: " + hashtableMatrix.get(rowid).toString());
                    System.out.println("chiave: " +genericInteger.toString()+ "  valore: " + rowidMap.get(genericInteger).toString());
                } 
            }                        
            generic = rowidMapReverse.keys();
            String genericString;
            if (verbose){  
                System.out.println("---------------------------------");
                System.out.println("1.5 stampa contenuto mapRowidReverse");
            } 
            while(generic.hasMoreElements()) {
                //rowid = (RowId) rowids.nextElement();
                genericString = (String) generic.nextElement();
                if (verbose){                      
                    System.out.println("chiave: " +genericString+ "  valore: " + rowidMapReverse.get(genericString).toString());
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
  
    
/*
    riceve in ingresso recordMap e misura la similarità tra gli elemenenti in essa contenuti

    */
      
        public static int [][] findSimilarity(Hashtable<String, Hashtable<String, String>> recordMap) throws SQLException {
  
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
        String method = "OPT";
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
                System.out.println("-->"+i+"  "+j);                
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
        printMatrix(similarityBase, maxElem, "BASE, NON OTTIMIZZATA");
        printMatrix(similarityOptimized, maxElem, "OTTIMIZZATA");
        printMatrix(similarityCluster, maxElem, "CLUSTER BASE");
        printMatrix(similarityClusterOpt, maxElem, "CLUSTER OTTIMIZZATA");
        
        System.out.println("---------------------------------");
        System.out.println("Cluster risultato:");
        System.out.println("---------------------------------");
        for (i = 1; i <= maxElem; i++ ) {                      
            for (j = 1 + i; j <= maxElem; j++ ) {     
                if (similarityCluster[i][j] == 1){
                    System.out.println("i,j: "+i+","+j+"rowid: "+rowidMap.get(i)+ " val: "+recordMap.get(rowidMap.get(i)).toString());
                    System.out.println("i,j: "+i+","+j+"rowid: "+rowidMap.get(j)+ " val: "+recordMap.get(rowidMap.get(j)).toString());
                    //System.out.println(recordMap.get(rowidMap.get(i)).toString());
                    //System.out.println(recordMap.get(rowidMap.get(j)).toString());
                }
            }
            //System.out.println();
        }
        System.out.println("---------------------------------");
        System.out.println("Cluster ottimizzato:");
        System.out.println("---------------------------------");
        for (i = 1; i <= maxElem; i++ ) {                      
            for (j = 1 + i; j <= maxElem; j++ ) {     
                if (similarityClusterOpt[i][j] == 1){
                    System.out.println("i,j: "+i+","+j+"rowid: "+rowidMap.get(i)+ " val: "+recordMap.get(rowidMap.get(i)).toString());
                    System.out.println("i,j: "+i+","+j+"rowid: "+rowidMap.get(j)+ " val: "+recordMap.get(rowidMap.get(j)).toString());
                    //System.out.println(recordMap.get(rowidMap.get(i)).toString());
                    //System.out.println(recordMap.get(rowidMap.get(j)).toString());
                }
            }
            //System.out.println();
        }
        System.out.println("---------------------------------");
        System.out.println("Numero calcoli: "+numeroCalcoli);
        System.out.println("---------------------------------");
        return similarityBase;
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
        ArrayList<String> campiCluster = new ArrayList<String>();
        String selectTableSQL = "select rowid, DENOMINAZIONE from ANAGAMB_V_GAU_ARPA_TEMP";                  
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
        try {

            Statement statement = connection.createStatement();
            System.out.println(selectTableSQL);
            // esegue lo statement SQL 
            ResultSet rs = statement.executeQuery(selectTableSQL);

            //inizializzo strutture per indice keyword sentimento (dizionario)...
            //TreeMap<String, Integer> dictKeywordSetWithOccurrences = new TreeMap<String, Integer>();
            while (rs.next()) {

                String id = rs.getString("ID");
                String word = rs.getString("WORD").toLowerCase();
                if (verbose){
                    System.out.println("id : " + id);
                    System.out.println("word : " + word);
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
