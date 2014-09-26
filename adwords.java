import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class adwords {

	private static final String INSERT_INTO_SYSTEM_PARAMS = "insert into systemInParams (taskName, taskLimit) values (?, ?)";
	
	private static final String INSERT_INTO_QUERIES = "insert into queries (queryId, queryText) values (?, ?)";
	
	private static final String INSERT_INTO_ADVERTISERS= "insert into advertisers (advertiserId, budget, ctc, hundred_ctc) values (?, ?, ?, ?)";
	
	private static final String INSERT_INTO_KEYWORDS= "insert into keywords (advertiserId, keywords, bid) values (?, ?, ?)";
	
	private static Connection conn = null;
	private static String url = null;
	
	private static String username = null;
	private static String password = null;
	private static final String systemInPath = "system.in";
	private static final String queryFilePath = "Queries.dat";
	private static final String advertiserFilePath = "Advertisers.dat";
	private static final String keywordsFilePath = "Keywords.dat";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		try{
		System.out.println("Reading system parameters\n");

		try{
			readSystemParameters(systemInPath);
		}catch(Exception e){
				e.printStackTrace();
		}
		
		System.out.println("Starting with Queries file\n");

		try{
			readFileAndInsertQueries(queryFilePath);
		}catch(Exception e){
			e.printStackTrace();
		}
		try{
			readFileAndInsertAdvertisers(advertiserFilePath);
		}catch(Exception e){
			e.printStackTrace();
		}
		try{
			readFileAndInsertKeywords(keywordsFilePath);
		}catch(Exception e){
			e.printStackTrace();
		}
		try{
			invokeSQLScript();
		}catch(Exception e){
			e.printStackTrace();
		}
		
		writeOutput("system.out.1", "greedy_first_output");
		writeOutput("system.out.2", "greedy_second_output");
		writeOutput("system.out.3", "balance_first_output");
		writeOutput("system.out.4", "balance_second_output");
		writeOutput("system.out.5", "generalized_first_output");
		writeOutput("system.out.6", "generalized_second_output");
		
		}catch(Exception e){
			
			e.printStackTrace();
			
		}

	}
	
	private static void readSystemParameters(String filePath){

		BufferedReader reader;
		try {
			FileInputStream fs = new FileInputStream(filePath);
			reader = new BufferedReader(new InputStreamReader(fs));
			
			String line = null;
			String firstWord = null;
			String secondWord = null;
		
			while ((line = reader.readLine()) != null) {
				
				String wordArray[] = line.split("=");
				firstWord = wordArray[0];
				secondWord = wordArray[1];
				
				firstWord = firstWord.trim();
				secondWord = secondWord.trim();
				
				if(firstWord.equalsIgnoreCase("username")){
					
					username = secondWord;
					
				}else if(firstWord.equalsIgnoreCase("password")){
					
					password = secondWord;
					
					System.out.println("Creating necessary tables\n");
					url = "jdbc:oracle:thin:" +username +"@//oracle.cise.ufl.edu:1521/orcl";
					createDatabaseConnection(username, password);
					createTables();
										
				}else{
									
					String firstWordArray[] = firstWord.split(":");
					firstWordArray[0] = firstWordArray[0].trim();
					firstWordArray[1] = firstWordArray[1].trim();
					
					insertSystemInParams(firstWordArray[0], Integer.parseInt(secondWord));
										
				}
				
			}
			
			reader.close();
		}catch (Exception e) {
			e.printStackTrace();
		} 

	}
	
	private static void insertSystemInParams(String taskName, int topTasks){
		
		try{
			
			PreparedStatement ps = createPreparedStatement(INSERT_INTO_SYSTEM_PARAMS);
			ps.setString(1, taskName);
			ps.setInt(2, topTasks);
			ps.executeUpdate();
			
			commit();
			ps.close();
			
		}catch(SQLException e){
			
			rollback();
			e.printStackTrace();
			
		}catch(Exception e){
			
			e.printStackTrace();
			
		}
		
	}
	private static void readFileAndInsertQueries(String filePath) {
				
		BufferedReader reader;
		try {
			PreparedStatement ps = null;
			reader = new BufferedReader(new FileReader(filePath));

			String line = null;
			String firstWord = new String();
			String secondWord = new String();
			
			while ((line = reader.readLine()) != null) {
				
				String wordArray[] = line.split("\\s+");
				
				firstWord = wordArray[0];
				StringBuffer builder = new StringBuffer();

				for (int i=1; i< wordArray.length; i++) {
				  
					String s = wordArray[i];
					if (builder.length() > 0) {
				        builder.append(" ");
				    }
				    builder.append(s);
				}

				secondWord = builder.toString();

				ps = createPreparedStatement(INSERT_INTO_QUERIES);
				ps.setInt(1, Integer.parseInt(firstWord));
				ps.setString(2, secondWord);
				
				ps.executeUpdate();
				commit();
				ps.close();
			}
			reader.close();
		}catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			rollback();
			e.printStackTrace();
		}
		
	}


	private static void readFileAndInsertAdvertisers(String filePath){
		
		BufferedReader reader;
		try {
			PreparedStatement ps = null;
			reader = new BufferedReader(new FileReader(filePath));

			String line = null;
			
			while ((line = reader.readLine()) != null) {
				
				String wordArray[] = line.split("\\s+");
				
				ps = createPreparedStatement(INSERT_INTO_ADVERTISERS);
				
				ps.setInt(1, Integer.parseInt(wordArray[0]));
				ps.setFloat(2, Float.parseFloat(wordArray[1]));
				ps.setFloat(3, Float.parseFloat(wordArray[2]));
				ps.setFloat(4, (int)(100 * Float.parseFloat(wordArray[2])));
				
				ps.executeUpdate();
				commit();
				ps.close();
			}
			reader.close();
		}catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {

			rollback();
			e.printStackTrace();
		}
		
		
	}
	
	private static void readFileAndInsertKeywords(String filePath){
		
		BufferedReader reader;
		try {
			PreparedStatement ps = null;
			reader = new BufferedReader(new FileReader(filePath));

			String line = null;
			
			while ((line = reader.readLine()) != null) {
				
				String wordArray[] = line.split("\\s+");
				
				ps = createPreparedStatement(INSERT_INTO_KEYWORDS);
				
				ps.setInt(1, Integer.parseInt(wordArray[0]));
				ps.setString(2, wordArray[1]);
				ps.setFloat(3, Float.parseFloat(wordArray[2]));
				
				ps.executeUpdate();
				commit();
				ps.close();
			}
			reader.close();
		}catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			rollback();
			e.printStackTrace();
		}
		
		
	}
	
	private static void writeOutput(String outputFilename, String tableName){
		
		BufferedWriter writer;
		StringBuffer finalString = null;
		int queryId, rank, advertiserId;
		float balance, budget;
		try{
			
			writer = new BufferedWriter(new FileWriter(outputFilename));
						
			PreparedStatement ps = createPreparedStatement("select * from " + tableName + " order by queryid, rank ASC");
			ResultSet rs = ps.executeQuery();
			
			while(rs.next()){
				
				finalString = new StringBuffer();
				
				queryId = rs.getInt("queryId");
				rank = rs.getInt("rank");
				advertiserId = rs.getInt("advertiserId");
				balance = rs.getFloat("balance");
				budget = rs.getFloat("budget");
				
				finalString = finalString.append(queryId);
				finalString = finalString.append(", ");
				finalString = finalString.append(rank);
				finalString = finalString.append(", ");
				finalString = finalString.append(advertiserId);
				finalString = finalString.append(", ");
				finalString = finalString.append(balance);
				finalString = finalString.append(", ");
				finalString = finalString.append(budget);
				finalString = finalString.append("\n");
				
				writer.append(finalString);
				finalString = null;
			}
			writer.close();
		} catch (SQLException e) {
			rollback();
			e.printStackTrace();
		} catch(Exception e){
			e.printStackTrace();			
		}		
		
	}
	
	private static void invokeSQLScript(){
		
		try{
			// RUN THE PLSQL PROCEDURE
			Process p = Runtime.getRuntime().exec("sqlplus " + username +"@orcl/"+ password + " @adwords.sql");
			p.waitFor();
			System.out.println("Procedure ran successfully. All tasks completed");
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	public static void createDatabaseConnection(String username, String password) throws ClassNotFoundException{

		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");

			/**
			 * This is a singleton instance of 
			 * database connection
			 */			
			if(conn == null){			
				conn = DriverManager.getConnection(
						url, username, password);
				conn.setAutoCommit(false);			
			}

		}catch(SQLException e){
			rollback();
			e.printStackTrace();
		}	

	}
	
	public static PreparedStatement createPreparedStatement(String query) throws SQLException{

		PreparedStatement ps = conn.prepareStatement(query);
		return ps;
	}

	public static void commit() throws SQLException{
		if(conn !=null){
			conn.commit();
		}
	}

	public static void rollback(){
		try {

			if(conn != null){
				conn.rollback();
			}

		} catch (SQLException e) {

			e.printStackTrace();
		}		
	}

	public static void createTables(){
		
		try{			
			PreparedStatement ps = conn.prepareStatement(
					"create table queries (queryId int PRIMARY KEY, queryText varchar2(310) NOT NULL)");

			ps.executeUpdate();
			commit();
			ps.close();

			ps = conn.prepareStatement(
					"create table systemInParams (taskName varchar2(10), taskLimit int, PRIMARY KEY(taskName))");
			ps.executeUpdate();
			commit();			
			ps.close();

			ps = conn.prepareStatement(
					"create table advertisers (advertiserId int PRIMARY KEY, budget float NOT NULL, ctc float NOT NULL, hundred_ctc float NOT NULL)");

			ps.executeUpdate();
			commit();
			ps.close();

			ps = conn.prepareStatement(
					"create table keywords (advertiserId int, keywords varchar2(100), bid float NOT NULL, PRIMARY KEY(advertiserId, keywords))");

			ps.executeUpdate();
			commit();
			ps.close();

		}catch(Exception e){
			rollback();
			e.printStackTrace();			

		}
	}

	
	
	
}
