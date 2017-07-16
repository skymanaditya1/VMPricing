package com.vmware.agarwaladity;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.util.*;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

class Attribute{
	public String name;
	public String type;
	public String property;
	
	public Attribute() {
		this.name = "";
		this.type = "";
		this.property = "";
	}
	
	public Attribute(String name, String type, String property) {
		this.name = name;
		this.type = type;
		this.property = property;
	}
}

class Data{
	public String name;
	public String value;
	
	public Data() {
		this.name = "";
		this.value = "";
	}
	
	public Data(String name, String value) {
		this.name = name;
		this.value = value;
	}
}

class DatabaseLoader{
	
	// Global configuration params
	public static final String CSV_FILE = "/Users/agarwaladity/Desktop/Dataset3/";
	public static final String DB_NAME = "VMRecord";
	public static final String DB_USER = "postgres";
	public static final String DB_PASS = "helloaditya123";
	
	public void createTable(String tableName, ArrayList<Attribute> attributes) {
		Connection connection = null;
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + DB_NAME, DB_USER, DB_PASS);
			Statement statement = connection.createStatement();
			String query = "CREATE TABLE " + tableName + "(";
			for(int i=0; i<attributes.size()-1; i++) {
				query += attributes.get(i).name + " " + attributes.get(i).type + " " + attributes.get(i).property + ", ";
			}
			query += attributes.get(attributes.size()-1).name + " " + attributes.get(attributes.size()-1).type + " " + attributes.get(attributes.size()-1).property + ")";
			System.out.println("Query generated : " + query);
			statement.executeUpdate(query);
			statement.close();
			connection.close();
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(tableName + " created successfully");
	}
	
	public void insertTable(String csvFile, String tableName, ArrayList<Attribute> attributes) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(CSV_FILE + csvFile));
			reader.readLine();
			String line = "";
			ArrayList<String[]> lines = new ArrayList<String[]>();
			String query = "INSERT INTO " + tableName + " (";
			for(int i=0; i<attributes.size()-1; i++) {
				query = query + attributes.get(i).name + ", ";
			}
			query += attributes.get(attributes.size()-1).name + ") VALUES ";
			while((line=reader.readLine())!=null) {
				String[] currentLine = line.split(",");
				lines.add(currentLine);
			}
			for(int i=0; i<lines.size()-1; i++) {
				query += "(";
				for(int j=0; j<lines.get(i).length-1; j++) {
					if(attributes.get(j).type.equals("text"))
						query += "'" + lines.get(i)[j] + "', ";
					else
						query += lines.get(i)[j] + ", ";
				}
				// add the last data value in the row
				if(attributes.get(lines.get(i).length-1).type.equals("text"))
					query += "'" + lines.get(i)[lines.get(i).length-1] + "'), ";
				else
					query += lines.get(i)[lines.get(i).length-1] + "), ";
			}
			// add the last row into the table
			query += "(";
			for(int j=0; j<lines.get(lines.size()-1).length-1; j++) {
				if(attributes.get(j).type.equals("text"))
					query += "'" + lines.get(lines.size()-1)[j] + "', ";
				else
					query += lines.get(lines.size()-1)[j] + ", ";
			}
			// add the last data value of the last row
			if(attributes.get(lines.get(lines.size()-1).length-1).type.equals("text"))
				query += "'" + lines.get(lines.size()-1)[lines.get(lines.size()-1).length-1] + "')";
			else
				query += lines.get(lines.size()-1)[lines.get(lines.size()-1).length-1] + ")";
			System.out.println("The query is : " + query);
			
			Connection connection = null;
			
			Class.forName("org.postgresql.Driver");
	         connection = DriverManager
	            .getConnection("jdbc:postgresql://localhost:5432/VMRecord",
	            "postgres", "helloaditya123");
	         connection.setAutoCommit(false);
	         
	         Statement statement = connection.createStatement();
	         statement.executeUpdate(query);
	         connection.commit();
	         connection.close();
			
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

public class DatabaseUtility {
	
	public void createDB() {
		
		ArrayList<Attribute> VM = new ArrayList<Attribute>();
		VM.add(new Attribute("vm_id", "int", "primary key"));
		VM.add(new Attribute("vm_name", "text", "not null"));
		
		ArrayList<Attribute> Utilization = new ArrayList<Attribute>();
		Utilization.add(new Attribute("vm_id", "int", ""));
		Utilization.add(new Attribute("resource_type", "text", "not null"));
		Utilization.add(new Attribute("units", "float", ""));
		Utilization.add(new Attribute("start", "bigint", ""));
		Utilization.add(new Attribute("ends", "bigint", ""));
		
		ArrayList<Attribute> Allocation = new ArrayList<Attribute>();
		Allocation.add(new Attribute("vm_id", "int", ""));
		Allocation.add(new Attribute("resource_type", "text", "not null"));
		Allocation.add(new Attribute("units", "float", ""));
		Allocation.add(new Attribute("start", "bigint", ""));
		Allocation.add(new Attribute("ends", "bigint", ""));
		
		ArrayList<Attribute> Property = new ArrayList<Attribute>();
		Property.add(new Attribute("vm_id", "int", ""));
		Property.add(new Attribute("property", "text", "not null"));
		Property.add(new Attribute("value", "text", ""));
		Property.add(new Attribute("start", "bigint", ""));
		Property.add(new Attribute("ends", "bigint", ""));
		
		ArrayList<Attribute> Power = new ArrayList<Attribute>();
		Power.add(new Attribute("vm_id", "int", ""));
		Power.add(new Attribute("event", "boolean", ""));
		Power.add(new Attribute("start", "bigint", ""));
		Power.add(new Attribute("ends", "bigint", ""));
		
		ArrayList<Attribute> VMHost = new ArrayList<Attribute>();
		VMHost.add(new Attribute("vm_id", "int", ""));
		VMHost.add(new Attribute("host_id", "int", ""));
		VMHost.add(new Attribute("start", "bigint", ""));
		VMHost.add(new Attribute("ends", "bigint", ""));
		
		ArrayList<Attribute> HostCluster = new ArrayList<Attribute>();
		HostCluster.add(new Attribute("host_id", "int", ""));
		HostCluster.add(new Attribute("cluster_id", "int", ""));
		HostCluster.add(new Attribute("start", "bigint", ""));
		HostCluster.add(new Attribute("ends", "bigint", ""));
		
		ArrayList<Attribute> Pricing = new ArrayList<Attribute>();
		Pricing.add(new Attribute("cluster_id", "int", ""));
		Pricing.add(new Attribute("resource_type", "text", ""));
		Pricing.add(new Attribute("price", "float", ""));
		Pricing.add(new Attribute("start", "bigint", ""));
		Pricing.add(new Attribute("ends", "bigint", ""));
		
		ArrayList<Attribute> PropertyPricing = new ArrayList<Attribute>();
		PropertyPricing.add(new Attribute("property_name", "text", ""));
		PropertyPricing.add(new Attribute("property_value", "text", ""));
		PropertyPricing.add(new Attribute("price", "float", ""));
		
		/**
		DatabaseLoader loader = new DatabaseLoader();
		loader.createTable("VM", VM);
		loader.createTable("Utilization", Utilization);
		loader.createTable("Allocation", Allocation);
		loader.createTable("Property", Property);
		loader.createTable("Power", Power);
		loader.createTable("VMHost", VMHost);
		loader.createTable("HostCluster", HostCluster);
		loader.createTable("Pricing", Pricing);
		loader.createTable("PropertyPricing", PropertyPricing);*/
		
		// DatabaseLoader loader = new DatabaseLoader();
		// loader.insertTable("F1.csv", "VM", VM);
		// loader.insertTable("F2.csv", "Utilization", Utilization);
		// loader.insertTable("F3.csv", "Allocation", Allocation);
		// loader.insertTable("F4.csv", "Property", Property);
		// loader.insertTable("F5.csv", "Power", Power);
		// loader.insertTable("F6.csv", "VMHost", VMHost);
		// loader.insertTable("F7.csv", "HostCluster", HostCluster);
		// loader.insertTable("F8.csv", "Pricing", Pricing);
		// loader.insertTable("F9.csv", "PropertyPricing", PropertyPricing);
	}
}
