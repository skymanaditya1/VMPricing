package com.vmware.agarwaladity;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.util.*;

import java.sql.DriverManager;
import java.sql.ResultSet;
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
	
	// Method to compute VM prices from the Database
	public void computeVMPrices() {
		HashMap<Integer, ArrayList<Resource>> clusterVMMapping = createClusterVMMapping();
		// printVMClusterMapping(clusterVMMapping);
		ArrayList<VM> vmPrices = calculateVMPrices(clusterVMMapping);
		printVMPrices(vmPrices);
	}
	
	// method to compute the VM prices
	public ArrayList<VM> calculateVMPrices(HashMap<Integer, ArrayList<Resource>> vmCluster){
		ArrayList<VM> vmPrices = new ArrayList<VM>();
		Connection connection = null;
		
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager
		            .getConnection("jdbc:postgresql://localhost:5432/VMRecord",
		            "postgres", "helloaditya123");
			connection.setAutoCommit(false);
			Statement statement = connection.createStatement();
			String query = "SELECT VM_ID FROM VM";
			ResultSet resultSet = statement.executeQuery(query);
			while(resultSet.next()) {
				// get the utilization prices corresponding to the vm_id
				float utilizationPrice = 0;
				int vm_id = resultSet.getInt(1);
				Statement statement1 = connection.createStatement();
				String query1 = "SELECT Resource_Type, Units, Start, Ends FROM Utilization WHERE VM_ID = " + resultSet.getInt(1);
				ResultSet resultSet1 = statement1.executeQuery(query1);
				while(resultSet1.next()) {
					long initialStart = resultSet1.getLong(3);
					long initialEnd = resultSet1.getLong(4);
					while(initialStart < initialEnd) {
						long tempStart = initialStart;
						long tempEnd = initialEnd;
						// find the cluster id corresponding to the vm
						int i = 0;
						int cluster_id = -1;
						while(i<vmCluster.size() && min(vmCluster.get(vm_id).get(i).end, tempEnd) - max(vmCluster.get(vm_id).get(i).start, tempStart) <= 0)
							i += 1;
						cluster_id = vmCluster.get(vm_id).get(i).id;
						tempStart = max(tempStart, vmCluster.get(vm_id).get(i).start);
						tempEnd = min(tempEnd, vmCluster.get(vm_id).get(i).end);
						
						// Find out the pricing and the time slab for the given cluster
						Statement statement2 = connection.createStatement();
						String query2 = "SELECT Price, Start, Ends FROM Pricing WHERE Cluster_ID = " + cluster_id + " AND Resource_Type LIKE '" + resultSet1.getString(1) + "'";
						ResultSet resultSet2 = statement2.executeQuery(query2);
						while(resultSet2.next()) {
							if(min(resultSet2.getLong(3), tempEnd) - max(resultSet2.getLong(2), tempStart) > 0) {
								// update the tempStart and tempEnd timings and compute the intermediate utilization price
								tempStart = max(resultSet2.getLong(2), tempStart);
								tempEnd = min(resultSet2.getLong(3), tempEnd);
								// price = price + (time units) * (price per unit) * (number of units)
								utilizationPrice = utilizationPrice + (tempEnd - tempStart) * resultSet2.getFloat(1) * resultSet1.getFloat(2);
								// System.out.println("The intermediate Utilization Price is : " + utilizationPrice);
							}
						}
						resultSet2.close();
						statement2.close();
						// update the initialStart time
						initialStart = tempEnd;
					}
				}
				resultSet1.close();
				statement1.close();
				// System.out.println("Utilization Price : " + utilizationPrice);
				
				// get the allocation price corresponding to the vm_id
				float allocationPrice = 0;
				statement1 = connection.createStatement();
				query1 = "SELECT Resource_Type, Units, Start, Ends FROM Allocation WHERE VM_ID = " + vm_id;
				resultSet1 = statement1.executeQuery(query1);
				while(resultSet1.next()) {
					// find the cluster id to which the VM belongs from the vmCluster map
					long initialStart = resultSet1.getLong(3);
					long initialEnd = resultSet1.getLong(4);
					while(initialStart < initialEnd) {
						long tempStart = initialStart;
						long tempEnd = initialEnd;
						int cluster_id = -1;
						int i = 0;
						while(i<vmCluster.size() && min(tempEnd, vmCluster.get(vm_id).get(i).end) - max(tempStart, vmCluster.get(vm_id).get(i).start) <= 0)
							i += 1;
						cluster_id = vmCluster.get(vm_id).get(i).id;
						tempStart = max(tempStart, vmCluster.get(vm_id).get(i).start);
						tempEnd = min(tempEnd, vmCluster.get(vm_id).get(i).end);
						
						// Find out the pricing slab for the given cluster, resource_type and time interval
						Statement statement2 = connection.createStatement();
						String query2 = "SELECT Price, Start, Ends FROM Pricing WHERE Cluster_Id = " + cluster_id + " AND Resource_Type LIKE '" + resultSet1.getString(1) + "'";
						ResultSet resultSet2 = statement2.executeQuery(query2);
						while(resultSet2.next()) {
							if(min(tempEnd, resultSet2.getLong(3)) - max(tempStart, resultSet2.getLong(2)) > 0) {
								// update tempStart, tempEnd and compute intermediate allocation price
								tempStart = max(tempStart, resultSet2.getLong(2));
								tempEnd = min(tempEnd, resultSet2.getLong(3));
								// price = price + (timeEnd - timeStart) * units * price_per_unit
								allocationPrice = allocationPrice + (tempEnd - tempStart) * resultSet1.getFloat(2) * resultSet2.getFloat(1);
								// System.out.println("Intermediate Allocation Price : " + allocationPrice);
							}
						}
						resultSet2.close();
						statement2.close();
						
						// update the initialStart
						initialStart = tempEnd;
					}
				}
				resultSet1.close();
				statement1.close();
				
				// System.out.println("Allocation Price : " + allocationPrice);
				
				// compute the property prices
				float propertyPrice = 0;
				statement1 = connection.createStatement();
				query1 = "SELECT Property, Value, Start, Ends FROM Property WHERE VM_ID = " + resultSet.getInt(1);
				resultSet1 = statement1.executeQuery(query1);
				while(resultSet1.next()) {
					// compute the property price
					Statement statement2 = connection.createStatement();
					String query2 = "SELECT Price FROM PropertyPricing WHERE Property_Name LIKE '" + resultSet1.getString(1) + "' AND Property_Value LIKE '" + resultSet1.getString(2) + "'";
					ResultSet resultSet2 = statement2.executeQuery(query2);
					while(resultSet2.next()) {
						// price = price + (time duration) * (price per unit) 
						propertyPrice = propertyPrice + (resultSet1.getLong(4) - resultSet1.getLong(3)) * resultSet2.getFloat(1);
						// System.out.println("Intermediate Property Price : " + propertyPrice);
					}
					resultSet2.close();
					statement2.close();
				}
				resultSet1.close();
				statement1.close();
				
				// System.out.println("Property Price : " + propertyPrice);
				vmPrices.add(new VM(vm_id, utilizationPrice, allocationPrice, propertyPrice));
			}
			resultSet.close();
			statement.close();
			connection.close();
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return vmPrices;
	}
	
	public void printVMPrices(ArrayList<VM> vmPrices) {
		for(int i=0; i<vmPrices.size(); i++) {
			System.out.println("VM ID : " + vmPrices.get(i).id + ", Utilization Price : " + vmPrices.get(i).utilizationPrice + 
					", Allocation Price : " + vmPrices.get(i).allocationPrice + ", Property Price : " + vmPrices.get(i).propertyPrice);
		}
	}
	
	// method to print the hosts
	public void printHosts(ArrayList<Resource> hosts) {
		System.out.println(hosts.size());
		for(int i=0; i<hosts.size(); i++) {
			System.out.println(hosts.get(i).id + ", " + hosts.get(i).start + ", " + hosts.get(i).end);
		}
		System.out.println();
	}
	
	// method to print VM Cluster mapping
	public void printVMClusterMapping(HashMap<Integer, ArrayList<Resource>> vmCluster) {
		for(Integer vm : vmCluster.keySet()) {
			System.out.println(vm);
			ArrayList<Resource> clusters = vmCluster.get(vm);
			for(int i=0; i<clusters.size(); i++) {
				System.out.println(clusters.get(i).id + ", " + clusters.get(i).start + " : " + clusters.get(i).end);
			}
		}
	}
	
	// method to create cluster VM Mapping
	public HashMap<Integer, ArrayList<Resource>> createClusterVMMapping(){
		HashMap<Integer, ArrayList<Resource>> vmCluster = new HashMap<Integer, ArrayList<Resource>>();
		Connection connection = null;
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager
			           .getConnection("jdbc:postgresql://localhost:5432/VMRecord",
			           "postgres", "helloaditya123");
			connection.setAutoCommit(false);
			Statement statement = connection.createStatement();
			String query = "SELECT VM_ID FROM VM";
			ResultSet resultSet = statement.executeQuery(query);
			while(resultSet.next()) {
				// find out the hosts and time intervals to which VMs belong
				ArrayList<Resource> hosts = new ArrayList<Resource>();
				String query1 = "SELECT Host_ID, Start, Ends FROM VMHost WHERE VM_ID = " + Integer.parseInt(resultSet.getString(1));
				Statement statement1 = connection.createStatement();
				ResultSet resultSet1 = statement1.executeQuery(query1);
				while(resultSet1.next()) {
					hosts.add(new Resource(resultSet1.getInt(1), resultSet1.getLong(2), resultSet1.getLong(3)));
				}
				// print the hosts chosen
				// System.out.println(resultSet.getInt(1));
				// printHosts(hosts);
				statement1.close();
				resultSet1.close();
				
				ArrayList<Resource> clusters = new ArrayList<Resource>();
				// create mapping between hosts and cluster
				for(Resource host : hosts) {
					String query2 = "SELECT Host_ID, Cluster_ID, Start, Ends FROM HostCluster WHERE Host_ID = " + host.id;
					Statement statement2 = connection.createStatement();
					ResultSet resultSet2 = statement2.executeQuery(query2);
					while(resultSet2.next()) {
						if(min(host.end, resultSet2.getLong(4)) - max(host.start, resultSet2.getLong(3)) > 0) {
							clusters.add(new Resource(resultSet2.getInt(2), max(host.start, resultSet2.getLong(3)), min(host.end, resultSet2.getLong(4))));
						}
					}
					
					resultSet2.close();
					statement2.close();
				}
				
				vmCluster.put(resultSet.getInt(1), clusters);
			}
			statement.close();
			connection.close();
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return vmCluster;
	}
	
	// function to calculate the min of two long numbers
	public long min(long number1, long number2) {
		return number1 < number2 ? number1 : number2;
	}
	
	// function to calculate the max of two long numbers
	public long max(long number1, long number2) {
		return number1 > number2 ? number1 : number2;
	}
	
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
		
		
		DatabaseLoader loader = new DatabaseLoader();
		/**loader.createTable("VM", VM);
		loader.createTable("Utilization", Utilization);
		loader.createTable("Allocation", Allocation);
		loader.createTable("Property", Property);
		loader.createTable("Power", Power);
		loader.createTable("VMHost", VMHost);
		loader.createTable("HostCluster", HostCluster);
		loader.createTable("Pricing", Pricing);*/
		loader.createTable("PropertyPricing", PropertyPricing);
		
		// DatabaseLoader loader = new DatabaseLoader();
		// loader.insertTable("F1.csv", "VM", VM);
		// loader.insertTable("F2.csv", "Utilization", Utilization);
		// loader.insertTable("F3.csv", "Allocation", Allocation);
		// loader.insertTable("F4.csv", "Property", Property);
		// loader.insertTable("F5.csv", "Power", Power);
		// loader.insertTable("F6.csv", "VMHost", VMHost);
		// loader.insertTable("F7.csv", "HostCluster", HostCluster);
		// loader.insertTable("F8.csv", "Pricing", Pricing);
		loader.insertTable("F9.csv", "PropertyPricing", PropertyPricing);
	}
}
