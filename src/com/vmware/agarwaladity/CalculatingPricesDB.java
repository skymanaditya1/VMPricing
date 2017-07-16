package com.vmware.agarwaladity;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

class Resource {
	public int id;
	public long start;
	public long end;
	
	public Resource(int id, long start, long end) {
		this.id = id;
		this.start = start;
		this.end = end;
	}
}

class VM{
	public int id;
	public float utilizationPrice;
	public float allocationPrice;
	public float propertyPrice;
	
	public VM(int id, float utilizationPrice, float allocationPrice, float propertyPrice) {
		this.id = id;
		this.utilizationPrice = utilizationPrice;
		this.allocationPrice = allocationPrice;
		this.propertyPrice = propertyPrice;
	}
}

public class CalculatingPricesDB {
	
	public static final String CSV = "/Users/agarwaladity/Desktop/DataSet3/";
	public static final String splitBy = ",";
	
	public void findVMPrices() {
		// creates mapping between VMs and cluster
		HashMap<Integer, ArrayList<Resource>> clusterVMMapping = createClusterVMMapping();
		ArrayList<VM> vmPrices = calculatePrices(clusterVMMapping);
		printVMPrices(vmPrices);
	}
	
	public static long min(long number1, long number2) {
		return number1 < number2 ? number1 : number2;
	}
	
	public static long max(long number1, long number2) {
		return number1 > number2 ? number1 : number2;
	}
	
	public static void printVMPrices1(HashMap<Integer, Float> vmPrices) {
		for(Integer vmid : vmPrices.keySet()) {
			System.out.println(vmid + " : " + vmPrices.get(vmid));
		}
	}
	
	public static void printVMPrices(ArrayList<VM> vmPrices) {
		for(int i=0; i<vmPrices.size(); i++) {
			System.out.println("VM ID : " + vmPrices.get(i).id + ", Utilization Price : " + vmPrices.get(i).utilizationPrice + 
					", Allocation Price : " + vmPrices.get(i).allocationPrice + ", Property Price : " + vmPrices.get(i).propertyPrice);
		}
	}
	
	public static ArrayList<VM> calculatePrices(HashMap<Integer, ArrayList<Resource>> clusterVM) {
		// HashMap<Integer, Float> vmPrices = new HashMap<Integer, Float>();
		ArrayList<VM> vmPrices = new ArrayList<VM>();
		try {
			BufferedReader vmReader = new BufferedReader(new FileReader(CSV + "F1.csv"));
			vmReader.readLine();
			String line = "";
			while((line=vmReader.readLine())!=null) {
				float utilizationPrice = 0;
				// if(Integer.parseInt(line.split(",")[0]) == 1) {
				// float price = 0;
				int vmid = Integer.parseInt(line.split(",")[0]);
				// System.out.println("VMID : " + vmid);
				BufferedReader utilizationReader = new BufferedReader(new FileReader(CSV + "F2.csv"));
				utilizationReader.readLine();
				String line1 = "";
				while((line1=utilizationReader.readLine())!=null) {
					if(Integer.parseInt(line1.split(",")[0]) == vmid) {
						long initialStart = Long.parseLong(line1.split(",")[3]);
						long initialEnd = Long.parseLong(line1.split(",")[4]);
						while(initialStart < initialEnd) {
							long tempStart = initialStart;
							long tempEnd = initialEnd;
							int i = 0;
							int clusterid = -1;
							while(i<clusterVM.size() && min(tempEnd, clusterVM.get(vmid).get(i).end) - max(tempStart, clusterVM.get(vmid).get(i).start) <= 0) {
								i += 1;
							}
							
							tempEnd = min(tempEnd, clusterVM.get(vmid).get(i).end);
							tempStart = max(tempStart, clusterVM.get(vmid).get(i).start);
							clusterid = clusterVM.get(vmid).get(i).id;
							BufferedReader clusterPrice = new BufferedReader(new FileReader(CSV + "F8.csv"));
							clusterPrice.readLine();
							String line2 = "";
							while((line2 = clusterPrice.readLine())!=null) {
								if(Integer.parseInt(line2.split(",")[0]) == clusterid && 
										line1.split(",")[1].equals(line2.split(",")[1]) && 
										min(tempEnd, Long.parseLong(line2.split(",")[4])) - max(tempStart, Long.parseLong(line2.split(",")[3])) > 0) {
									tempEnd = min(tempEnd, Long.parseLong(line2.split(",")[4]));
									tempStart = max(tempStart, Long.parseLong(line2.split(",")[3]));
									utilizationPrice = utilizationPrice + (tempEnd - tempStart) * Float.parseFloat(line1.split(",")[2]) * Float.parseFloat(line2.split(",")[2]);
									break;
								}
							}
							clusterPrice.close();
							// update the value of start
							initialStart = tempEnd;
						}
					}
				}
				utilizationReader.close();
				// vmPrices.put(vmid, price);
				
				// computing allocation price
				float allocationPrice = 0;
				BufferedReader allocationReader = new BufferedReader(new FileReader(CSV + "F3.csv"));
				allocationReader.readLine();
				line1 = "";
				while((line1=allocationReader.readLine())!=null) {
					if(Integer.parseInt(line1.split(",")[0]) == vmid) {
						long initialStart = Long.parseLong(line1.split(",")[3]);
						long initialEnd = Long.parseLong(line1.split(",")[4]);
						while(initialStart < initialEnd) {
							long tempStart = initialStart;
							long tempEnd = initialEnd;
							// find out the cluster to which the VM belongs
							int i = 0;
							while(i < clusterVM.size() && min(tempEnd, clusterVM.get(vmid).get(i).end) - max(tempStart, clusterVM.get(vmid).get(i).start) <= 0)
								i += 1;
							// find out the cluster id and update start and end timings
							int clusterid = clusterVM.get(vmid).get(i).id;
							tempStart = max(tempStart, clusterVM.get(vmid).get(i).start);
							tempEnd = min(tempEnd, clusterVM.get(vmid).get(i).end);
							// find out the common cluster pricing for this interval
							BufferedReader clusterPriceReader = new BufferedReader(new FileReader(CSV + "F8.csv"));
							clusterPriceReader.readLine();
							String line2 = "";
							while((line2=clusterPriceReader.readLine())!=null) {
								// check if the cluster id is same, resource type is same
								// and cluster pricing falls within the given cluster
								if(Integer.parseInt(line2.split(",")[0]) == clusterid && 
										line2.split(",")[1].equals(line1.split(",")[1]) && 
										min(tempEnd, Long.parseLong(line2.split(",")[4])) - max(tempStart, Long.parseLong(line2.split(",")[3])) > 0) {
									// compute the price and update start and end timings
									// units allocated * time * price per unit time per unit
									float intermediatePrice = Float.parseFloat(line1.split(",")[2]) * (min(tempEnd, Long.parseLong(line2.split(",")[4])) - max(tempStart, Long.parseLong(line2.split(",")[3]))) * Float.parseFloat(line2.split(",")[2]);
									// allocationPrice = allocationPrice + Float.parseFloat(line1.split(",")[2]) * min(tempEnd, Long.parseLong(line2.split(",")[4])) * Float.parseFloat(line2.split(",")[2]);
									allocationPrice = allocationPrice + intermediatePrice;
									tempStart = max(tempStart, Long.parseLong(line2.split(",")[3]));
									tempEnd = min(tempEnd, Long.parseLong(line2.split(",")[4]));
								}
							}
							clusterPriceReader.close();
							// update the initialStart time
							initialStart = tempEnd;
						}
					}
				}
				
				allocationReader.close();
				
				// computing the property price
				float propertyPrice = 0;
				BufferedReader propertyReader = new BufferedReader(new FileReader(CSV + "F4.csv"));
				propertyReader.readLine();
				line1 = "";
				while((line1=propertyReader.readLine())!=null) {
					if(Integer.parseInt(line1.split(",")[0]) == vmid) {
						// find out the property price from property prices file
						BufferedReader propertyPrices = new BufferedReader(new FileReader(CSV + "F9.csv"));
						propertyPrices.readLine();
						String line2 = "";
						while((line2 = propertyPrices.readLine())!=null) {
							if(line1.split(",")[1].equals(line2.split(",")[0]) && line1.split(",")[2].equals(line2.split(",")[1])) {
								propertyPrice = propertyPrice + (Long.parseLong(line1.split(",")[4]) - Long.parseLong(line1.split(",")[3])) * Float.parseFloat(line2.split(",")[2]);
								// System.out.println("Intermediate Property Price : " + propertyPrice);
							}
						}
						propertyPrices.close();
					}
				}
				propertyReader.close();
				
				vmPrices.add(new VM(vmid, utilizationPrice, allocationPrice, propertyPrice));
			}
			vmReader.close();
			return vmPrices;
			// }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static void printClusterVMRelationship(HashMap<Integer, ArrayList<Resource>> resources) {
		for(Integer vmid : resources.keySet()) {
			System.out.println("VM ID : " + vmid);
			ArrayList<Resource> clusters = resources.get(vmid);
			System.out.println("Cluster ID, Start, End");
			for(int i=0; i<clusters.size(); i++) {
				System.out.println(clusters.get(i).id + ", " + clusters.get(i).start + " : " + clusters.get(i).end);
			}
		}
	}
	
	// method to create the mapping between the cluster and vms
	public static HashMap<Integer, ArrayList<Resource>> createClusterVMMapping() {
		HashMap<Integer, ArrayList<Resource>> vmCluster = new HashMap<Integer, ArrayList<Resource>>();
		try {
			BufferedReader vmReader = new BufferedReader(new FileReader(CSV + "F1.csv"));
			vmReader.readLine();
			String line = "";
			while((line=vmReader.readLine())!= null) {
				int vmid = Integer.parseInt(line.split(splitBy)[0]);
				ArrayList<Resource> hosts = new ArrayList<Resource>();
				BufferedReader hostReader =  new BufferedReader(new FileReader(CSV + "F6.csv"));
				hostReader.readLine();
				String line1 = "";
				while((line1=hostReader.readLine())!= null) {
					if(Integer.parseInt(line1.split(",")[0]) == vmid) {
						hosts.add(new Resource(Integer.parseInt(line1.split(",")[1]), Long.parseLong(line1.split(",")[2]), Long.parseLong(line1.split(",")[3])));
					}
				}
				
				hostReader.close();
				
				ArrayList<Resource> clusters = new ArrayList<Resource>();
				for(Resource host : hosts) {
					BufferedReader clusterReader = new BufferedReader(new FileReader(CSV + "F7.csv"));
					clusterReader.readLine();
					String line2  = "";
					while((line2=clusterReader.readLine())!=null) {
						if(Integer.parseInt(line2.split(",")[0]) == host.id && min(host.end, Long.parseLong(line2.split(",")[3])) - max(host.start, Long.parseLong(line2.split(",")[2])) >= 0) {
							clusters.add(new Resource(Integer.parseInt(line2.split(",")[1]), max(host.start, Long.parseLong(line2.split(",")[2])), min(host.end, Long.parseLong(line2.split(",")[3]))));
						}
					}
					clusterReader.close();
				}
				vmCluster.put(Integer.parseInt(line.split(",")[0]), clusters);
			}
			vmReader.close();
			// printClusterVMRelationship(vmCluster);
			return vmCluster;
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
}