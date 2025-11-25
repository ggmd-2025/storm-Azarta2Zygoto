package stormTP.topology;


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.ConsumeTimeBolt;
import stormTP.operator.ExitInLogBolt;
import stormTP.operator.MyTortoiseBolt;


public class TopologyE1 {
	
	public static void main(String[] args) throws Exception {

        int nbExecutors = 1;
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);

        /*Création du spout*/
        InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);
        /*Création de la topologie*/
        TopologyBuilder builder = new TopologyBuilder();
        /*Affectation à la topologie du spout*/
        builder.setSpout("masterStream", spout);
        /* Opérateur qui consomme du temps */
        // builder.setBolt("consume", new ConsumeTimeBolt(), nbExecutors).shuffleGrouping("masterStream");
        builder.setBolt("consume", new MyTortoiseBolt(portOUTPUT), nbExecutors).shuffleGrouping("masterStream");
        /* OutPut mis dans le log  */
        builder.setBolt("exit", new ExitInLogBolt(), nbExecutors).shuffleGrouping("consume");
           
        /**
         * Configuration of metadata of the topology
         */
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(4);
		
		/**
		 * Call to the topology submitter for storm
		 */
		
        StormSubmitter.submitTopology("topoE1", config, builder.createTopology());
        
      
		
	}
}