package be.datablend.mutationstore.main;

import be.datablend.mutationstore.query.Query;
import be.datablend.mutationstore.setup.MutationCluster;

/**
 * User: dsuvee
 * Date: 21/07/11
 */
public class Main {

    public static void main(String[] args) {
        // Start by creating the cluster connection
        MutationCluster cluster = new MutationCluster();
        cluster.create();

        // Execute a set of example queries
        Query queries = new Query(cluster.getCluster());
        queries.executeSingleMutationQuery();

        // Close the cluster connection
        cluster.close();
    }
}
