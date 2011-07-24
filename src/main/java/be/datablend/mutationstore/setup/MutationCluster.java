package be.datablend.mutationstore.setup;

import be.datablend.mutationstore.setup.model.Mutation;
import be.datablend.mutationstore.setup.model.Sequence;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import static be.datablend.mutationstore.definition.Definition.*;

import java.util.Arrays;
import java.util.UUID;

/**
 * User: dsuvee
 * Date: 21/07/11
 */
public class MutationCluster {

    public Cluster cluster = null;
    private ColumnFamilyTemplate<UUID, String> sequenceTemplate;
    private ColumnFamilyTemplate<String, UUID> mutationTemplate;

    public void create() {
        // Get the cluster
        cluster = HFactory.getOrCreateCluster(MUTATION_CLUSTER,"localhost:9160");
        // Generate the keyspaces
        generateKeyspaces();
        // Generate the mutations
        //generateMutations();
    }

    public void generateKeyspaces() {
        // Check whether the keyspace already exist
        KeyspaceDefinition keyspace = cluster.describeKeyspace(MUTATION_KEYSPACE);

        // Keyspace does not yet exist, create it ...
        if (keyspace == null) {
            System.out.println("Started setup of mutation keyspace ... ");

            // Setup the definition of all column families
            ColumnFamilyDefinition mutations = HFactory.createColumnFamilyDefinition(MUTATION_KEYSPACE, MUTATIONS_CF, ComparatorType.TIMEUUIDTYPE);
            ColumnFamilyDefinition sequences = HFactory.createColumnFamilyDefinition(MUTATION_KEYSPACE, SEQUENCES_CF, ComparatorType.UTF8TYPE);

            // Setup the keyspace
            keyspace = HFactory.createKeyspaceDefinition(MUTATION_KEYSPACE, ThriftKsDef.DEF_STRATEGY_CLASS, REPLACTION_FACTOR, Arrays.asList(mutations, sequences));

            // Create it
            cluster.addKeyspace(keyspace, true);

            System.out.println("Finished setup of mutation keyspace ... ");
        }
    }

    public void generateMutations() {
        System.out.println("Started import of mutation data ... ");

        // Retrieve the mutation keyspace
        Keyspace mutationkeyspace = HFactory.createKeyspace(MUTATION_KEYSPACE, cluster);

        // Create a set of templates used to import sequences and mutations (in batch mode)
        sequenceTemplate = new ThriftColumnFamilyTemplate<UUID, String>(mutationkeyspace, SEQUENCES_CF, UUIDSerializer.get(), StringSerializer.get());
        sequenceTemplate.setBatched(true);
        mutationTemplate = new ThriftColumnFamilyTemplate<String, UUID>(mutationkeyspace, MUTATIONS_CF, StringSerializer.get(), UUIDSerializer.get());
        mutationTemplate.setBatched(true);

        // A counter for maintaing the set of generated mutations
        int numberofmutations = 0;

        // Let's generate a million sequences, each with a set of mutations
        SequenceGenerator generator = new SequenceGenerator();
        for (int i = 0; i < 1000000; i++) {
            // Generate the a new random sequence
            Sequence sequence = generator.generateRandomSequence();

            // Convert date to Time UUID (the unique key of a sequence in the sequence keyspace
            UUID uuid = TimeUUIDUtils.getTimeUUID(sequence.getDate());

            // Create an updater to persist in the sequence keyspace
            ColumnFamilyUpdater<UUID, String> sequenceUpdater = sequenceTemplate.createUpdater(uuid);
            sequenceUpdater.setString(INTERNALID_CN,sequence.getInternalId());
            sequenceUpdater.setString(METHOD_CN,sequence.getMethod().name());
            sequenceUpdater.setString(ORIGIN_CN,sequence.getOrigin().name());
            sequenceTemplate.update(sequenceUpdater);

            // Create an updater to persist the mutations in the mutation keyspace
            for (Mutation mutation : sequence.getMutations()) {
                ColumnFamilyUpdater<String, UUID> mutationUpdater = mutationTemplate.createUpdater(mutation.toString());
                mutationUpdater.setString(uuid,"");
                mutationTemplate.update(mutationUpdater);
                numberofmutations++;
            }

            // Persist every 250 sequences
            if (i % 250 == 0) {
                sequenceTemplate.executeBatch();
                mutationTemplate.executeBatch();
            }

            System.out.println("Finished import of mutation data ... ");
            System.out.println(numberofmutations + " mutations imported");

        }
    }

    public void close() {
        cluster.getConnectionManager().shutdown();
    }

    public Cluster getCluster() {
        return cluster;
    }

}
