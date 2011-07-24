package be.datablend.mutationstore.query;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static be.datablend.mutationstore.definition.Definition.*;

/**
 * User: dsuvee
 * Date: 21/07/11
 */
public class Query {

    private static final long RANGE_START_DATE;
    private static final long RANGE_STOP_DATE;
    private static final String SINGLE_MUTATION_TO_FIND = "11-POINT-C";

    private Cluster cluster;
    private Keyspace mutationkeyspace;

    static {
        Calendar cdr = Calendar.getInstance();
        cdr.set(Calendar.DAY_OF_MONTH, 1);
		cdr.set(Calendar.MONTH, Calendar.JANUARY);
		cdr.set(Calendar.YEAR, 2003);
		RANGE_START_DATE = cdr.getTimeInMillis();

        cdr.set(Calendar.DAY_OF_MONTH, 31);
		cdr.set(Calendar.MONTH, Calendar.DECEMBER);
		cdr.set(Calendar.YEAR, 2007);
		RANGE_STOP_DATE = cdr.getTimeInMillis();
    }

    public Query(Cluster cluster) {
        this.cluster = cluster;
        // Retrieve the mutation keyspace
        this.mutationkeyspace = HFactory.createKeyspace(MUTATION_KEYSPACE, cluster);
    }

    // Retrieve all sequences that have a particular mutation
    public void executeSingleMutationQuery() {
        // Create the range slice query
        RangeSlicesQuery<String, UUID, String> rangequery = HFactory.createRangeSlicesQuery(mutationkeyspace, StringSerializer.get(), UUIDSerializer.get(), StringSerializer.get());
		rangequery.setColumnFamily(MUTATIONS_CF);
		rangequery.setReturnKeysOnly();
		rangequery.setKeys(SINGLE_MUTATION_TO_FIND,SINGLE_MUTATION_TO_FIND);
		rangequery.setRange(TimeUUIDUtils.getTimeUUID(RANGE_START_DATE), TimeUUIDUtils.getTimeUUID(RANGE_STOP_DATE), false, 100000);

        // Execute the query
		QueryResult<OrderedRows<String, UUID, String>> result = rangequery.execute();

        // Retrieve the row related to the specific mutation
        Row<String, UUID, String> mutationrow = result.get().getByKey(SINGLE_MUTATION_TO_FIND);
        // Retrieve the columns related to the mutation row
		List<HColumn<UUID,String>> sequences = mutationrow.getColumnSlice().getColumns();

        System.out.println("Query executed in " + TimeUnit.MILLISECONDS.convert(result.getExecutionTimeNano(), TimeUnit.NANOSECONDS) + " milliseconds");
        System.out.println(sequences.size() + " sequences found with mutation " + SINGLE_MUTATION_TO_FIND);

        // Retrieve the sequence keys
        List<UUID> sequencekeys = new ArrayList<UUID>();
        for (HColumn<UUID,String> sequence : sequences) {
            sequencekeys.add(sequence.getName());
        }

        // Print the individual sequence information
        printSequenceInformation(sequencekeys);

    }

    public void printSequenceInformation(List<UUID> sequencekeys) {
        // Create the multiget slice query
        MultigetSliceQuery<UUID, String, String> slicequery = HFactory.createMultigetSliceQuery(mutationkeyspace, UUIDSerializer.get(), StringSerializer.get(), StringSerializer.get());
		slicequery.setColumnFamily(SEQUENCES_CF);
		slicequery.setColumnNames(INTERNALID_CN, METHOD_CN, ORIGIN_CN);
		slicequery.setKeys(sequencekeys);
		Rows<UUID, String, String> sequences = slicequery.execute().get();

        for (UUID sequencekey : sequencekeys) {
            Row<UUID, String, String> sequence = sequences.getByKey(sequencekey);
            // Retrieve the sequence data
            Calendar sequencedate = Calendar.getInstance();
            sequencedate.setTimeInMillis(TimeUUIDUtils.getTimeFromUUID(sequence.getKey()));
            // Retrieve the sequence internal id, sequencing method and sample origin
            String internalid = sequence.getColumnSlice().getColumnByName(INTERNALID_CN).getValue();
            String method = sequence.getColumnSlice().getColumnByName(METHOD_CN).getValue();
            String origin = sequence.getColumnSlice().getColumnByName(ORIGIN_CN).getValue();
            // Print it
            System.out.println(sequencedate.getTime() + " - " + internalid + " - " + method + " - " + origin);
        }
    }

}
