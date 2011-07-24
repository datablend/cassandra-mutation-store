package be.datablend.mutationstore.setup;

import be.datablend.mutationstore.setup.model.Mutation;
import be.datablend.mutationstore.setup.model.Sequence;
import org.apache.commons.lang.RandomStringUtils;

import java.util.Calendar;
import java.util.Random;

/**
 * User: dsuvee
 * Date: 21/07/11
 */
public class SequenceGenerator {

    private static final int MIN_NUMBER_OF_MUTATIONS = 1;
    private static final int MAX_NUMBER_OF_MUTATIONS = 300;
    private static final long START_DATE;
    private static final long STOP_DATE;

    static {
        Calendar cdr = Calendar.getInstance();
        cdr.set(Calendar.DAY_OF_MONTH, 1);
		cdr.set(Calendar.MONTH, Calendar.JANUARY);
		cdr.set(Calendar.YEAR, 2000);
		START_DATE = cdr.getTimeInMillis();

        cdr.set(Calendar.DAY_OF_MONTH, 31);
		cdr.set(Calendar.MONTH, Calendar.DECEMBER);
		cdr.set(Calendar.YEAR, 2010);
		STOP_DATE = cdr.getTimeInMillis();
    }

    private final Random random = new Random();
    private final MutationGenerator mutationGenerator = new MutationGenerator();

    private Sequence.METHOD generateRandomSequenceMethod() {
        return Sequence.METHOD.values()[random.nextInt(Sequence.METHOD.values().length)];
    }

    private Sequence.ORIGIN generateRandomSequenceOrigin() {
        return Sequence.ORIGIN.values()[random.nextInt(Sequence.ORIGIN.values().length)];
    }

    private String generateRandomSequenceId() {
        return RandomStringUtils.random(8,true,false);
    }

    private Mutation[] generateRandomMutations() {
        int count = random.nextInt(MAX_NUMBER_OF_MUTATIONS - MIN_NUMBER_OF_MUTATIONS + 1) + MIN_NUMBER_OF_MUTATIONS;
        Mutation[] mutations = new Mutation[count];
        for (int i = 0; i < count; i++) {
            mutations[i] = mutationGenerator.generateRandomMutation();
        }
        return mutations;
    }

    private long generateRandomDate() {
        return (long)(random.nextLong()*(STOP_DATE-START_DATE))+START_DATE;
    }

    public Sequence generateRandomSequence() {
        return new Sequence(generateRandomSequenceId(), generateRandomSequenceOrigin(),generateRandomSequenceMethod(), generateRandomMutations(), generateRandomDate());
    }

}
