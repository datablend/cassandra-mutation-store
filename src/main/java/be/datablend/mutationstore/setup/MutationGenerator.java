package be.datablend.mutationstore.setup;

import be.datablend.mutationstore.setup.model.Mutation;

import java.util.Random;

/**
 * User: dsuvee
 * Date: 21/07/11
 */
public class MutationGenerator {

    private static final int MIN_POSITION = 1;
    private static final int MAX_POSITION = 300;
    private static final String[] bases = { "A", "G","C", "T" };

    private final Random random = new Random();

    private int generateMutatationPosition() {
        return random.nextInt(MAX_POSITION - MIN_POSITION + 1) + MIN_POSITION;
    }

    private Mutation.Type generateRandomMutationType() {
        return Mutation.Type.values()[random.nextInt(Mutation.Type.values().length)];
    }

    private String generateRandomMutationBase() {
        return bases[random.nextInt(bases.length)];
    }

    public Mutation generateRandomMutation() {
        Mutation mutation = null;
        Mutation.Type type = generateRandomMutationType();
        switch(type) {
            case POINT : mutation = new Mutation(Mutation.Type.POINT, generateMutatationPosition(), generateRandomMutationBase()); break;
            case DELETION : mutation = new Mutation(Mutation.Type.DELETION, generateMutatationPosition(), ""); break;
            case INSERTION : mutation = new Mutation(Mutation.Type.INSERTION, generateMutatationPosition(), generateRandomMutationBase()); break;
        }
        return mutation;
    }

}
