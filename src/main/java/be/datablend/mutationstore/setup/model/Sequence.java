package be.datablend.mutationstore.setup.model;

/**
 * User: dsuvee
 * Date: 21/07/11
 */
public class Sequence {

    public enum ORIGIN { BLOOD, HAIR };
    public enum METHOD { M_GIL, SANGER};

    private Mutation[] mutations;
    private String internalId;
    private ORIGIN origin;
    private METHOD method;
    private long date;

    public Sequence(String internalId, ORIGIN origin, METHOD method, Mutation[] mutations, long date) {
        this.mutations = mutations;
        this.internalId = internalId;
        this.origin = origin;
        this.method = method;
        this.date = date;
    }

    public Mutation[] getMutations() {
        return mutations;
    }

    public void setMutations(Mutation[] mutations) {
        this.mutations = mutations;
    }

    public String getInternalId() {
        return internalId;
    }

    public void setInternalId(String internalId) {
        this.internalId = internalId;
    }

    public ORIGIN getOrigin() {
        return origin;
    }

    public void setOrigin(ORIGIN origin) {
        this.origin = origin;
    }

    public METHOD getMethod() {
        return method;
    }

    public void setMethod(METHOD method) {
        this.method = method;
    }

    public long getDate() {
        return date;
    }

    public void setDate(long date) {
        this.date = date;
    }

}
