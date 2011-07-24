package be.datablend.mutationstore.setup.model;

/**
 * User: dsuvee
 * Date: 21/07/11
 */
public class Mutation {

    public enum Type { POINT, DELETION, INSERTION };

    private Type type;
    private int position;
    private String base;

    public Mutation(Type type, int position, String base) {
        this.type = type;
        this.position = position;
        this.base = base;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public String getBase() {
        return base;
    }

    public void setBase(String base) {
        this.base = base;
    }

    public String toString() {
        String mutation = "";
        switch(type) {
            case POINT : mutation = getPosition() + "-" + Type.POINT.name() + "-" + getBase(); break;
            case DELETION : mutation = getPosition() + "-" + Type.DELETION.name(); break;
            case INSERTION : mutation = getPosition() + "-" + Type.INSERTION.name() + "-" + getBase(); break;
        }
        return mutation;
    }

}
