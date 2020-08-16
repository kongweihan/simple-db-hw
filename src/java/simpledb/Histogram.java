package simpledb;

public interface Histogram<T> {
    void addValue(T t);
    double estimateSelectivity(Predicate.Op op, T t);
    double avgSelectivity(Predicate.Op op);
    double avgSelectivity();
}
