package functions;

public interface ReachabilityQuerySolver {
	void Preprocess();
	boolean ReachabilityQuery(int start_id, MyRectangle rect);
}
